use std::{
	collections::{HashMap},
	sync::{Arc, RwLock}
};
use rand::{Rng, SeedableRng};
use tarpc::{
	context,
	tokio_serde::formats::Bincode,
	server::Channel,
	serde::Serialize,
	serde::Deserialize
};
use futures::{future, prelude::*};
use log::{info, warn, debug};
use super::{
	ring::*,
	config::*,
	data_store::*
};
use crate::rpc::*;
use super::calculate_hash;

// Data part of the node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
	pub id: Digest,
	pub addr: String
}

#[derive(Clone)]
pub struct NodeServer {
	node: Node,
	store: DataStore,
	replication_factor: u64,
	predecessor: Arc<RwLock<Option<Node>>>,
	// The first entry is successor
	finger_table: Arc<RwLock<Vec<Node>>>,
	// connection to remote nodes
	connection_map: Arc<RwLock<HashMap<Digest, NodeServiceClient>>>
}

impl NodeServer {
	pub fn new(config: &NodeConfig) -> Self {
		assert!(config.replication_factor != 0, "replication_factor equal to 0");

		// init a ring with only one node
		// (see second part of n.join in Figure 6)
		let node = config.node.clone();
		let finger_table = vec![node.clone(); NUM_BITS];

		NodeServer {
			node: config.node.clone(),
			store: DataStore::new(),
			replication_factor: config.replication_factor,
			predecessor: Arc::new(RwLock::new(Some(node.clone()))),
			finger_table: Arc::new(RwLock::new(finger_table)),
			connection_map: Arc::new(RwLock::new(HashMap::new()))
		}
	}

	pub fn get_successor(&self) -> Node {
		let table = self.finger_table.read().unwrap();
		table[0].clone()
	}

	pub fn set_successor(&self, node: Node) {
		let mut table = self.finger_table.write().unwrap();
		table[0] = node;
	}

	pub fn get_predecessor(&self) -> Option<Node> {
		self.predecessor.read().unwrap().clone()
	}

	pub fn set_predecessor(&self, node: Option<Node>) {
		*self.predecessor.write().unwrap() = node;
	}

	// Start the server
	pub async fn start(&mut self, config: &RuntimeConfig) -> anyhow::Result<tokio::task::JoinHandle<()>> {
		if let Some(n) = config.join_node.as_ref() {
			self.join(&n).await;
		}

		let mut listener = tarpc::serde_transport::tcp::listen(&self.node.addr, Bincode::default).await?;
		let server = self.clone();
		// Listen for rpc call
		let handle = tokio::spawn(async move {
			listener.config_mut().max_frame_length(usize::MAX);
			listener
				.filter_map(|r| future::ready(r.ok()))
				.map(tarpc::server::BaseChannel::with_defaults)
				.map(|channel| async {
					// Clone a new server to share the data in Arc
					channel.execute(server.clone().serve()).await;
				})
				.buffer_unordered(10)
				.for_each(|_| async {})
				.await;
		});

		// Periodically stabilize
		let mut server = self.clone();
		let stabilize_interval = config.stabilize_interval;
		if stabilize_interval > 0 {
			tokio::spawn(async move {
				let mut interval = tokio::time::interval(
					tokio::time::Duration::from_millis(stabilize_interval)
				);
				// let mut s= server.clone();
				loop {
					interval.tick().await;
					server.stabilize().await;
				}
			});
		}

		// Periodically refresh finger table
		let mut server = self.clone();
		let fix_finger_interval = config.fix_finger_interval;
		if fix_finger_interval > 0 {
			tokio::spawn(async move {
				let mut interval = tokio::time::interval(
					tokio::time::Duration::from_millis(fix_finger_interval)
				);
				// StdRng can be sent across threads
				let mut rng = rand::prelude::StdRng::from_entropy();
				// let mut s= server.clone();
				loop {
					interval.tick().await;
					let index = rng.gen_range(1..NUM_BITS);
					server.fix_finger(index).await;
				}
			});
		}

		info!("Node {} listening at {}", self.node.id, self.node.addr);
		Ok(handle)
	}

	// Calculate start field of finger table (see Table 1)
	// k in [0, m)
	pub fn finger_table_start(&self, k: usize) -> u64 {
		self.node.id.wrapping_add(1 << k)
	}
	
	async fn get_connection(&mut self, node: &Node) -> NodeServiceClient {
		// Use block to drop map immediately after use
		{
			let map = self.connection_map.read().unwrap();
			if let Some(c) = map.get(&node.id) {
				// client can be cloned with lost cost
				return c.clone();
			}
		}
		{
			info!("Connecting from node {} to node {}", self.node.id, node.id);
			let c = crate::client::setup_client(&node.addr).await;
			info!("Connected from node {} to node {}", self.node.id, node.id);
			let mut map = self.connection_map.write().unwrap();
			map.insert(node.id, c.clone());
			return c;
		}
	}

	// Figure 7: n.join
	pub async fn join(&mut self, node: &Node) {
		debug!("Node {}: joining node {}", self.node.id, node.id);
		self.set_predecessor(None);
		let n = self.get_connection(node).await;
		let succ = n.find_successor_rpc(context::current(), self.node.id).await.unwrap();
		self.set_successor(succ);
		debug!("Node {}: joined node {}", self.node.id, node.id);
	}

	// Figure 7: n.stabilize
	pub async fn stabilize(&mut self) {
		let ctx = context::current();
		let mut succ = self.get_successor();

		let self_node = self.node.clone();
		let n = self.get_connection(&succ).await;
		let x= match n.get_predecessor_rpc(ctx).await.unwrap() {
			Some(v) => v,
			None => {
				warn!("Node {}: empty predecessor of successor node: {}", self_node.id, succ.id);
				return;
			}
		};
		if in_range(x.id, self.node.id, succ.id) {
			self.set_successor(x.clone());
			// update succ
			succ = x;
		}

		// update connection because succ may change here
		let n = self.get_connection(&succ).await;
		n.notify_rpc(ctx, self_node).await.unwrap();
	}

	// Figure 7: n.fix_fingers
	pub async fn fix_finger(&mut self, index: usize) {
		let succ = self.find_successor(self.finger_table_start(index)).await;
		let mut table = self.finger_table.write().unwrap();
		table[index] = succ;
	}

	// Figure 4: n.find_successor
	async fn find_successor(&mut self, id: Digest) -> Node {
		debug!("Node {}: find_successor({})", self.node.id, id);
		let n = self.find_predecessor(id).await;
		let c = self.get_connection(&n).await;
		let succ = c.get_successor_rpc(context::current()).await.unwrap();
		debug!("Node {}: find_successor({}) returns {}", self.node.id, id, succ.id);
		succ
	}

	// Figure 4: n.find_predecessor
	async fn find_predecessor(&mut self, id: Digest) -> Node {
		debug!("Node {}: find_predecessor({})", self.node.id, id);
		let mut n = self.node.clone();
		let mut succ = self.get_successor();
		let mut conn = self.get_connection(&n).await;

		// stop when id in (n, succ]
		while !(in_range(id, n.id, succ.id) || id == succ.id) {
			debug!("Node {}: find_predecessor range ({}, {}]", self.node.id, n.id, succ.id);
			n = conn.closest_preceding_finger_rpc(context::current(), id).await.unwrap();
			conn = self.get_connection(&n).await;
			succ = conn.get_successor_rpc(context::current()).await.unwrap();
		}
		debug!("Node {}: find_predecessor({}) returns {}", self.node.id, id, n.id);
		n
	}

	// Figure 4: n.closest_preceding_finger
	async fn closest_preceding_finger(&mut self, id: Digest) -> Node {
		let table = self.finger_table.read().unwrap();
		for i in (0..NUM_BITS).rev() {
			let f = &table[i];
			if in_range(f.id, self.node.id, id) {
				return f.clone();
			};
		}
		self.node.clone()
	}

	// Figure 7: n.notify
	async fn notify(&mut self, node: Node) {
		let pred = self.get_predecessor();
		if let Some(p) = pred {
			if !in_range(node.id, p.id, self.node.id) {
				return;
			}
		}

		debug!("Node {}: new predecessor set in notify: {}", self.node.id, node.id);
		self.set_predecessor(Some(node));
	}

	// Get key on the ring
	async fn get(&mut self, key: Key) -> Option<Value> {
		// Try local store first because of replication
		match self.store.get(&key) {
			Some(v) => return Some(v),
			None => ()
		}

		// Fetch from the responsible node
		let id = calculate_hash(&key);
		let succ = self.find_successor(id).await;
		let c = self.get_connection(&succ).await;
		let value = c.get_local_rpc(context::current(), key).await.unwrap();
		value
	}

	// Set key on the ring
	async fn set(&mut self, key: Key, value: Option<Value>) {
		let id = calculate_hash(&key);
		let succ = self.find_successor(id).await;
		let c = self.get_connection(&succ).await;

		c.replicate_rpc(context::current(), key, value, self.replication_factor).await.unwrap();
	}

	// Replicate key to (num - 1) successors and itself
	async fn replicate(&mut self, key: Key, value: Option<Value>, num: u64) {
		assert!(num != 0, "replicate num is 0");
		// replicate it locally
		self.store.set(key.clone(), value.clone());

		// replicate it to successors asynchronously (lazy)
		if num - 1 > 0 {
			let succ = self.get_successor();
			let c = self.get_connection(&succ).await;
			tokio::spawn(async move {
				c.replicate_rpc(context::current(), key, value, num-1).await.unwrap();
			});
		}
	}
}

#[tarpc::server]
impl NodeService for NodeServer {
	async fn get_node_rpc(self, _: context::Context) -> Node {
		debug!("Node {}: get_node_rpc called", self.node.id);
		let node = self.node.clone();
		debug!("Node {}: get_node_rpc called", self.node.id);
		node
	}

	async fn get_predecessor_rpc(self, _: context::Context) -> Option<Node> {
		debug!("Node {}: get_predecessor_rpc called", self.node.id);
		let pred = self.predecessor.read().unwrap().clone();
		debug!("Node {}: get_predecessor_rpc finished", self.node.id);
		pred
	}

	async fn get_successor_rpc(self, _: context::Context) -> Node {
		debug!("Node {}: get_successor_rpc called", self.node.id);
		let succ = self.get_successor();
		debug!("Node {}: get_successor_rpc finished", self.node.id);
		succ
	}

	async fn find_successor_rpc(mut self, _: context::Context, id: Digest) -> Node {
		debug!("Node {}: find_successor_rpc called", self.node.id);
		let succ = self.find_successor(id).await;
		debug!("Node {}: find_successor_rpc finished", self.node.id);
		succ
	}

	async fn find_predecessor_rpc(mut self, _: context::Context, id: Digest) -> Node {
		debug!("Node {}: find_predecessor_rpc called", self.node.id);
		let pred = self.find_predecessor(id).await;
		debug!("Node {}: find_predecessor_rpc finished", self.node.id);
		pred
	}

	async fn closest_preceding_finger_rpc(mut self, _: context::Context, id: Digest) -> Node {
		debug!("Node {}: closest_preceding_finger_rpc called", self.node.id);
		let node = self.closest_preceding_finger(id).await;
		debug!("Node {}: closest_preceding_finger_rpc finished", self.node.id);
		node
	}

	async fn notify_rpc(mut self, _: context::Context, node: Node) {
		debug!("Node {}: notify_rpc called", self.node.id);
		self.notify(node).await;
		debug!("Node {}: notify_rpc finished", self.node.id);
	}

	async fn stabilize_rpc(mut self, _: context::Context) {
		debug!("Node {}: stabilize_rpc called", self.node.id);
		self.stabilize().await;
		debug!("Node {}: stabilize_rpc finished", self.node.id);
	}

	async fn get_local_rpc(self, _: context::Context, key: Key) -> Option<Value> {
		debug!("Node {}: get_local_rpc called", self.node.id);
		let value = self.store.get(&key);
		debug!("Node {}: get_local_rpc finished", self.node.id);
		value
	}

	async fn set_local_rpc(self, _: context::Context, key: Key, value: Option<Value>) {
		debug!("Node {}: set_local_rpc called", self.node.id);
		self.store.set(key, value);
		debug!("Node {}: set_local_rpc finished", self.node.id);
	}

	async fn get_rpc(mut self, _: context::Context, key: Key) -> Option<Value> {
		debug!("Node {}: get_rpc called", self.node.id);
		let value = self.get(key).await;
		debug!("Node {}: get_rpc finished", self.node.id);
		value
	}

	async fn set_rpc(mut self, _: context::Context, key: Key, value: Option<Value>) {
		debug!("Node {}: set_rpc called", self.node.id);
		self.set(key, value).await;
		debug!("Node {}: set_rpc finished", self.node.id);
	}

	async fn replicate_rpc(mut self, _: context::Context, key: Key, value: Option<Value>, num: u64) {
		debug!("Node {}: replicate_rpc called", self.node.id);
		self.replicate(key, value, num).await;
		debug!("Node {}: replicate_rpc finished", self.node.id);
	}
}


#[cfg(test)]
mod tests {
	use super::*;

	async fn fix_all_fingers(server: &mut NodeServer) {
		for i in 1..NUM_BITS {
			server.fix_finger(i).await;
		}
	}

	/// Test figure 3b, 5a
	#[tokio::test]
	async fn test_node_metadata() -> anyhow::Result<()> {
		env_logger::init();

		// Node 0
		let n0 = Node {
			addr: "localhost:9800".to_string(),
			id: 0
		};
		// Node 1
		let n1 = Node {
			addr: "localhost:9801".to_string(),
			id: 1
		};
		// Node 3
		let n3 = Node {
			addr: "localhost:9803".to_string(),
			id: 3
		};
		// Node 6
		let n6 = Node {
			addr: "localhost:9806".to_string(),
			id: 6
		};

		let mut node_config = NodeConfig {
			node: n0.clone(),
			replication_factor: 1
		};
		let mut runtime_config = RuntimeConfig {
			join_node: None,
			fix_finger_interval: 0,
			stabilize_interval: 0
		};
		let mut s0 = NodeServer::new(&node_config);
		// Disable auto fix_finger and stabilize
		s0.start(&runtime_config).await?;
		s0.stabilize().await;
		// single-node ring
		assert_eq!(s0.get_predecessor().unwrap().id, 0);
		assert_eq!(s0.get_successor().id, 0);


		// Node 1 joins node 0
		node_config.node = n1.clone();
		let mut s1 = NodeServer::new(&node_config);
		runtime_config.join_node = Some(n0.clone());
		s1.start(&runtime_config).await?;
		assert_eq!(s1.get_successor().id, 0);

		// Stabilize c1 first to notify c0
		s1.stabilize().await;
		assert_eq!(s0.get_predecessor().unwrap().id, 1);
		s0.stabilize().await;
		assert_eq!(s0.get_predecessor().unwrap().id, 1);
		assert_eq!(s0.get_successor().id, 1);
		assert_eq!(s1.get_predecessor().unwrap().id, 0);
		assert_eq!(s1.get_successor().id, 0);
		
		// Fix fingers
		fix_all_fingers(&mut s0).await;
		{
			let table = s0.finger_table.read().unwrap();
			assert_eq!(table[1].id, 0);
		}
		fix_all_fingers(&mut s1).await;
		{
			let table = s1.finger_table.read().unwrap();
			assert_eq!(table[1].id, 0);
			assert_eq!(table[2].id, 0);
		}


		// Node 3 joins node 1
		node_config.node = n3.clone();
		let mut s3 = NodeServer::new(&node_config);
		runtime_config.join_node = Some(n1.clone());
		s3.start(&runtime_config).await?;
		s3.stabilize().await;
		s1.stabilize().await;
		s0.stabilize().await;

		assert_eq!(s3.get_predecessor().unwrap().id, 1);
		assert_eq!(s1.get_predecessor().unwrap().id, 0);
		assert_eq!(s0.get_predecessor().unwrap().id, 3);

		// See finger table in Figure 3b
		fix_all_fingers(&mut s0).await;
		{
			let table = s0.finger_table.read().unwrap();
			assert_eq!(table[0].id, 1);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s1).await;
		{
			let table = s1.finger_table.read().unwrap();
			assert_eq!(table[0].id, 3);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s3).await;
		{
			let table = s3.finger_table.read().unwrap();
			assert_eq!(table[0].id, 0);
			assert_eq!(table[1].id, 0);
			assert_eq!(table[2].id, 0);
		}



		// Node 6 joins node 0
		node_config.node = n6.clone();
		let mut s6 = NodeServer::new(&node_config);
		runtime_config.join_node = Some(n0.clone());
		s6.start(&runtime_config).await?;
		s6.stabilize().await;
		s3.stabilize().await;
		s1.stabilize().await;
		s0.stabilize().await;

		assert_eq!(s6.get_predecessor().unwrap().id, 3);
		assert_eq!(s0.get_predecessor().unwrap().id, 6);
		assert_eq!(s1.get_predecessor().unwrap().id, 0);
		assert_eq!(s3.get_predecessor().unwrap().id, 1);

		// See finger table in Figure 6a
		fix_all_fingers(&mut s0).await;
		{
			let table = s0.finger_table.read().unwrap();
			assert_eq!(table[0].id, 1);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 6);
		}
		fix_all_fingers(&mut s1).await;
		{
			let table = s1.finger_table.read().unwrap();
			assert_eq!(table[0].id, 3);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 6);
		}
		fix_all_fingers(&mut s3).await;
		{
			let table = s3.finger_table.read().unwrap();
			assert_eq!(table[0].id, 6);
			assert_eq!(table[1].id, 6);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s6).await;
		{
			let table = s6.finger_table.read().unwrap();
			assert_eq!(table[0].id, 0);
			assert_eq!(table[1].id, 0);
			// different from figure 6 because of different NUM_BITS
			assert_eq!(table[2].id, 0);
		}

		Ok(())
	}
}
