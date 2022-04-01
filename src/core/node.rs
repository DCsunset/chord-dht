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
use log::{info, warn, debug, error};
use super::{
	ring::*,
	config::*,
	data_store::*,
	error::{
		*,
		DhtError::*
	}
};
use crate::{rpc::*, server::ServerManager};
use super::calculate_hash;

// Data part of the node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
	pub id: Digest,
	pub addr: String
}

impl std::fmt::Display for Node {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Node({}, {})", self.id, self.addr)
	}
}

#[derive(Clone)]
pub struct NodeServer {
	node: Node,
	store: DataStore,
	config: Config,
	predecessor: Arc<RwLock<Option<Node>>>,
	// The first entry is maintained by successor_list[0]
	finger_table: Arc<RwLock<Vec<Node>>>,
	// Maintain (fault_tolerance + 1) successors for recovery
	successor_list: Arc<RwLock<Vec<Node>>>,
	// connection to remote nodes
	connection_map: Arc<RwLock<HashMap<Digest, NodeServiceClient>>>
}

impl NodeServer {
	pub fn new(node: Node, config: Config) -> Self {
		assert!(config.replication_factor != 0, "replication_factor equal to 0");
		assert!(config.replication_factor <= config.fault_tolerance + 1, "replication_factor greater than fault_tolerance + 1");

		// init a ring with only one node
		// (see second part of n.join in Figure 6)
		let finger_table = vec![node.clone(); NUM_BITS];
		let successor_list = vec![node.clone(); config.fault_tolerance as usize + 1];

		NodeServer {
			node: node.clone(),
			store: DataStore::new(),
			config: config,
			predecessor: Arc::new(RwLock::new(Some(node.clone()))),
			finger_table: Arc::new(RwLock::new(finger_table)),
			successor_list: Arc::new(RwLock::new(successor_list)),
			connection_map: Arc::new(RwLock::new(HashMap::new()))
		}
	}

	pub fn get_successor(&self) -> Node {
		self.successor_list.read().unwrap()[0].clone()
	}

	pub fn get_successor_list(&self) -> Vec<Node> {
		self.successor_list.read().unwrap().clone()
	}

	pub fn set_successor_list(&self, succ_list: Vec<Node>) {
		*self.successor_list.write().unwrap() = succ_list;
	}

	pub fn get_predecessor(&self) -> Option<Node> {
		self.predecessor.read().unwrap().clone()
	}

	pub fn set_predecessor(&self, node: Option<Node>) {
		*self.predecessor.write().unwrap() = node;
	}

	/// Start the server
	/// Returns if the listener starts
	pub async fn start(&mut self, join_node: Option<Node>) -> DhtResult<ServerManager> {
		// channel used to shutdown (true means shutdown)
		let (tx, rx) = tokio::sync::watch::channel(false);

		// Listen locally first
		let mut listener = tarpc::serde_transport::tcp::listen(&self.node.addr, Bincode::default).await?;
		let server = self.clone();
		let mut listener_rx = rx.clone();
		// Listen for rpc call
		let listener_handle = tokio::spawn(async move {
			listener.config_mut().max_frame_length(usize::MAX);
			let listener_fut = listener
				.filter_map(|r| future::ready(r.ok()))
				.map(tarpc::server::BaseChannel::with_defaults)
				.map(|channel| async {
					// Clone a new server to share the data in Arc
					channel.execute(server.clone().serve()).await;
				})
				.buffer_unordered(server.config.max_connections as usize)
				.for_each(|_| async {});

			// listener_fut.await;
			debug!("{}: listening", server.node);
			
			tokio::select! {
				_ = listener_fut => {
					warn!("{}: listener terminated", server.node);
				},
				_ = listener_rx.changed() => {
					debug!("{}: listener stopped gracefully", server.node);
				}
			};
		});

		// Join node after server starts
		if let Some(n) = join_node.as_ref() {
			match self.join(&n).await {
				Ok(_) => (),
				Err(e) => {
					return Err(JoinFailure {
						node: n.clone(),
						message: e.to_string()
					});
				}
			};
		}

		// Periodically stabilize
		let mut server = self.clone();
		let mut stabilize_rx = rx.clone();
		let stabilize_interval = self.config.stabilize_interval;
		let stabilize_handle = tokio::spawn(async move {
			if stabilize_interval > 0 {
				let mut interval = tokio::time::interval(
					tokio::time::Duration::from_millis(stabilize_interval)
				);

				tokio::select! {
					_ = async {
						interval.tick().await;
						server.stabilize().await;
					} => (),
					_ = stabilize_rx.changed() => {
						debug!("{}: stabilize task stopped gracefully", server.node);
					}
				};
			}
		});

		// Periodically refresh finger table
		let mut server = self.clone();
		let mut fix_finger_rx = rx.clone();
		let fix_finger_interval = self.config.fix_finger_interval;
		let fix_finger_handle = tokio::spawn(async move {
			if fix_finger_interval > 0 {
				let mut interval = tokio::time::interval(
					tokio::time::Duration::from_millis(fix_finger_interval)
				);
				// StdRng can be sent across threads
				let mut rng = rand::prelude::StdRng::from_entropy();

				tokio::select! {
					_ = async {
						interval.tick().await;
						let index = rng.gen_range(1..NUM_BITS);
						server.fix_finger(index).await;
					} => (),
					_ = fix_finger_rx.changed() => {
						debug!("{}: fix_finger task stopped gracefully", server.node);
					}
				};
			}
		});

		info!("{}: listening at {}", self.node, self.node.addr);
		// An aggregated handle for all tasks
		let joined_handle = future::join_all(vec![
			listener_handle,
			stabilize_handle,
			fix_finger_handle
		]);

		Ok(ServerManager {
			handle: joined_handle,
			tx: tx
		})
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
			debug!("{}: connecting to {}", self.node, node);
			let c = crate::client::setup_client(&node.addr).await;
			debug!("{}: connected to {}", self.node, node);
			let mut map = self.connection_map.write().unwrap();
			map.insert(node.id, c.clone());
			return c;
		}
	}

	// Figure 7: n.join
	pub async fn join(&mut self, node: &Node) -> DhtResult<()> {
		debug!("{}: joining {}", self.node, node);
		self.set_predecessor(None);
		let ctx = context::current();
		let n = self.get_connection(node).await;
		let succ_list = n.find_successor_list_rpc(ctx, self.node.id).await?;
		self.set_successor_list(succ_list);
		debug!("{}: joined {}", self.node, node);
		Ok(())
	}

	// Figure 7: n.stabilize
	pub async fn stabilize(&mut self) {
		let ctx = context::current();

		let successor_list = self.get_successor_list();
		for mut succ in successor_list.into_iter() {
			let mut n = self.get_connection(&succ).await;

			match n.get_predecessor_rpc(ctx).await {
				Ok(pred) => {
					// Update successors normally
					let x = match pred {
						Some(v) => v,
						None => {
							warn!("{}: empty predecessor of successor {}", self.node, succ);
							return;
						}
					};
					if in_range(x.id, self.node.id, succ.id) {
						// update connection because succ change
						n = self.get_connection(&x).await;
						// update succ
						succ = x;
					}

					// Get succ_list from new node
					// only update list if success
					if let Ok(mut new_succ_list) = n.get_successor_list_rpc(ctx).await {
						new_succ_list.pop();
						new_succ_list.insert(0, succ);
						self.set_successor_list(new_succ_list);
						// ignore error here because it can only be fixed by stabilizing again
						n.notify_rpc(ctx, self.node.clone()).await.unwrap_or(());
					}

					return;
				},
				Err(e) => {
					error!("{}: fail to stabilize: {}", self.node, e);
					// Fail to connect to succ, try next
				}
			}
		}
		panic!("{}: no live successors!", self.node);
	}

	// Figure 7: n.fix_fingers
	pub async fn fix_finger(&mut self, index: usize) {
		match self.find_successor_list(self.finger_table_start(index)).await {
			Ok(succ) => {
				let mut table = self.finger_table.write().unwrap();
				table[index] = succ[0].clone();
			},
			Err(e) => {
				error!("{}: failed to fix finger: {}", self.node, e);
			}
		};
	}

	// A modified version using successor_list
	// from figure 4: n.find_successor
	async fn find_successor_list(&mut self, id: Digest) -> DhtResult<Vec<Node>> {
		let n = self.find_predecessor(id).await?;
		let c = self.get_connection(&n).await;
		let succ_list = c.get_successor_list_rpc(context::current()).await?;
		Ok(succ_list)
	}

	// Figure 4: n.find_predecessor
	async fn find_predecessor(&mut self, id: Digest) -> DhtResult<Node> {
		debug!("{}: find_predecessor({})", self.node, id);
		let mut n = self.node.clone();
		let mut succ = self.get_successor();
		let mut conn = self.get_connection(&n).await;
		let ctx = context::current();

		// stop when id in (n, succ]
		while !(in_range(id, n.id, succ.id) || id == succ.id) {
			debug!("{}: find_predecessor range ({}, {}]", self.node, n.id, succ.id);
			n = conn.closest_preceding_finger_rpc(ctx, id).await?;
			conn = self.get_connection(&n).await;
			succ = conn.get_successor_rpc(ctx).await?;
		}
		debug!("{}: find_predecessor({}) returns {}", self.node, id, n);
		Ok(n)
	}

	// Figure 4: n.closest_preceding_finger
	async fn closest_preceding_finger(&mut self, id: Digest) -> Node {
		let table = self.finger_table.read().unwrap();
		for i in (0..NUM_BITS).rev() {
			let f = if i > 0 {
				table[i].clone()
			} else {
				// table[0] is maintained by successor_list[0]
				self.get_successor()
			};
			if in_range(f.id, self.node.id, id) {
				return f;
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

		debug!("{}: new predecessor set in notify: {}", self.node, node);
		self.set_predecessor(Some(node));
	}

	// Get key on the ring
	async fn get(&mut self, key: Key) -> DhtResult<Option<Value>> {
		// Try readiing from local replica first
		match self.store.get(&key) {
			Some(v) => return Ok(Some(v)),
			None => ()
		};

		// Fetch from the responsible node
		let id = calculate_hash(&key);
		let succ_list = self.find_successor_list(id).await?;
		for succ in succ_list.iter() {
			let c = self.get_connection(&succ).await;
			match c.get_local_rpc(context::current(), key.clone()).await {
				Ok(value) => return Ok(value),
				Err(e) => {
					error!("{}: fail to get key digest {} from {}: {}", self.node, id, succ, e);
					// Continue trying next replica
				}
			};
		}

		Err(NoLiveReplica(id))
	}

	// Set key on the ring
	async fn set(&mut self, key: Key, value: Option<Value>) -> DhtResult<()> {
		let id = calculate_hash(&key);
		let succ_list = self.find_successor_list(id).await?;
		let c = self.get_connection(&succ_list[0]).await;

		c.replicate_rpc(context::current(), key, value).await?;
		Ok(())
	}

	// Replicate key to (num - 1) successors and itself
	async fn replicate(&mut self, key: Key, value: Option<Value>) -> DhtResult<()> {
		// replicate it locally
		self.store.set(key.clone(), value.clone());

		// replicate data to (replication_factor - 1) nodes
		let num = (self.config.replication_factor - 1) as usize;
		if num > 0 {
			let ctx = context::current();
			// Must store conn because fut_list borrows them
			let mut conn_list = Vec::new();
			let mut fut_list = Vec::new();
			for i in 0..num {
				let node = self.successor_list.read().unwrap()[i].clone();
				let c = self.get_connection(&node).await;
				conn_list.push(c);
			}

			for c in conn_list.iter() {
				let k = key.clone();
				let v = value.clone();
				fut_list.push(c.set_local_rpc(ctx, k, v));
			}

			// replicate data concurrently
			future::join_all(fut_list)
				.await
				.into_iter()
				.collect::<Result<Vec<_>, _>>()?;
		}
		Ok(())
	}
}

#[tarpc::server]
impl NodeService for NodeServer {
	async fn get_node_rpc(self, _: context::Context) -> Node {
		self.node.clone()
	}

	async fn get_predecessor_rpc(self, _: context::Context) -> Option<Node> {
		self.get_predecessor()
	}

	async fn get_successor_rpc(self, _: context::Context) -> Node {
		self.get_successor()
	}

	async fn get_successor_list_rpc(self, _: context::Context) -> Vec<Node> {
		self.get_successor_list()
	}

	async fn find_successor_list_rpc(mut self, _: context::Context, id: Digest) -> Vec<Node> {
		loop {
			for i in 0..(self.config.retry_limit+1) {
				match self.find_successor_list(id).await {
					Ok(succ_list) => return succ_list,
					Err(e) => {
						error!("{}: find_successor_list_rpc failed (retry {}): {}", self.node, i, e);
						tokio::time::sleep(
							tokio::time::Duration::from_millis(self.config.retry_interval)
						).await;
					}
				};
			}

			error!("{}: find_successor_list_rpc retry limit reached", self.node);
			// call stabilize to update successor_list
			self.stabilize().await;
		}
	}

	async fn find_predecessor_rpc(mut self, _: context::Context, id: Digest) -> Node {
		loop {
			for i in 0..(self.config.retry_limit+1) {
				match self.find_predecessor(id).await {
					Ok(succ_list) => return succ_list,
					Err(e) => {
						error!("{}: find_predecessor_rpc failed (retry {}): {}", self.node, i, e);
						tokio::time::sleep(
							tokio::time::Duration::from_millis(self.config.retry_interval)
						).await;
					}
				};
			}

			error!("{}: find_predecessor_rpc retry limit reached", self.node);
			// call stabilize to update successor_list
			self.stabilize().await;
		}
	}

	async fn closest_preceding_finger_rpc(mut self, _: context::Context, id: Digest) -> Node {
		self.closest_preceding_finger(id).await
	}

	async fn notify_rpc(mut self, _: context::Context, node: Node) {
		self.notify(node).await
	}

	async fn stabilize_rpc(mut self, _: context::Context) {
		self.stabilize().await
	}

	async fn get_local_rpc(self, _: context::Context, key: Key) -> Option<Value> {
		self.store.get(&key)
	}

	async fn set_local_rpc(self, _: context::Context, key: Key, value: Option<Value>) {
		self.store.set(key, value)
	}

	async fn get_rpc(mut self, _: context::Context, key: Key) -> Option<Value> {
		loop {
			for i in 0..(self.config.retry_limit+1) {
				match self.get(key.clone()).await {
					Ok(value) => return value,
					Err(e) => {
						error!("{}: get_rpc failed (retry {}): {}", self.node, i, e);
						tokio::time::sleep(
							tokio::time::Duration::from_millis(self.config.retry_interval)
						).await;
					}
				};
			}

			error!("{}: get_rpc retry limit reached", self.node);
			// call stabilize to update successor_list
			self.stabilize().await;
		}
	}

	async fn set_rpc(mut self, _: context::Context, key: Key, value: Option<Value>) {
		loop {
			for i in 0..(self.config.retry_limit+1) {
				match self.set(key.clone(), value.clone()).await {
					Ok(_) => return,
					Err(e) => {
						error!("{}: set_rpc failed (retry {}): {}", self.node, i, e);
						tokio::time::sleep(
							tokio::time::Duration::from_millis(self.config.retry_interval)
						).await;
					}
				};
			}

			error!("{}: set_rpc retry limit reached", self.node);
			// call stabilize to update successor_list
			self.stabilize().await;
		}
	}

	async fn replicate_rpc(mut self, _: context::Context, key: Key, value: Option<Value>) {
		loop {
			for i in 0..(self.config.retry_limit+1) {
				match self.replicate(key.clone(), value.clone()).await {
					Ok(_) => return,
					Err(e) => {
						error!("{}: replicate_rpc failed (retry {}): {}", self.node, i, e);
						tokio::time::sleep(
							tokio::time::Duration::from_millis(self.config.retry_interval)
						).await;
					}
				};
			}

			error!("{}: replicate_rpc retry limit reached", self.node);
			// call stabilize to update successor_list
			self.stabilize().await;
		}
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
	async fn test_node_metadata() -> DhtResult<()> {
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

		// Disable auto fix_finger and stabilize
		let config = Config {
			fix_finger_interval: 0,
			stabilize_interval: 0,
			..Config::default()
		};
		let mut s0 = NodeServer::new(n0.clone(), config.clone());
		let m0 = s0.start(None).await?;
		s0.stabilize().await;
		// single-node ring
		assert_eq!(s0.get_predecessor().unwrap().id, 0);
		assert_eq!(s0.get_successor().id, 0);


		// Node 1 joins node 0
		let mut s1 = NodeServer::new(n1.clone(), config.clone());
		let m1 = s1.start(Some(n0.clone())).await?;
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
		let mut s3 = NodeServer::new(n3.clone(), config.clone());
		let m3 = s3.start(Some(n1.clone())).await?;
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
			assert_eq!(s0.get_successor().id, 1);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s1).await;
		{
			let table = s1.finger_table.read().unwrap();
			assert_eq!(s1.get_successor().id, 3);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s3).await;
		{
			let table = s3.finger_table.read().unwrap();
			assert_eq!(s3.get_successor().id, 0);
			assert_eq!(table[1].id, 0);
			assert_eq!(table[2].id, 0);
		}


		// Node 6 joins node 0
		let mut s6 = NodeServer::new(n6.clone(), config.clone());
		let m6 = s6.start(Some(n0.clone())).await?;
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
			assert_eq!(s0.get_successor().id, 1);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 6);
		}
		fix_all_fingers(&mut s1).await;
		{
			let table = s1.finger_table.read().unwrap();
			assert_eq!(s1.get_successor().id, 3);
			assert_eq!(table[1].id, 3);
			assert_eq!(table[2].id, 6);
		}
		fix_all_fingers(&mut s3).await;
		{
			let table = s3.finger_table.read().unwrap();
			assert_eq!(s3.get_successor().id, 6);
			assert_eq!(table[1].id, 6);
			assert_eq!(table[2].id, 0);
		}
		fix_all_fingers(&mut s6).await;
		{
			let table = s6.finger_table.read().unwrap();
			assert_eq!(s6.get_successor().id, 0);
			assert_eq!(table[1].id, 0);
			// different from figure 6 because of different NUM_BITS
			assert_eq!(table[2].id, 0);
		}

		m0.stop().await?;
		m1.stop().await?;
		m3.stop().await?;
		m6.stop().await?;
		Ok(())
	}
}
