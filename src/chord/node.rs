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
use crate::chord::{
	ring::*,
	config::*
};

// Data part of the node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
	pub id: Digest,
	pub addr: String
}

#[tarpc::service]
pub trait NodeService {
	async fn get_node_rpc() -> Node;
	async fn get_predecessor_rpc() -> Option<Node>;
	async fn get_successor_rpc() -> Node;

	async fn find_successor_rpc(id: Digest) -> Node;
	async fn find_predecessor_rpc(id: Digest) -> Node;
	async fn closest_preceding_finger_rpc(id: Digest) -> Node;
	async fn notify_rpc(node: Node);
	async fn stabilize_rpc();
}

// TODO: make fields share between different instances
#[derive(Clone)]
pub struct NodeServer {
	node: Node,
	// Successor is never None (for correctness)
	successor: Arc<RwLock<Node>>,
	predecessor: Arc<RwLock<Option<Node>>>,
	finger_table: Arc<RwLock<Vec<Node>>>,
	// connection to remote nodes
	connection_map: Arc<RwLock<HashMap<Digest, NodeServiceClient>>>
}

impl NodeServer {
	pub fn new(node: &Node) -> NodeServer {
		// init a ring with only one node
		// (see second part of n.join in Figure 6)
		let finger_table = vec![node.clone(); NUM_BITS];

		NodeServer {
			node: node.clone(),
			successor: Arc::new(RwLock::new(node.clone())),
			predecessor: Arc::new(RwLock::new(Some(node.clone()))),
			finger_table: Arc::new(RwLock::new(finger_table)),
			connection_map: Arc::new(RwLock::new(HashMap::new()))
		}
	}

	// Start the server
	pub async fn start(&mut self, config: &Config) -> anyhow::Result<tokio::task::JoinHandle<()>> {
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
		let stabilize_interval = config.fix_finger_interval;
		if stabilize_interval > 0 {
			tokio::spawn(async move {
				let mut interval = tokio::time::interval(
					chrono::Duration::milliseconds(stabilize_interval.into()).to_std().unwrap()
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
					chrono::Duration::milliseconds(fix_finger_interval.into()).to_std().unwrap()
				);
				// StdRng can be sent across threads
				let mut rng = rand::prelude::StdRng::from_entropy();
				// let mut s= server.clone();
				loop {
					interval.tick().await;
					let index = rng.gen_range(1..NUM_BITS);
					server.fix_fingers(index).await;
				}
			});
		}

		info!("Node {} listening at {}", self.node.id, self.node.addr);
		Ok(handle)
	}

	// Calculate start field of finger table (see Table 1)
	// k in [0, m)
	pub fn finger_table_start(&self, k: usize) -> u64 {
		(self.node.id + (1 << k)) % (NUM_BITS as u64)
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
		*self.predecessor.write().unwrap() = None;
		let n = self.get_connection(node).await;
		*self.successor.write().unwrap() = n.find_successor_rpc(context::current(), self.node.id).await.unwrap();
		debug!("Node {}: joined node {}", self.node.id, node.id);
	}

	// Figure 7: n.stabilize
	pub async fn stabilize(&mut self) {
		let ctx = context::current();
		let mut succ = self.successor.read().unwrap().clone();

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
			*self.successor.write().unwrap() = x.clone();
			// update succ
			succ = x;
		}

		// update connection because succ may change here
		let n = self.get_connection(&succ).await;
		n.notify_rpc(ctx, self_node).await.unwrap();
	}

	// Figure 7: n.fix_fingers
	pub async fn fix_fingers(&mut self, index: usize) {
		let succ = self.find_successor(self.finger_table_start(index)).await;
		let mut table = self.finger_table.write().unwrap();
		table[index] = succ;
	}

	// Figure 4: n.find_successor
	async fn find_successor(&mut self, id: Digest) -> Node {
		debug!("Node {}: find_successor({})", self.node.id, id);
		let n = self.find_predecessor(id).await;
		if n.id == self.node.id {
			return self.successor.read().unwrap().clone()
		}
		let node = self.get_connection(&n).await;
		node.get_successor_rpc(context::current()).await.unwrap()
	}

	// Figure 4: n.find_predecessor
	async fn find_predecessor(&mut self, id: Digest) -> Node {
		debug!("Node {}: find_predecessor({})", self.node.id, id);
		let mut n = self.node.clone();
		let mut succ = self.successor.read().unwrap().clone();

		// stop when id in (n, succ]
		while !(in_range(id, n.id, succ.id) || id == succ.id) {
			debug!("Node {}: find_predecessor range ({}, {}]", self.node.id, n.id, succ.id);
			let node = self.get_connection(&n).await;
			n = node.closest_preceding_finger_rpc(context::current(), id).await.unwrap();
			let new_node = self.get_connection(&n).await;
			succ = new_node.get_successor_rpc(context::current()).await.unwrap();
		}
		n
	}

	// Figure 4: n.closest_preceding_finger
	async fn closest_preceding_finger(&mut self, id: Digest) -> Node {
		let table = self.finger_table.read().unwrap();
		for i in (0..NUM_BITS).rev() {
			let n = &table[i];
			if in_range(n.id, id, self.node.id) {
				return n.clone();
			};
		}
		self.node.clone()
	}

	// Figure 7: n.notify
	async fn notify(&mut self, node: Node) {
		let mut pred = self.predecessor.write().unwrap();
		let new_pred = match pred.as_ref() {
			Some(p) => if in_range(node.id, p.id, self.node.id) {
				node
			} else {
				p.clone()
			},
			None => node
		};
		debug!("Node {}: new predecessor set in notify: {}", self.node.id, new_pred.id);
		*pred = Some(new_pred);
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
		let succ = self.successor.read().unwrap().clone();
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
}
