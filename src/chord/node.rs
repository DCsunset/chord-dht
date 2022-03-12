use std::{mem::size_of, collections::{HashMap, hash_map::Entry}};
use rand::Rng;
use tarpc::{context, serde::Serialize, serde::Deserialize, tokio_serde::formats::Bincode};
use futures::{future, executor};


type Digest = u64;
// number of bits
const NUM_BITS: usize = size_of::<Digest>() * 8;

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
	async fn get_successor_rpc() -> Option<Node>;

	async fn find_successor_rpc(id: Digest) -> Node;
	async fn find_predecessor_rpc(id: Digest) -> Node;
	async fn closest_preceding_finger_rpc(id: Digest) -> Node;
	async fn notify_rpc(node: Node);
}

#[derive(Clone)]
pub struct NodeServer {
	node: Node,
	successor: Option<Node>,
	predecessor: Option<Node>,
	finger_table: [Option<Node>; NUM_BITS as usize],
	// connection to remote nodes
	connection_map: HashMap<Digest, NodeServiceClient>
}

impl NodeServer {
	pub fn new(node: &Node) -> NodeServer {
		const INIT_FINGER: Option<Node> = None;
		NodeServer {
			node: node.clone(),
			successor: None,
			predecessor: None,
			finger_table: [INIT_FINGER; NUM_BITS],
			connection_map: HashMap::new()
		}
	}

	// async fn listen(self) -> anyhow::Result<()> {
	// 	let mut listener = tarpc::serde_transport::tcp::listen(&self.node.addr, Bincode::default).await?;
	// 	listener.config_mut().max_frame_length(usize::MAX);
	// 	listener
	// 		.filter(|r| future::ready(r.ok()))
	// 		.map(tarpc::server::BaseChannel::with_defaults)
	// 		.map(|channel| {
	// 			channel.execute(self.serve());
	// 		})
	// 		// Max 20 channels
	// 		.buffer_unordered(20)
	// 		.for_each(|_| async {})
	// 		.await;
	// 	Ok(())
	// }

	// Calculate start field of finger table (see Table 1)
	// k in [0, m)
	pub fn finger_table_start(&self, k: usize) -> u64 {
		(self.node.id + (1 << k)) % (NUM_BITS as u64)
	}
	
	async fn get_connection(&mut self, node: &Node) -> &NodeServiceClient {
		match self.connection_map.entry(node.id) {
			Entry::Occupied(c) => c.into_mut(),
			// connect to the node
			Entry::Vacant(m) => {
				let transport = tarpc::serde_transport::tcp::connect(node.addr.clone(),Bincode::default);
				let c = NodeServiceClient::new(tarpc::client::Config::default(), transport.await.unwrap()).spawn();
				m.insert(c)

			}
		}
	}

	// Figure 7: n.join
	pub async fn join(&mut self, node: &Node) {
		self.predecessor = None;
		let n = self.get_connection(node).await;
		self.successor = Some(n.find_successor_rpc(context::current(), node.id).await.unwrap());
	}

	// Figure 7: n.stabilize
	pub async fn stabilize(&mut self) {
		let ctx = context::current();
		let successor = match self.successor.as_ref() {
			Some(s) => s.clone(),
			None => return
		};

    let self_node = self.node.clone();
		let n= self.get_connection(&successor).await;
		let x= match n.get_predecessor_rpc(ctx).await.unwrap() {
			Some(v) => v,
			// Empty predecessor (TODO: log)
			None => return
		};
		n.notify_rpc(ctx, self_node).await.unwrap();

		if x.id > self.node.id && x.id < successor.id {
			self.successor = Some(x);
		}
	}

	// Figure 7: n.fix_fingers
	pub async fn fix_fingers(&mut self) {
		let mut rng = rand::thread_rng();
		let i = rng.gen_range(1..NUM_BITS);
		self.finger_table[i] = Some(self.find_successor(self.finger_table_start(i)).await);
	}

	// Figure 4: n.find_successor
	async fn find_successor(&mut self, id: Digest) -> Node {
		let n = self.find_predecessor(id).await;
		let node = self.get_connection(&n).await;
		node.get_successor_rpc(context::current()).await.unwrap().unwrap()
	}

	// Figure 4: n.find_predecessor
	async fn find_predecessor(&mut self, id: Digest) -> Node {
		let mut n = self.node.clone();
		// TODO: check when its empty
		let mut succ = self.successor.as_ref().unwrap().clone();
		while id > n.id && id < succ.id {
			let node = self.get_connection(&n).await;
			n = node.closest_preceding_finger_rpc(context::current(), id).await.unwrap();
			let new_node = self.get_connection(&n).await;
			// TODO: handle empty
      succ = new_node.get_successor_rpc(context::current()).await.unwrap().unwrap();
		}
		n
	}

	// Figure 4: n.closest_preceding_finger
	async fn closest_preceding_finger(&mut self, id: Digest) -> Node {
		for i in (0..NUM_BITS).rev() {
			match self.finger_table[i].as_ref() {
				Some(n) => if n.id > id && n.id < self.node.id {
					return n.clone();
				},
				None => ()
			};
		}
		self.node.clone()
	}

	// Figure 7: n.notify
	async fn notify(&mut self, node: Node) {
    let new_pred = match self.predecessor.as_ref() {
			Some(v) => if node.id > v.id && node.id < self.node.id {
				node
			} else {
				v.clone()
			},
			None => node
		};
    self.predecessor = Some(new_pred);
	}
}

#[tarpc::server]
impl NodeService for NodeServer {
	type GetNodeRpcFut = future::Ready<Node>;
	fn get_node_rpc(self, _: context::Context) -> Self::GetNodeRpcFut {
		future::ready(self.node.clone())
	}

	type GetPredecessorRpcFut = future::Ready<Option<Node>>;
	fn get_predecessor_rpc(self, _: context::Context) -> Self::GetPredecessorRpcFut {
		future::ready(self.predecessor.clone())
	}

	type GetSuccessorRpcFut = future::Ready<Option<Node>>;
	fn get_successor_rpc(self, _: context::Context) -> Self::GetPredecessorRpcFut {
		future::ready(self.successor.clone())
	}

	type FindSuccessorRpcFut = future::Ready<Node>;
	fn find_successor_rpc(mut self, _: context::Context, id: Digest) -> Self::FindSuccessorRpcFut {
		future::ready(
			executor::block_on(self.find_successor(id))
		)
	}

	type FindPredecessorRpcFut = future::Ready<Node>;
	fn find_predecessor_rpc(mut self, _: context::Context, id: Digest) -> Self::FindPredecessorRpcFut {
		future::ready(
			executor::block_on(self.find_predecessor(id))
		)
	}

	// Figure 4: n.closest_preceding_finger
	type ClosestPrecedingFingerRpcFut = future::Ready<Node>;
	fn closest_preceding_finger_rpc(mut self, _: context::Context, id: Digest) -> Self::ClosestPrecedingFingerRpcFut {
		future::ready(
			executor::block_on(self.closest_preceding_finger(id))
		)
	}

	type NotifyRpcFut = future::Ready<()>;
	fn notify_rpc(mut self, _: context::Context, node: Node) -> Self::NotifyRpcFut {
		future::ready(
			executor::block_on(self.notify(node))
		)
	}
}
