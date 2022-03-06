use std::{mem::size_of, collections::HashMap};
use rand::Rng;

use tarpc::{context};
use futures::future;

type Digest = u64;
// number of bits
const NUM_BITS: usize = size_of::<Digest>() * 8;

// Data part of the node
#[derive(Debug, Clone)]
struct Node {
	addr: String,
	id: Digest
}

#[derive(Clone)]
struct Finger {
	start: Digest,
	node: Option<Node>
}

#[tarpc::service]
trait NodeService {
	async fn get_node() -> Node;
	async fn get_predecessor() -> Option<Node>;
	async fn get_successor() -> Option<Node>;

	async fn find_successor(id: Digest) -> Node;
	async fn find_predecessor(id: Digest) -> Node;
	async fn closest_preceding_finger(id: Digest) -> Node;
	async fn notify(node: Node);
}

pub struct NodeServer {
	node: Node,
	successor: Option<Node>,
	predecessor: Option<Node>,
	finger_table: [Finger; NUM_BITS],
	// connection to remote nodes
	connection_map: HashMap<Digest, NodeServiceClient>
}

impl NodeServer {
	pub fn new(node: &Node) -> NodeServer {
		NodeServer {
			node: node.clone(),
			successor: None,
			predecessor: None,
			finger_table: [Finger {
				start: 0,
				node: None
			}; 64],
			connection_map: HashMap::new()
		}
	}
	
	pub async fn get_connection(&mut self, node: &Node) -> &NodeServiceClient {
		match self.connection_map.get(&node.id) {
			Some(c) => c,
			// TODO: connect to the node
			None => panic!()
		}
	}

	pub async fn join(&mut self, node: &Node) {
		self.predecessor = None;
		let n = self.get_connection(node).await;
		self.successor = Some(n.find_successor(context::current(), node.id).await.unwrap());
	}

	pub async fn stabilize(&mut self) {
		let ctx = context::current();
		let successor = match self.successor.as_ref() {
			Some(s) => s.clone(),
			None => return
		};

    let self_node = self.node.clone();
		let n= self.get_connection(&successor).await;
		let x= match n.get_predecessor(ctx).await.unwrap() {
			Some(v) => v,
			// Empty predecessor (TODO: log)
			None => return
		};
		n.notify(ctx, self_node).await;

		if x.id > self.node.id && x.id < successor.id {
			self.successor = Some(x);
		}
	}

	pub async fn fix_fingers(&mut self) {
		let mut rng = rand::thread_rng();
		let i = rng.gen_range(1..NUM_BITS);
		self.finger_table[i].node = Some(self.find_successor(context::current(), self.finger_table[i].start).await);
	}
}

impl NodeService for NodeServer {
	type GetNodeFut = future::Ready<Node>;
	fn get_node(self, ctx: context::Context) -> Self::GetNodeFut {
		future::ready(self.node.clone())
	}

	type GetPredecessorFut = future::Ready<Option<Node>>;
	fn get_predecessor(self, ctx: context::Context) -> Self::GetPredecessorFut {
		future::ready(self.predecessor.clone())
	}

	type GetSuccessorFut = future::Ready<Option<Node>>;
	fn get_successor(self, ctx: context::Context) -> Self::GetPredecessorFut {
		future::ready(self.successor.clone())
	}

	// Figure 4: n.find_successor
	type FindSuccessorFut = future::Ready<Node>;
	fn find_successor(self, ctx: context::Context, id: Digest) -> Self::FindSuccessorFut {
		let n = self.find_predecessor(ctx, id).await;
		let node = self.get_connection(&n).await;
		let succ = node.get_successor(context::current()).await.unwrap().unwrap();
		future::ready(succ)
	}

	// Figure 4: n.find_predecessor
	type FindPredecessorFut = future::Ready<Node>;
	fn find_predecessor(self, ctx: context::Context, id: Digest) -> Self::FindPredecessorFut {
		let n = self.node;
		// TODO: check when its empty
		let succ = self.successor.unwrap();
		while id > n.id && id < succ.id {
			let node = self.get_connection(&n).await;
			n = node.closest_preceding_finger(ctx, id).await;
			let new_node = self.get_connection(&n).await;
			// TODO: handle empty
      succ = new_node.get_successor(context::current()).await.unwrap().unwrap();
		}
		future::ready(n)
	}

	// Figure 4: n.closest_preceding_finger
	type ClosestPrecedingFingerFut = future::Ready<Node>;
	fn closest_preceding_finger(self, _: context::Context, id: Digest) -> Self::ClosestPrecedingFingerFut {
		for i in (0..NUM_BITS).rev() {
			match self.finger_table[i].node.as_ref() {
				Some(n) => if n.id > id && n.id < self.node.id {
					return future::ready(n.clone())
				},
				None => ()
			};
		}
		future::ready(self.node.clone())
	}

	type NotifyFut = future::Ready<()>;
	fn notify(self, _: context::Context, node: Node) -> Self::NotifyFut {
    let new_pred = match self.predecessor {
			Some(v) => if node.id > v.id && node.id < self.node.id {
				node
			} else {
				v
			},
			None => node
		};
    self.predecessor = Some(new_pred);
		future::ready(())
	}
}
