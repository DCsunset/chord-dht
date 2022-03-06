use std::{mem::size_of, collections::HashMap};
use rand::Rng;

use tarpc::{context};
use futures::future;

type Digest = u64;
// number of bits
const NUM_BITS: usize = size_of::<Digest>() * 8;

// Data part of the node
#[derive(Debug, Clone)]
struct NodeData {
	addr: String,
	id: Digest,
	successor: Digest,
	predecessor: Digest
}

#[derive(Clone)]
struct Finger {
	start: Digest,
	node: Digest
}

#[tarpc::service]
trait NodeRPC {
	async fn get_data() -> NodeData;

	async fn find_successor(id: Digest) -> Digest;
	async fn find_predecessor(id: Digest) -> NodeData;
	async fn closest_preceding_finger(id: Digest) -> Digest;
	async fn notify(id: Digest);
}

pub struct Node {
	data: NodeData,
	finger_table: [Finger; NUM_BITS],
	// connection to remote nodes
	connection_map: HashMap<Digest, Node>
}

impl Node {
	pub fn new() -> Node {
		Node {
			data: NodeData {
				id: 0,
				successor: 0,
				predecessor: 0,
				addr: String::from("")
			},
			finger_table: [Finger {
				start: 0,
				node: 0
			}; 64],
			connection_map: HashMap::new()
		}
	}

	pub async fn join(&mut self, id: Digest) {
		self.data.predecessor = 0;
		let node = self.connection_map.get(&id).unwrap();
		self.data.successor = node.find_successor(context::current(), id).await;
	}

	pub async fn stabilize(&mut self) {
		let ctx = context::current();
		let node = self.connection_map.get(&self.data.successor).unwrap();
		let x = node.get_data(ctx).await.predecessor;
		if x > self.data.id && x < self.data.successor {
			self.data.successor = x;
		}
		node.notify(ctx, self.data.id).await;
	}

	pub async fn fix_fingers(&mut self) {
		let mut rng = rand::thread_rng();
		let i = rng.gen_range(1..NUM_BITS);
		self.finger_table[i].node = self.find_successor(context::current(), self.finger_table[i].start).await;
	}
}

impl NodeRPC for Node {
	type GetDataFut = future::Ready<NodeData>;
	fn get_data(self, ctx: context::Context) -> Self::GetDataFut {
		future::ready(self.data)
	}

	type FindSuccessorFut = future::Ready<Digest>;
	fn find_successor(self, ctx: context::Context, id: Digest) -> Self::FindSuccessorFut {
		let n = self.find_predecessor(ctx, id).await;
		future::ready(n.successor)
	}

	type FindPredecessorFut = future::Ready<NodeData>;
	fn find_predecessor(self, ctx: context::Context, id: Digest) -> Self::FindPredecessorFut {
		let n = self.data;
		while id > n.id && id < n.successor {
			let node = self.connection_map.get(&n.id);
			let n_id = node.unwrap().closest_preceding_finger(ctx, id).await;
		}
		future::ready(n)
	}

	type ClosestPrecedingFingerFut = future::Ready<Digest>;
	fn closest_preceding_finger(self, _: context::Context, id: Digest) -> Self::ClosestPrecedingFingerFut {
		for i in (0..NUM_BITS).rev() {
			let n = self.finger_table[i].node;
			if n > id && n < self.data.id {
				return future::ready(n);
			}
		}
		future::ready(self.data.id)
	}

	type NotifyFut = future::Ready<()>;
	fn notify(self, _: context::Context, id: Digest) -> Self::NotifyFut {
		if self.data.predecessor == 0 || (id > self.data.predecessor && id < self.data.id) {
			self.data.predecessor = id;
		}
		future::ready(())
	}
}
