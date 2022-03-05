use tarpc::{context};
use futures::future;

#[tarpc::service]
trait NodeRPC {
	async fn find_successor(id: u64) -> u64;
}

pub struct Node {
	pub id: u64
}

impl NodeRPC for Node {
	type FindSuccessorFut = future::Ready<u64>;
	
	fn find_successor(self, _: context::Context, id: u64) -> Self::FindSuccessorFut {
		future::ready(self.id)
	}
}
