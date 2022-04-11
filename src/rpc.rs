use crate::core::{
	ring::Digest,
	Node,
	data_store::{Key, Value}
};

#[tarpc::service]
pub trait NodeService {
	// Get fields at this node
	async fn get_node_rpc() -> Node;
	async fn get_predecessor_rpc() -> Option<Node>;
	async fn get_successor_rpc() -> Node;
	async fn get_successor_list_rpc() -> Vec<Node>;

	// Core functions for Chord
	async fn find_successor_list_rpc(id: Digest) -> Vec<Node>;
	async fn find_predecessor_rpc(id: Digest) -> Node;
	async fn closest_preceding_finger_rpc(id: Digest) -> Node;
	async fn notify_rpc(node: Node);
	async fn stabilize_rpc();

	// Get or set key locally
	async fn get_local_rpc(key: Key) -> Option<Value>;
	async fn set_local_rpc(key: Key, value: Option<Value>);

	// Get or set key on the ring
	async fn get_rpc(key: Key) -> Option<Value>;
	async fn set_rpc(key: Key, value: Option<Value>);

	// Replicate data at this node
	async fn replicate_rpc(key: Key, value: Option<Value>);
}
