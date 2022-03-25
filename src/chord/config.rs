use super::Node;
use std::default::Default;

pub struct NodeConfig {
	pub node: Node,
	// replicate data in k successors
	pub replication_factor: u32
}

pub struct RuntimeConfig {
	pub join_node: Option<Node>,
	// interval in ms (0 means disabling it)
	pub stabilize_interval: u32,
	pub fix_finger_interval: u32,
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		RuntimeConfig {
			join_node: None,
			stabilize_interval: 50,
			fix_finger_interval: 50
		}
	}
}
