use super::Node;
use std::default::Default;

pub struct Config {
	pub join_node: Option<Node>,
	// interval in ms (0 means disabling it)
	pub stabilize_interval: u32,
	pub fix_finger_interval: u32
}

impl Default for Config {
	fn default() -> Self {
		Config {
			join_node: None,
			stabilize_interval: 50,
			fix_finger_interval: 50
		}
	}
}
