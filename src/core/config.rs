use std::default::Default;

#[derive(Clone)]
pub struct Config {
	// tolerate max n node failures
	pub fault_tolerance: u64,
	// replicate data in k successors (1 <= k <= n+1)
	pub replication_factor: u64,
	pub stabilize_interval: u64,
	pub fix_finger_interval: u64,
	/// max number of concurrent connections buffered
	pub max_connections: u64
}

impl Default for Config {
	fn default() -> Self {
		Self {
			fault_tolerance: 0,
			replication_factor: 1,
			max_connections: 16,
			stabilize_interval: 200,
			fix_finger_interval: 200
		}
	}
}
