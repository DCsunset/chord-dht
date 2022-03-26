use std::default::Default;

#[derive(Clone)]
pub struct Config {
	// replicate data in k successors
	pub replication_factor: u64,
	// allow asynchronous replication (lazy)
	pub async_replication: bool,
	// allow reading from repicas
	pub read_replica: bool,
	pub stabilize_interval: u64,
	pub fix_finger_interval: u64
}

impl Default for Config {
	fn default() -> Self {
		Self {
			replication_factor: 1,
			async_replication: true,
			read_replica: true,
			stabilize_interval: 50,
			fix_finger_interval: 50
		}
	}
}
