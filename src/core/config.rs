use std::default::Default;

#[derive(Clone)]
pub struct Config {
	/// Tolerate at most n node failures
	pub fault_tolerance: u64,
	/// Replicate data in k successors (1 <= k <= n+1)
	pub replication_factor: u64,
	/// Interval to periodically stabilize (in ms)
	pub stabilize_interval: u64,
	/// Interval to periodically fix finger table (in ms)
	pub fix_finger_interval: u64,
	/// Max number of concurrent connections in buffer
	pub max_connections: u64,
	/// Retrying n times if the RPC fails
	pub retry_limit: u64,
	/// Interval to retry connecting to the same node (in ms)
	pub retry_interval: u64
}

impl Default for Config {
	fn default() -> Self {
		Self {
			fault_tolerance: 0,
			replication_factor: 1,
			max_connections: 16,
			stabilize_interval: 200,
			fix_finger_interval: 200,
			retry_limit: 2,
			retry_interval: 50
		}
	}
}
