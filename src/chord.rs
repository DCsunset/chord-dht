pub mod node;

pub use node::*;

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher}
};

pub fn calculate_hash(addr: &str) -> u64 {
	let mut hasher = DefaultHasher::new();
	addr.hash(&mut hasher);
	hasher.finish()
}

pub fn construct_node(addr: &str) -> Node {
	Node {
		addr: addr.to_string(),
		id: calculate_hash(addr)
	}
}
