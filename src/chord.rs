pub mod node;
pub use node::*;

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher}
};

pub fn calculate_hash(addr: &String) -> u64 {
	let mut hasher = DefaultHasher::new();
	addr.hash(&mut hasher);
	hasher.finish()
}

pub fn construct_node(addr: &String) -> Node {
	Node {
		addr: addr.clone(),
		id: calculate_hash(addr)
	}
}
