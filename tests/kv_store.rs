use chord_rust::chord::{Node, NodeServer, calculate_hash};
use rand::Rng;

// Generate key whose digest is in range (start, end]
fn generate_key_in_range<T: Rng>(rng: &mut T, start: u64, end: u64) -> Vec<u8> {
	// gen 8-byte key
	loop {
		let key = rng.gen::<[u8; 8]>();
		let digest = calculate_hash(&key);
		if digest > start && digest <= end {
			return Vec::from(key);
		}
	}
}

/// Test kv store operations
/// based on the ring of figure 5a but with a larger range
#[tokio::test]
async fn test_kv_store() -> anyhow::Result<()> {
	env_logger::init();
	// Node 0
	let n0 = Node {
		addr: "localhost:9800".to_string(),
		id: 0
	};
	// Node 1
	let n1 = Node {
		addr: "localhost:9801".to_string(),
		id: u64::MAX / 4
	};
	// Node 3
	let n3 = Node {
		addr: "localhost:9803".to_string(),
		id: u64::MAX / 4 * 2
	};
	// Node 6
	let n6 = Node {
		addr: "localhost:9806".to_string(),
		id: u64::MAX / 4 * 3
	};



	Ok(())
}
