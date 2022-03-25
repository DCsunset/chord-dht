use chord_dht::{
	core::{
		ring::{
			NUM_BITS,
			in_range
		},
		NodeServer,
		calculate_hash
	},
};
use rand::Rng;


pub async fn fix_all_fingers(server: &mut NodeServer) {
	for i in 1..NUM_BITS {
		server.fix_finger(i).await;
	}
}

// Generate key whose digest is in range (start, end]
pub fn generate_key_in_range<T: Rng>(rng: &mut T, start: u64, end: u64) -> Vec<u8> {
	// gen 8-byte key
	loop {
		let key = rng.gen::<[u8; 8]>();
		let digest = calculate_hash(&key);
		if in_range(digest, start, end) || digest == end {
			return Vec::from(key);
		}
	}
}
