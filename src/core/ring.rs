use std::mem::size_of;

pub type Digest = u64;
// number of bits
pub const NUM_BITS: usize = size_of::<Digest>() * 8;

// Strictly in range: id in (start, end)
pub fn in_range(id: Digest, start: Digest, end: Digest) -> bool {
	if end > start {
		// (start, id, end)
		id > start && id < end
	} 
	else {
		// end <= start
		// case 1: (start, id, end + MAX_VAL)
		// case 2: (start, id + MAX_VAL, end + MAX_VAL)
		id > start || id < end
	}
}
