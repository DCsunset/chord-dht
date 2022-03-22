use std::{
	collections::{
		HashMap,
		hash_map::Entry
	},
	sync::{Arc, RwLock}
};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub trait KVStore {
	fn get(&self, key: &Key) -> Option<Value>;
	fn set(&self, key: Key, value: Option<Value>);
}

/// Thread-safe key-value data store
#[derive(Clone)]
pub struct DataStore {
	data: Arc<RwLock<HashMap<Key, Value>>>
}

impl DataStore {
	pub fn new() -> Self {
		DataStore {
			data: Arc::new(RwLock::new(HashMap::new()))
		}
	}
}

impl KVStore for DataStore {
	fn get(&self, key: &Key) -> Option<Value> {
		let data = self.data.read().unwrap();
		match data.get(key) {
			Some(v) => Some(v.clone()),
			None => None
		}
	}

	/**
	 * Set a key
	 * When value is None, remove that entry;
	 * otherwise, insert or update the entry.
	 */
	fn set(&self, key: Key, value: Option<Value>) {
		let mut data = self.data.write().unwrap();
		match data.entry(key) {
			Entry::Occupied(mut entry) => {
				match value {
					Some(v) => entry.insert(v),
					None => entry.remove()
				};
			},
			Entry::Vacant(entry) => {
				match value {
					Some(v) => {
						entry.insert(v);
					},
					None => ()
				};
			}
		};
	}
}
