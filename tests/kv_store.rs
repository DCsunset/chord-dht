use chord_dht::{
	core::{
		ring::{
			NUM_BITS,
			in_range
		},
		config::*,
		Node,
		NodeServer,
		calculate_hash
	},
	client::setup_client
};
use rand::{
	prelude::*,
	Rng
};
use tarpc::context;

async fn fix_all_fingers(server: &mut NodeServer) {
	for i in 1..NUM_BITS {
		server.fix_finger(i).await;
	}
}

// Generate key whose digest is in range (start, end]
fn generate_key_in_range<T: Rng>(rng: &mut T, start: u64, end: u64) -> Vec<u8> {
	// gen 8-byte key
	loop {
		let key = rng.gen::<[u8; 8]>();
		let digest = calculate_hash(&key);
		if in_range(digest, start, end) || digest == end {
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

	let mut node_config = NodeConfig {
		node: n0.clone(),
		replication_factor: 1
	};
	let mut s0 = NodeServer::new(&node_config);
	// Disable auto fix_finger and stabilize
	let mut runtime_config = RuntimeConfig {
		join_node: None,
		fix_finger_interval: 0,
		stabilize_interval: 0
	};
	s0.start(&runtime_config).await?;
	let c0 = setup_client(&n0.addr).await;
	s0.stabilize().await;

	node_config.node = n1.clone();
	let mut s1 = NodeServer::new(&node_config);
	runtime_config.join_node = Some(n0.clone());
	s1.start(&runtime_config).await?;
	let c1 = setup_client(&n1.addr).await;
	s1.stabilize().await;
	s0.stabilize().await;

	fix_all_fingers(&mut s0).await;
	fix_all_fingers(&mut s1).await;

	node_config.node = n3.clone();
	let mut s3 = NodeServer::new(&node_config);
	runtime_config.join_node = Some(n1.clone());
	s3.start(&runtime_config).await?;
	let c3 = setup_client(&n3.addr).await;
	s3.stabilize().await;
	s1.stabilize().await;
	s0.stabilize().await;

	// See finger table in Figure 3b
	fix_all_fingers(&mut s0).await;
	fix_all_fingers(&mut s1).await;
	fix_all_fingers(&mut s3).await;

	// Node 6 joins node 0
	node_config.node = n6.clone();
	let mut s6 = NodeServer::new(&node_config);
	runtime_config.join_node = Some(n0.clone());
	s6.start(&runtime_config).await?;
	let c6 = setup_client(&n6.addr).await;
	s6.stabilize().await;
	s3.stabilize().await;
	s1.stabilize().await;
	s0.stabilize().await;

	// See finger table in Figure 6a
	fix_all_fingers(&mut s0).await;
	fix_all_fingers(&mut s1).await;
	fix_all_fingers(&mut s3).await;
	fix_all_fingers(&mut s6).await;


	let mut rng = StdRng::seed_from_u64(0);
	// k1 should be placed at n1
	let k1 = generate_key_in_range(&mut rng, n0.id, n1.id);
	let v1 = vec![1u8];
	c0.set_rpc(context::current(), k1.clone(), Some(v1.clone())).await.unwrap();
	assert_eq!(c0.get_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);
	assert_eq!(c0.get_local_rpc(context::current(), k1.clone()).await.unwrap(), None);
	assert_eq!(c1.get_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);
	assert_eq!(c1.get_local_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);

	// k2 should be placed at n3
	let k2 = generate_key_in_range(&mut rng, n1.id, n3.id);
	let v2 = vec![2u8];
	c6.set_rpc(context::current(), k2.clone(), Some(v2.clone())).await.unwrap();
	assert_eq!(c0.get_rpc(context::current(), k2.clone()).await.unwrap().unwrap(), v2);
	assert_eq!(c0.get_local_rpc(context::current(), k2.clone()).await.unwrap(), None);
	assert_eq!(c3.get_rpc(context::current(), k2.clone()).await.unwrap().unwrap(), v2);
	assert_eq!(c3.get_local_rpc(context::current(), k2.clone()).await.unwrap().unwrap(), v2);

	// delete k1
	c3.set_rpc(context::current(), k1.clone(), None).await.unwrap();
	assert_eq!(c0.get_rpc(context::current(), k1.clone()).await.unwrap(), None);
	assert_eq!(c1.get_local_rpc(context::current(), k1.clone()).await.unwrap(), None);

	Ok(())
}