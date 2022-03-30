use chord_dht::{
	core::{
		config::*,
		Node,
		NodeServer
	},
	client::setup_client
};
use rand::prelude::*;
use tarpc::context;

// Common mod in tests
mod common;
use common::*;

/// Test kv store operations
/// based on the ring of figure 5a but with a larger range
#[tokio::test]
async fn test_replication() -> anyhow::Result<()> {
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

	// With replication factor of 3
	let config = Config {
		fault_tolerance: 2,
		replication_factor: 3,
		fix_finger_interval: 0,
		stabilize_interval: 0,
		..Config::default()
	};
	// Node 1 initializes
	let mut s0 = NodeServer::new(n0.clone(), config.clone());
	s0.start(None).await?;
	let c0 = setup_client(&n0.addr).await;
	s0.stabilize().await;

	// Node 1 joins node 0
	let mut s1 = NodeServer::new(n1.clone(), config.clone());
	s1.start(Some(n0.clone())).await?;
	let c1 = setup_client(&n1.addr).await;
	s1.stabilize().await;
	s0.stabilize().await;

	fix_all_fingers(&mut s0).await;
	fix_all_fingers(&mut s1).await;

	// Node 3 joins node 1
	let mut s3 = NodeServer::new(n3.clone(), config.clone());
	s3.start(Some(n1.clone())).await?;
	let c3 = setup_client(&n3.addr).await;
	s3.stabilize().await;
	s1.stabilize().await;
	s0.stabilize().await;

	// See finger table in Figure 3b
	fix_all_fingers(&mut s0).await;
	fix_all_fingers(&mut s1).await;
	fix_all_fingers(&mut s3).await;

	// Node 6 joins node 0
	let mut s6 = NodeServer::new(n6.clone(), config.clone());
	s6.start(Some(n0.clone())).await?;
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
	// k1 should be placed at n1, n3, n6
	let k1 = generate_key_in_range(&mut rng, n0.id, n1.id);
	let v1 = vec![1u8];
	c0.set_rpc(context::current(), k1.clone(), Some(v1.clone())).await?;

	assert_eq!(c0.get_rpc(context::current(), k1.clone()).await?.unwrap(), v1);
	assert_eq!(c0.get_local_rpc(context::current(), k1.clone()).await.unwrap(), None);
	assert_eq!(c1.get_local_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);
	assert_eq!(c3.get_local_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);
	assert_eq!(c6.get_local_rpc(context::current(), k1.clone()).await.unwrap().unwrap(), v1);

	// k2 should be placed at n3, n6, n0
	let k2 = generate_key_in_range(&mut rng, n1.id, n3.id);
	let v2 = vec![2u8];
	c6.set_rpc(context::current(), k2.clone(), Some(v2.clone())).await?;

	assert_eq!(c1.get_rpc(context::current(), k2.clone()).await?.unwrap(), v2);
	assert_eq!(c1.get_local_rpc(context::current(), k2.clone()).await?, None);
	assert_eq!(c3.get_rpc(context::current(), k2.clone()).await?.unwrap(), v2);
	assert_eq!(c3.get_local_rpc(context::current(), k2.clone()).await?.unwrap(), v2);
	assert_eq!(c6.get_local_rpc(context::current(), k2.clone()).await?.unwrap(), v2);
	assert_eq!(c0.get_local_rpc(context::current(), k2.clone()).await?.unwrap(), v2);

	// delete k1 from n1, n3, n6
	c3.set_rpc(context::current(), k1.clone(), None).await?;

	assert_eq!(c0.get_rpc(context::current(), k1.clone()).await?, None);
	assert_eq!(c1.get_local_rpc(context::current(), k1.clone()).await?, None);
	assert_eq!(c3.get_local_rpc(context::current(), k1.clone()).await?, None);
	assert_eq!(c6.get_local_rpc(context::current(), k1.clone()).await?, None);

	Ok(())
}
