// Test based on Figure 3 (b) in Chord paper
use chord_rust::{chord::{Node, NodeServer, Config}, client::setup_client};
use tarpc::context;

/// Test figure 3b, 5a
#[tokio::test]
async fn test_node_metadata() -> anyhow::Result<()> {
	env_logger::init();

	// Node 0
	let n0 = Node {
		addr: "localhost:9800".to_string(),
		id: 0
	};
	// Node 1
	let n1 = Node {
		addr: "localhost:9801".to_string(),
		id: 1
	};
	// Node 3
	let n3 = Node {
		addr: "localhost:9803".to_string(),
		id: 3
	};
	// Node 6
	let n6 = Node {
		addr: "localhost:9806".to_string(),
		id: 6
	};

	let mut s0 = NodeServer::new(&n0);
	// Disable auto fix_finger and stabilize
	let mut config = Config {
		join_node: None,
		fix_finger_interval: 0,
		stabilize_interval: 0
	};
	s0.start(&config).await?;
	// Wait for server to start
	let c0 = setup_client(&n0.addr).await;
	c0.stabilize_rpc(context::current()).await.unwrap();
	// single-node ring
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	assert_eq!(c0.get_successor_rpc(context::current()).await.unwrap().id, 0);


	// Node 1 joins node 0
	let mut s1 = NodeServer::new(&n1);
	config.join_node = Some(n0.clone());
	s1.start(&config).await?;
	let c1 = setup_client(&n1.addr).await;
	assert_eq!(c1.get_successor_rpc(context::current()).await.unwrap().id, 0);

	// Stabilize c1 first to notify c0
	c1.stabilize_rpc(context::current()).await.unwrap();
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	c0.stabilize_rpc(context::current()).await.unwrap();
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	assert_eq!(c0.get_successor_rpc(context::current()).await.unwrap().id, 1);
	assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	assert_eq!(c1.get_successor_rpc(context::current()).await.unwrap().id, 0);


	// Node 3 joins node 1
	let mut s3 = NodeServer::new(&n3);
	config.join_node = Some(n1.clone());
	s3.start(&config).await?;
	let c3 = setup_client(&n3.addr).await;
	c3.stabilize_rpc(context::current()).await.unwrap();
	c1.stabilize_rpc(context::current()).await.unwrap();
	c0.stabilize_rpc(context::current()).await.unwrap();

	assert_eq!(c3.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 3);


	// Node 6 joins node 0
	let mut s6 = NodeServer::new(&n6);
	config.join_node = Some(n0.clone());
	s6.start(&config).await?;
	let c6 = setup_client(&n6.addr).await;
	c6.stabilize_rpc(context::current()).await.unwrap();
	c3.stabilize_rpc(context::current()).await.unwrap();
	c1.stabilize_rpc(context::current()).await.unwrap();
	c0.stabilize_rpc(context::current()).await.unwrap();
	assert_eq!(c6.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 3);
	assert_eq!(c3.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 6);

	Ok(())
}
