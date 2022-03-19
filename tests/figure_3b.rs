// Test based on Figure 3 (b) in Chord paper
use chord_rust::{chord::{Node, NodeServer}, client::setup_client};
use tarpc::context;
use std::time::Duration;

#[tokio::test]
async fn test_figure_3b() {
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

	let s0 = NodeServer::new(&n0);
	tokio::spawn(s0.start(None));
	tokio::time::sleep(Duration::from_millis(50)).await;
	let c0 = setup_client(&n0.addr).await;
	c0.stabilize_rpc(context::current()).await.unwrap();
	// single-node ring
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	assert_eq!(c0.get_successor_rpc(context::current()).await.unwrap().id, 0);

	// Node 1 joins node 0
	let s1 = NodeServer::new(&n1);
	tokio::spawn(s1.start(Some(n0.clone())));
	tokio::time::sleep(Duration::from_millis(50)).await;
	let c1 = setup_client(&n1.addr).await;
	assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().is_none(), true);
	assert_eq!(c1.get_successor_rpc(context::current()).await.unwrap().id, 0);

	// Stabilize c1 first to notify c0
	c1.stabilize_rpc(context::current()).await.unwrap();
	assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	c0.stabilize_rpc(context::current()).await.unwrap();
	assert_eq!(c0.get_successor_rpc(context::current()).await.unwrap().id, 1);
	// Stabilize c1 again to set predecessor
	c1.stabilize_rpc(context::current()).await.unwrap();

	assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);


	// let s3 = start_server(n3.clone(), Some(n1.clone()));
	// tokio::spawn(s3);
	// tokio::time::sleep(Duration::from_millis(50)).await;
	// let c3 = setup_client(&n3.addr).await;
	// c0.stabilize_rpc(context::current()).await.unwrap();
	// c1.stabilize_rpc(context::current()).await.unwrap();
	// c3.stabilize_rpc(context::current()).await.unwrap();

	// assert_eq!(c3.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 1);
	// assert_eq!(c1.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 0);
	// assert_eq!(c0.get_predecessor_rpc(context::current()).await.unwrap().unwrap().id, 3);
}
