// Test based on Figure 3 (b) in Chord paper
use chord_rust::{chord::Node, server::start_server, client::setup_client};

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

	let s0 = start_server(n0.clone(), None);
	tokio::spawn(s0);
	let c0 = setup_client(&n0.addr).await;

	let s1 = start_server(n1.clone(), Some(n0.clone()));
	tokio::spawn(s1);
	let c1 = setup_client(&n1.addr).await;

	let s3 = start_server(n3.clone(), Some(n1.clone()));
	tokio::spawn(s3);
	let c3 = setup_client(&n3.addr).await;
}
