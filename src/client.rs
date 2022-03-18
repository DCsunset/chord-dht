use crate::chord::NodeServiceClient;

use tarpc::tokio_serde::formats::Bincode;

pub async fn setup_client(addr: &str) -> NodeServiceClient {
	let transport = tarpc::serde_transport::tcp::connect(addr,Bincode::default);
	NodeServiceClient::new(tarpc::client::Config::default(), transport.await.unwrap()).spawn()
}
