use crate::rpc::NodeServiceClient;
use tarpc::tokio_serde::formats::Bincode;
use log::info;

pub async fn setup_client(addr: &str) -> NodeServiceClient {
	info!("connecting to {}", addr);
	let transport = tarpc::serde_transport::tcp::connect(addr,Bincode::default).await.unwrap();
	info!("connected to {}", addr);
	NodeServiceClient::new(tarpc::client::Config::default(), transport).spawn()
}
