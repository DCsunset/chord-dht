use chord_rust::chord::node::{Node, NodeServer, NodeService};
use futures::{future, prelude::*, executor};
use tarpc::{
	tokio_serde::formats::Bincode,
	server::{Channel, incoming::Incoming}};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let existing_node = Node {
		addr: "".to_string(),
		id: 0
	};
	let node = Node {
		addr: "".to_string(),
		id: 0
	};

	let mut listener = tarpc::serde_transport::tcp::listen(&node.addr, Bincode::default).await?;
	listener.config_mut().max_frame_length(usize::MAX);
	listener
    .filter_map(|r| future::ready(r.ok()))
    .map(tarpc::server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
    .map(|channel| {
			let mut server = NodeServer::new(&node);
			executor::block_on(server.join(&existing_node));

			channel.execute(server.serve())
		})
		// Max 20 channels
    .buffer_unordered(20)
    .for_each(|_| async {})
    .await;

	println!("Current node: {}", node.id);
	Ok(())
}
