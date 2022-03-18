use crate::chord::{
	node::{NodeServer, NodeService},
	Node
};
use futures::{future, prelude::*, executor};
use tarpc::{
	tokio_serde::formats::Bincode,
	server::{Channel, incoming::Incoming}
};
use log::{info};

pub async fn start_server(node: Node, join_node: Option<Node>) -> anyhow::Result<()> {
	let mut listener = tarpc::serde_transport::tcp::listen(&node.addr, Bincode::default).await?;
	info!("node {}: listening at {}", node.id, &node.addr);
	listener.config_mut().max_frame_length(usize::MAX);
	listener
		.filter_map(|r| future::ready(r.ok()))
		.map(tarpc::server::BaseChannel::with_defaults)
		.max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
		.map(|channel| {
			info!("Starting server for node: {:?}", node);
			let mut server = NodeServer::new(&node);
			match join_node.as_ref() {
				Some(n) => executor::block_on(server.join(n)),
				None => ()
			};

			channel.execute(server.serve())
		})
		// Max 20 channels
    .buffer_unordered(20)
    .for_each(|_| async {})
    .await;
	Ok(())
}
