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
		.map(|channel| async {
			info!("Starting server for node {}", node.id);
			let mut server = NodeServer::new(&node);
			match join_node.as_ref() {
				Some(n) => server.join(n).await,
				None => ()
			};
			info!("Node {} started", node.id);

			channel.execute(server.serve()).await;
		})
    .buffer_unordered(10)
    .for_each(|_| async {})
    .await;
	Ok(())
}
