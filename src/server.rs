use chord_rust::chord::{
	self,
	node::{NodeServer, NodeService}
};
use futures::{future, prelude::*, executor};
use tarpc::{
	tokio_serde::formats::Bincode,
	server::{Channel, incoming::Incoming}
};
use clap::Parser;

#[derive(Parser)]
struct Args {
	/// Local addr to bind (<host>:<port>)
	#[clap(short, long)]
	addr: String,

	/// Join an existing node on init (<host>:<port>)
	#[clap(short, long)]
	join: Option<String>
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();

	let node = chord::construct_node(&args.addr);
	// TODO: handle cases where node == join_node
	let join_node: Option<chord::Node> = match args.join.as_ref() {
		Some(n) => Some(chord::construct_node(n)),
		None => None
	};

	let mut listener = tarpc::serde_transport::tcp::listen(&node.addr, Bincode::default).await?;
	listener.config_mut().max_frame_length(usize::MAX);
	listener
    .filter_map(|r| future::ready(r.ok()))
    .map(tarpc::server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
    .map(|channel| {
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

	println!("Current node: {}", node.id);
	Ok(())
}
