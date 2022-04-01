use chord_dht::core::{
	self,
	config::*,
	NodeServer,
	Node
};
use clap::Parser;

#[derive(Parser)]
struct Args {
	/// Local addr to bind (<host>:<port>)
	addr: String,

	/// Join an existing node on init (<host>:<port>)
	#[clap(short, long)]
	join: Option<String>
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	env_logger::init();
	let args = Args::parse();

	let node = core::construct_node(&args.addr);
	let join_node: Option<Node> = match args.join.as_ref() {
		Some(n) => Some(core::construct_node(n)),
		None => None
	};

	let config = Config::default();
	let mut s = NodeServer::new(node, config);
	let manager = s.start(join_node).await?;
	manager.wait().await?;
	Ok(())
}
