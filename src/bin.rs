use chord_rust::{chord, server};
use clap::Parser;
use log::info;

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

	let node = chord::construct_node(&args.addr);
	let join_node: Option<chord::Node> = match args.join.as_ref() {
		Some(n) => Some(chord::construct_node(n)),
		None => None
	};

	info!("listening at: {}, id: {}", &args.addr, node.id);
	server::start_server(&node, &join_node).await?;
	Ok(())
}
