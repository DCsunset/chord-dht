use chord_rust::chord;
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

	let node = chord::construct_node(&args.addr);
	let join_node: Option<chord::Node> = match args.join.as_ref() {
		Some(n) => Some(chord::construct_node(n)),
		None => None
	};

	let mut s = chord::NodeServer::new(&node);
	let handle = s.start(join_node).await?;
	handle.await?;
	Ok(())
}
