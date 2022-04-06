use chord_dht::{
	client::setup_client,
	rpc::NodeServiceClient
};
use tarpc::context;
use clap::Parser;
use inquire::{Text, CustomUserError};
use anyhow::anyhow;

#[derive(Parser)]
struct Args {
	/// Server addr to connect to (<host>:<port>)
	addr: String,
}

const COMMANDS: [&str; 2] = [
	"get",
	"set"
];

fn suggest_command(v: &str) -> Result<Vec<String>, CustomUserError> {
	let mut result = Vec::new();
	for command in COMMANDS {
		if v.len() > 0 && command.starts_with(v) {
			result.push(command.to_string());
		}
	}
	Ok(result)
}

fn complete_command(v: &str) -> Result<Option<String>, CustomUserError> {
	let result = suggest_command(v)?;
	let command = if result.len() > 0 {
		Some(result[0].clone() + " ")
	}
	else {
		None
	};
	Ok(command)
}

async fn execute_command(client: &NodeServiceClient, command: &str) -> anyhow::Result<()> {
	// execute command
	let words: Vec<_> = command.split_whitespace().collect();
	if words.len() == 0 {
		return Err(anyhow!("invalid command"));
	}

	let ctx = context::current();
	match words[0] {
		"get" => {
			if words.len() != 2 {
				return Err(anyhow!("get: invalid number of arguments for"));
			}
			let value = client.get_rpc(
				ctx,
				words[1].as_bytes().to_vec()
			).await?;
			match value {
				Some(v) => println!("{}", String::from_utf8(v)?),
				None => return Err(anyhow!("get: key doesn't exist"))
			};
		},
		"set" => {
			if words.len() < 2 || words.len() > 3 {
				return Err(anyhow!("set: invalid number of arguments for"));
			}
			client.set_rpc(
				ctx,
				words[1].as_bytes().to_vec(),
				if words.len() == 3 {
					Some(words[2].as_bytes().to_vec())
				} else {
					None
				}
			).await?;
		},
		_ => {
			return Err(anyhow!("invalid command"));
		}
	};
	Ok(())
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	env_logger::init();
	let args = Args::parse();
	let client = setup_client(&args.addr).await?;

	loop {
		let command = Text::new("")
			.with_suggester(&suggest_command)
			.with_completer(&complete_command)
			.prompt()?;
		
		match execute_command(&client, &command).await {
			Ok(_) => (),
			Err(e) => println!("Error: {}", e)
		};
	}
}
