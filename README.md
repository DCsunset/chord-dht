# chord-dht

[![crates.io](https://badgen.net/crates/v/chord-dht)](https://crates.io/crates/chord-dht)
[![docker](https://badgen.net/docker/size/dcsunset/chord-dht)](https://hub.docker.com/repository/docker/dcsunset/chord-dht)

A DHT (distributed hash table) implementation in Rust based on Chord with high performance and data replication.

It can be used either as a library or as a standalone application.

## Installation

For library, add `chord-dht` to the dependencies in your `Cargo.toml`.

For standalone binaries, use `cargo install chord-dht`.
You can also use the docker image:

```sh
# For server (suppose the port to use is <port>)
docker run -d -p <port>:<port> chord-dht chord-dht-server <server_args>

# For client
docker run -it chord-dht chord-dht-client <client_args>
```

## Usage

### Library

As a client:

```rust
use chord_dht::client::setup_client;
use tarpc::context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	// server address
	let addr = "127.0.0.1:9800"
	let client = setup_client(&addr).await?;

	let ctx = context::current();
	let key = "key".as_bytes();
	let value = "value".as_bytes();
	client.set_rpc(ctx, key.clone(), Some(value.clone())).await?;
	let ret = client.get_rpc(ctx, key.clone()).await?;
	assert_eq!(ret.unwrap(), value);
	Ok(())
}
```

As a server:

```rust
use chord_dht::core::{
	Node,
	NodeServer,
	config::Config
};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let node = Node {
		addr: "127.0.0.1:9800".to_string(),
		id: 0
	};
	let mut server = NodeServer::new(node, Config::default());
	let manager = server.start(None).await?;
	// Wait for the server
	manager.wait().await?;

	Ok(())
}
```


### Standalone Application

To start a server:

```sh
cargo run -- chord-dht-server <bind_addr> [--join <addr>]
```

To start a client:

```sh
cargo run -- chord-dht-client <server_addr>
> set key value
> get key
value
```


## Features built upon Chord

* In-memory key-value storage
* Data replication
* Fault tolerance

The in-memory key-value DHT is aimed to be efficient when storing ephemeral data (e.g. user tokens).


## TODO

- [ ] Transfer existing keys when node is down or joins the ring
- [ ] Allow node to leave


## License

Licensed under the [AGPLv3](LICENSE) license.

