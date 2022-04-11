# chord-dht

A DHT (distributed hash table) implementation in Rust based on Chord with high peformance and data replication.

## Usage

To start a server:

```sh
cargo run -- chord-dht-server <bind_addr> [--join <addr>]
```

To start a client:

```sh
cargo run -- chord-dht-client <server_addr>
```


## Features built upon Chord

* In-memory key-value storage
* Data replication
* Fault tolerance

The in-memory key-value DHT is aimed to be efficient when storing ephemeral data (e.g. user tokens).


## TODO

- [ ] Transfer existing keys when node is down or joins the ring
- [ ] Allow node to leave
