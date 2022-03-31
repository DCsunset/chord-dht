# chord-dht

A DHT (distributed hash table) implementation in Rust based on Chord with high peformance and data replication.

## Features built upon Chord

* In-memory key-value DHT
* Replication
* Fault tolerance

The in-memory key-value DHT is aimed to be efficient when storing ephemeral data (e.g. user tokens).

## TODO

- [] Transfer keys when joining or a node is back
- [] Allow node to leave
- [] Replicate keys when a node is down
