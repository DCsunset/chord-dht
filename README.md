# chord-dht

A DHT (distributed hash table) implementation in Rust based on Chord with high peformance and data replication.

## Features built upon Chord

* In-memory key-value DHT
* Lazy replication

Lazy replication is preferred because the in-memory DHT is aimed to store ephemeral data (e.g. user tokens).
It is much more efficient for this kind of use cases.
