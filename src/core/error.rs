use thiserror::Error;
use std::result::Result;
use super::{ring::Digest, Node};

#[derive(Error, Debug)]
pub enum DhtError {
	#[error("{node}: no live successor for id {id}")]
	NoLiveSucc {
		node: Node,
		id: Digest
	},
	#[error("RPC error")]
	RpcError(#[from] tarpc::client::RpcError),
	#[error("IO error")]
	IoError(#[from] std::io::Error)
}

pub type DhtResult<T> = Result<T, DhtError>;
