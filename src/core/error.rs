use thiserror::Error;
use std::result::Result;
use super::ring::Digest;

#[derive(Error, Debug)]
pub enum DhtError {
	#[error("No live replica for key digest {0}")]
	NoLiveReplica(Digest),
	#[error("RPC error")]
	RpcError(#[from] tarpc::client::RpcError),
	#[error("IO error")]
	IoError(#[from] std::io::Error)
}

pub type DhtResult<T> = Result<T, DhtError>;
