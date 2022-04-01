use thiserror::Error;
use std::result::Result;
use super::{ring::Digest, Node};

#[derive(Error, Debug)]
pub enum DhtError {
	#[error("No live replica for key digest {0}")]
	NoLiveReplica(Digest),
	#[error("Fail to join node {node}: {message}")]
	JoinFailure {
		node: Node,
		message: String
	},
	#[error("Server error")]
	ServerError(#[from] tokio::task::JoinError),
	#[error("ServerManager error")]
	ServerManagerError(#[from] tokio::sync::watch::error::SendError<bool>),
	#[error("RPC error")]
	RpcError(#[from] tarpc::client::RpcError),
	#[error("IO error")]
	IoError(#[from] std::io::Error)
}

pub type DhtResult<T> = Result<T, DhtError>;
