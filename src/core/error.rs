use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::{
	result::Result,
	error,
	fmt
};
use super::{ring::Digest, Node};

/// Fail to execute the RPC at the server side
#[derive(Serialize, Deserialize, Debug)]
pub struct RpcFailure {
	/// Error message
	message: String
}

impl fmt::Display for RpcFailure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "RPC failed: {}", self.message)
	}
}

/// Convert DhtError to RpcFailure to serialize it for RPC call
impl From<DhtError> for RpcFailure {
	fn from(e: DhtError) -> Self {
		Self {
			message: e.to_string()
		}
	}
}

impl error::Error for RpcFailure {
}


#[derive(Error, Debug)]
pub enum DhtError {
	#[error("{node}: no live successor for id {id}")]
	NoLiveSucc {
		node: Node,
		id: Digest
	},
	/// Error when calling the RPC
	#[error("RPC failure")]
	RpcFailure(#[from] RpcFailure),
	#[error("RPC internal error")]
	RpcError(#[from] tarpc::client::RpcError),
	#[error("IO error")]
	IoError(#[from] std::io::Error)
}

pub type DhtResult<T> = Result<T, DhtError>;
pub type RpcResult<T> = Result<T, RpcFailure>;
