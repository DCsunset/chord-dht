use crate::core::error::*;
use futures::future;

pub struct ServerManager {
	pub handle: future::JoinAll<tokio::task::JoinHandle<()>>,
	pub tx: tokio::sync::watch::Sender<bool>
}

impl ServerManager {
	/// Wait for the server to terminate
	pub async fn wait(self) -> DhtResult<()> {
		self.handle.await
			.into_iter()
			.collect::<Result<Vec<_>, tokio::task::JoinError>>()?;

		Ok(())
	}

	/// Stop the server gracefully
	pub async fn stop(self) -> DhtResult<()> {
		self.tx.send(true)?;
		self.wait().await
	}
}
