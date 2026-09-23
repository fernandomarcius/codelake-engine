//! Connection pool for SQL Server (bb8 over tiberius).
//!
//! A connection whose result stream was abandoned mid-way (client cancelled the
//! query) still has unread TDS tokens on the wire. Such connections are marked
//! `poisoned` and discarded by the pool instead of being handed to the next query.

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub(crate) type Pool = bb8::Pool<Manager>;

#[derive(Debug)]
pub(crate) struct Manager {
    config: Config,
}

impl Manager {
    pub(crate) fn new(config: Config) -> Self {
        Self { config }
    }
}

pub(crate) struct Connection {
    pub(crate) client: Client<Compat<TcpStream>>,
    /// Set while a result stream is being consumed; cleared only after the
    /// stream is fully drained.
    pub(crate) poisoned: bool,
}

impl bb8::ManageConnection for Manager {
    type Connection = Connection;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Connection, Self::Error> {
        let tcp = TcpStream::connect(self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let client = Client::connect(self.config.clone(), tcp.compat_write()).await?;
        Ok(Connection {
            client,
            poisoned: false,
        })
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), Self::Error> {
        conn.client.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Connection) -> bool {
        conn.poisoned
    }
}
