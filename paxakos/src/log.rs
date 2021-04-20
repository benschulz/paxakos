use async_trait::async_trait;
use futures::io::AsyncRead;

use crate::Identifier;

/// Appended to the shared log and applied to the shared `State`.
#[async_trait]
pub trait LogEntry: 'static + Clone + std::fmt::Debug + Send + Sync + Unpin {
    type Id: Identifier;
    type Reader: std::io::Read;
    type ReadError: std::error::Error + Send + Sync + 'static;

    /// Deserializes log entry that was previously serialized using
    /// `to_reader()`.
    ///
    /// While implementations need not detect arbitrary data corruption, they
    /// must not panic.
    async fn from_reader<R: AsyncRead + Send + Unpin>(read: R) -> Result<Self, Self::ReadError>;

    /// Number of bytes the result of `to_reader()` will emit.
    fn size(&self) -> usize;

    /// Serializes the log entry to enable snapshots.
    ///
    /// `LogEntry::from_reader(e.to_reader())` must yield an equivalent log
    /// entry.
    fn to_reader(&self) -> Self::Reader;

    /// Returns a unique identifier for the log entry.
    ///
    /// Identifiers need only be unique within the rough timeframe of them being
    /// appended to the distributed log. That notwithstanding it is recommended
    /// that UUIDs or some other "universally unique" identifier are used.
    fn id(&self) -> Self::Id;
}

#[derive(Debug)]
pub struct LogKeeping {
    pub(crate) logs_kept: usize,
    pub(crate) entry_limit: usize,
}

impl Default for LogKeeping {
    fn default() -> Self {
        Self {
            logs_kept: 5,
            entry_limit: 1024,
        }
    }
}
