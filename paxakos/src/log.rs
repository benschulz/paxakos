use crate::Identifier;

/// Appended to the shared log and applied to the shared `State`.
pub trait LogEntry: Clone + std::fmt::Debug + Send + Sync + Unpin + 'static
where
    Self: serde::Serialize,
    for<'de> Self: serde::Deserialize<'de>,
{
    type Id: Identifier;

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
