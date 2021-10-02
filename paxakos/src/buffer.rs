//! Defines the applied log entry [`Buffer`] trait.
//!
//! When a node is temporarily shut down or becomes unreachable, it needs to be
//! catch back up with the rest of the cluster. One way to do that is to poll
//! other nodes for entries that have been committed in the meantime. These
//! other nodes will in turn query their applied entry buffer for these entries.
//!
//! A node may fall so far behind that no other node can catch it up through its
//! `Buffer`. In such a case the node must ask another node to create a snapshot
//! and catch up that way. It should be noted that the cluster will likely
//! progress while the snapshot is taken, transferred and installed. That is to
//! say that `Buffer`s are indispensable and should have a reasonable minimal
//! capacity.
//!
//! The default implementation is [`InMemoryBuffer`].
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

use crate::CoordNum;
use crate::LogEntry;
use crate::RoundNum;

/// A buffer of entries that were applied to the local state.
// TODO provide an (optional) persistent implementation
pub trait Buffer: Send + 'static {
    /// The round number type.
    type RoundNum: RoundNum;
    /// The coordination number type.
    type CoordNum: CoordNum;
    /// The log entry type.
    type Entry: LogEntry;
    /// The type of error this buffer may raise.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Insert an applied entry into the buffer.
    fn insert(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        entry: Arc<Self::Entry>,
    );

    /// Retrieve an entry from the buffer.
    ///
    /// May be called for round numbers that were not previously inserted.
    #[allow(clippy::type_complexity)]
    fn get(
        &self,
        round_num: Self::RoundNum,
    ) -> Result<Option<(Self::CoordNum, Arc<Self::Entry>)>, Self::Error>;
}

/// An in-memory buffer.
pub struct InMemoryBuffer<R, C, E: LogEntry> {
    capacity: usize,
    buffer: VecDeque<(R, C, Arc<E>)>,
}

impl<R, C, E: LogEntry> InMemoryBuffer<R, C, E> {
    /// Creates a new in-memory buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);

        Self {
            capacity,
            buffer: VecDeque::new(),
        }
    }
}

impl<R, C, E> Buffer for InMemoryBuffer<R, C, E>
where
    R: RoundNum,
    C: CoordNum,
    E: LogEntry,
{
    type RoundNum = R;
    type CoordNum = C;
    type Entry = E;
    type Error = Infallible;

    fn insert(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        entry: Arc<Self::Entry>,
    ) {
        if self.capacity <= self.buffer.len() {
            self.buffer.pop_front();
        }

        self.buffer.push_back((round_num, coord_num, entry));
    }

    #[allow(clippy::type_complexity)]
    fn get(
        &self,
        round_num: Self::RoundNum,
    ) -> Result<Option<(Self::CoordNum, Arc<Self::Entry>)>, Self::Error> {
        // hot path
        if let Some((r, _, _)) = self.buffer.get(0) {
            if round_num >= *r {
                if let Some(delta) = crate::util::try_usize_delta(round_num, *r) {
                    if let Some((r, c, e)) = self.buffer.get(delta) {
                        if *r == round_num {
                            return Ok(Some((*c, Arc::clone(e))));
                        }
                    }
                }
            }
        }

        // cold path
        Ok(self
            .buffer
            .iter()
            .find(|(r, _, _)| *r == round_num)
            .map(|(_, c, e)| (*c, Arc::clone(e))))
    }
}
