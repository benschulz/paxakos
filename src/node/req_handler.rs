use std::future::Future;
use std::sync::Arc;

use crate::error::{AcceptError, CommitError, PrepareError};
use crate::state::{LogEntryIdOf, LogEntryOf};
use crate::{CoordNum, Promise, RoundNum, State};

use super::state_keeper::StateKeeperHandle;

/// Used by [`Communicator`][crate::communicator::Communicator]s to prepare
/// replies.
#[derive(Debug)]
pub struct RequestHandler<S: State, R: RoundNum, C: CoordNum>(StateKeeperHandle<S, R, C>);

impl<S: State, R: RoundNum, C: CoordNum> RequestHandler<S, R, C> {
    pub(crate) fn new(state_keeper: StateKeeperHandle<S, R, C>) -> Self {
        Self(state_keeper)
    }

    /// Asks this node to answer a `send_prepare` sent by another node.
    pub fn handle_prepare(
        &self,
        round_num: R,
        coord_num: C,
    ) -> impl Future<Output = Result<Promise<R, C, LogEntryOf<S>>, PrepareError<S, C>>> {
        let handle = self.0.clone();
        async move { handle.handle_prepare(round_num, coord_num).await }
    }

    /// Asks this node to answer a `send_proposal` sent by another node.
    pub fn handle_proposal<I: Into<Arc<LogEntryOf<S>>>>(
        &self,
        round_num: R,
        coord_num: C,
        entry: I,
    ) -> impl Future<Output = Result<(), AcceptError<S, C>>> {
        let handle = self.0.clone();
        async move { handle.handle_proposal(round_num, coord_num, entry).await }
    }

    /// Asks this node to commit the given log entry for the given round number.
    ///
    /// This method will typically be called in order to answer a `send_commit`
    /// sent by another node. However, it can and should also be used to
    /// implement catch-up mechanisms.
    pub fn handle_commit<I: Into<Arc<LogEntryOf<S>>>>(
        &self,
        round_num: R,
        entry: I,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        self.0.handle_commit(round_num, entry)
    }

    /// Asks this node to answer a `send_commit_by_id` sent by another node.
    pub fn handle_commit_by_id(
        &self,
        round_num: R,
        entry_id: LogEntryIdOf<S>,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        let handle = self.0.clone();
        async move { handle.handle_commit_by_id(round_num, entry_id).await }
    }
}
