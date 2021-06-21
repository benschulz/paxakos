use std::future::Future;
use std::sync::Arc;

use crate::error::AcceptError;
use crate::error::CommitError;
use crate::error::PrepareError;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::PromiseFor;
use crate::invocation::RoundNumOf;
use crate::invocation::YeaOf;

use super::state_keeper::StateKeeperHandle;

/// Used by [`Communicator`][crate::communicator::Communicator]s to prepare
/// replies.
#[derive(Debug)]
pub struct RequestHandler<I: Invocation>(StateKeeperHandle<I>);

impl<I: Invocation> RequestHandler<I> {
    pub(crate) fn new(state_keeper: StateKeeperHandle<I>) -> Self {
        Self(state_keeper)
    }

    /// Asks this node to answer a `send_prepare` sent by another node.
    pub fn handle_prepare(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) -> impl Future<Output = Result<PromiseFor<I>, PrepareError<I>>> {
        let handle = self.0.clone();
        async move { handle.handle_prepare(round_num, coord_num).await }
    }

    /// Asks this node to answer a `send_proposal` sent by another node.
    pub fn handle_proposal<E: Into<Arc<LogEntryOf<I>>>>(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: E,
    ) -> impl Future<Output = Result<YeaOf<I>, AcceptError<I>>> {
        let handle = self.0.clone();
        async move { handle.handle_proposal(round_num, coord_num, entry).await }
    }

    /// Asks this node to commit the given log entry for the given round number.
    pub fn handle_commit<E: Into<Arc<LogEntryOf<I>>>>(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: E,
    ) -> impl Future<Output = Result<(), CommitError<I>>> {
        self.0.handle_commit(round_num, coord_num, entry)
    }

    /// Asks this node to answer a `send_commit_by_id` sent by another node.
    pub fn handle_commit_by_id(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry_id: LogEntryIdOf<I>,
    ) -> impl Future<Output = Result<(), CommitError<I>>> {
        let handle = self.0.clone();
        async move {
            handle
                .handle_commit_by_id(round_num, coord_num, entry_id)
                .await
        }
    }
}
