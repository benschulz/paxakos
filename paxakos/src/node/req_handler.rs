use std::future::Future;
use std::sync::Arc;

use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::PromiseFor;
use crate::communicator::RoundNumOf;
use crate::error::AcceptError;
use crate::error::CommitError;
use crate::error::PrepareError;
use crate::node::CommunicatorOf;
use crate::node::StateOf;
use crate::state::LogEntryIdOf;
use crate::state::LogEntryOf;
use crate::State;

use super::state_keeper::StateKeeperHandle;

pub type RequestHandlerFor<N> = RequestHandler<StateOf<N>, CommunicatorOf<N>>;

/// Used by [`Communicator`][crate::communicator::Communicator]s to prepare
/// replies.
#[derive(Debug)]
pub struct RequestHandler<S: State, C: Communicator>(StateKeeperHandle<S, C>);

impl<S: State, C: Communicator> RequestHandler<S, C> {
    pub(crate) fn new(state_keeper: StateKeeperHandle<S, C>) -> Self {
        Self(state_keeper)
    }

    /// Asks this node to answer a `send_prepare` sent by another node.
    pub fn handle_prepare(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
    ) -> impl Future<Output = Result<PromiseFor<C>, PrepareError<C>>> {
        let handle = self.0.clone();
        async move { handle.handle_prepare(round_num, coord_num).await }
    }

    /// Asks this node to answer a `send_proposal` sent by another node.
    pub fn handle_proposal<I: Into<Arc<LogEntryOf<S>>>>(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: I,
    ) -> impl Future<Output = Result<(), AcceptError<C>>> {
        let handle = self.0.clone();
        async move { handle.handle_proposal(round_num, coord_num, entry).await }
    }

    /// Asks this node to commit the given log entry for the given round number.
    pub fn handle_commit<I: Into<Arc<LogEntryOf<S>>>>(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: I,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        self.0.handle_commit(round_num, coord_num, entry)
    }

    /// Asks this node to answer a `send_commit_by_id` sent by another node.
    pub fn handle_commit_by_id(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry_id: LogEntryIdOf<S>,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        let handle = self.0.clone();
        async move {
            handle
                .handle_commit_by_id(round_num, coord_num, entry_id)
                .await
        }
    }
}
