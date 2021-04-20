use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::StreamExt;

use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::event::ShutdownEventFor;
use crate::state::State;

use super::commits::Commits;
use super::state_keeper::EventStream;

/// A `Node` that is being [`shut_down`][crate::Node::shut_down].
pub trait Shutdown {
    type State: State;
    type Communicator: Communicator;

    /// Polls the node's event stream, driving the shutdown to conclusion.
    fn poll_shutdown(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<ShutdownEventFor<Self>>;
}

pub struct DefaultShutdown<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    trigger: LocalBoxFuture<'static, ()>,
    events: EventStream<S, RoundNumOf<C>, CoordNumOf<C>>,
    commits: Commits,
}

impl<S, C> DefaultShutdown<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    pub(crate) fn new(
        trigger: LocalBoxFuture<'static, ()>,
        events: EventStream<S, RoundNumOf<C>, CoordNumOf<C>>,
        commits: Commits,
    ) -> Self {
        Self {
            trigger,
            events,
            commits,
        }
    }
}

impl<S, C> super::Shutdown for DefaultShutdown<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    type State = S;
    type Communicator = C;

    fn poll_shutdown(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<ShutdownEventFor<Self>> {
        let _ = self.trigger.poll_unpin(cx);

        while let std::task::Poll::Ready(Some(())) = self.commits.poll_next(cx) {
            // keep going
        }

        self.events
            .poll_next_unpin(cx)
            .map(|e| e.expect("Event stream ended"))
    }
}
