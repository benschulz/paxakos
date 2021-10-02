use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::event::ShutdownEvent;
use crate::invocation::Invocation;

use super::commits::Commits;
use super::state_keeper::EventStream;

/// A `Node` that is being [`shut_down`][crate::Node::shut_down].
pub trait Shutdown {
    /// Parametrization of the paxakos algorithm.
    type Invocation: Invocation;

    /// Polls the node's event stream, driving the shutdown to conclusion.
    fn poll_shutdown(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<ShutdownEvent<Self::Invocation>>;
}

/// The default `Shutdown` implementation.
pub struct DefaultShutdown<I: Invocation> {
    trigger: LocalBoxFuture<'static, ()>,
    events: EventStream<I>,
    commits: Commits,
}

impl<I: Invocation> DefaultShutdown<I> {
    pub(crate) fn new(
        trigger: LocalBoxFuture<'static, ()>,
        events: EventStream<I>,
        commits: Commits,
    ) -> Self {
        Self {
            trigger,
            events,
            commits,
        }
    }
}

impl<I: Invocation> super::Shutdown for DefaultShutdown<I> {
    type Invocation = I;

    fn poll_shutdown(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<ShutdownEvent<Self::Invocation>> {
        let _ = self.trigger.poll_unpin(cx);

        while let std::task::Poll::Ready(Some(())) = self.commits.poll_next(cx) {
            // keep going
        }

        self.events
            .poll_next_unpin(cx)
            .map(|e| e.expect("Event stream ended"))
    }
}
