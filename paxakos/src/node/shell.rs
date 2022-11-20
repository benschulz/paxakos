use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::error::AffirmSnapshotError;
use crate::error::Disoriented;
use crate::error::InstallSnapshotError;
use crate::error::ShutDownOr;
use crate::retry::RetryPolicy;
use crate::Node;
use crate::NodeHandle;
use crate::NodeStatus;

use super::handle::BoxedRetryPolicy;
use super::handle::NodeHandleRequest;
use super::handle::NodeHandleResponse;
use super::AppendResultFor;
use super::ImplAppendResultFor;
use super::InvocationOf;
use super::LogEntryOf;
use super::NodeIdOf;
use super::NodeImpl;
use super::Participation;
use super::RoundNumOf;
use super::ShutdownOf;
use super::SnapshotFor;
use super::StateOf;

/// The [`Node`][crate::Node] implementation that's returned from
/// [`NodeBuilder`][crate::NodeBuilder]s.
///
/// This `Node` implementation processes requests from [`NodeHandle`]s. It
/// delegates to the outermost [`Decoration`][crate::decoration::Decoration],
/// thus giving decorations a chance to intercept.
pub struct Shell<N: NodeImpl> {
    pub(crate) wrapped: N,

    receiver: mpsc::Receiver<super::handle::RequestAndResponseSender<InvocationOf<N>>>,
    appends: FuturesUnordered<HandleAppend<Self>>,
}

impl<N: NodeImpl> Shell<N> {
    pub(crate) fn new(
        wrapped: N,
        receiver: mpsc::Receiver<super::handle::RequestAndResponseSender<InvocationOf<N>>>,
    ) -> Self {
        Self {
            wrapped,
            receiver,
            appends: FuturesUnordered::new(),
        }
    }

    fn process_handle_req(
        &mut self,
        req: NodeHandleRequest<InvocationOf<N>>,
        send: oneshot::Sender<NodeHandleResponse<InvocationOf<N>>>,
    ) {
        match req {
            NodeHandleRequest::Status => {
                let status = self.status();
                let _ = send.send(NodeHandleResponse::Status(status));
            }
            NodeHandleRequest::Append { log_entry, args } => {
                let append = self.wrapped.append_impl(log_entry, args);

                self.appends.push(HandleAppend {
                    append,
                    send: Some(send),
                });
            }
        }
    }
}

impl<N> Node for Shell<N>
where
    N: NodeImpl,
{
    type Invocation = InvocationOf<N>;

    type Shutdown = ShutdownOf<N>;

    fn id(&self) -> NodeIdOf<Self> {
        self.wrapped.id()
    }

    fn status(&self) -> NodeStatus {
        self.wrapped.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.wrapped.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<super::EventFor<Self>> {
        while let std::task::Poll::Ready(Some((req, send))) = self.receiver.poll_next_unpin(cx) {
            self.process_handle_req(req, send);
        }

        while let std::task::Poll::Ready(Some(())) = self.appends.poll_next_unpin(cx) {
            // keep going
        }

        self.wrapped.poll_events(cx)
    }

    fn handle(&self) -> NodeHandle<Self::Invocation> {
        self.wrapped.handle()
    }

    fn prepare_snapshot(&self) -> LocalBoxFuture<'static, SnapshotFor<Self>> {
        self.wrapped.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), AffirmSnapshotError>> {
        self.wrapped.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), InstallSnapshotError>> {
        self.wrapped.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> LocalBoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.wrapped.read_stale(f)
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> LocalBoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.wrapped.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> LocalBoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.wrapped.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> LocalBoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.wrapped.read_stale_scoped_infallibly(f)
    }

    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.wrapped.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.wrapped.shut_down()
    }
}

struct HandleAppend<N: Node> {
    append: LocalBoxFuture<
        'static,
        ImplAppendResultFor<N, LogEntryOf<N>, BoxedRetryPolicy<InvocationOf<N>>>,
    >,
    send: Option<oneshot::Sender<NodeHandleResponse<InvocationOf<N>>>>,
}

impl<N: Node> std::future::Future for HandleAppend<N> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.send.as_ref().unwrap().is_canceled() {
            return std::task::Poll::Ready(());
        }

        self.append.poll_unpin(cx).map(|r| {
            let send = self.send.take().unwrap();
            let _ = send.send(NodeHandleResponse::Append(r));
        })
    }
}
