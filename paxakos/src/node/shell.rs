use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::lock::MutexGuard;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use pin_project::pin_project;
use snarc::ErasedNarc;
use snarc::ErasedSnarc;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::error::AffirmSnapshotError;
use crate::error::Disoriented;
use crate::error::InstallSnapshotError;
use crate::error::ShutDownOr;
use crate::retry::RetryPolicy;
use crate::Event;
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

#[doc(hidden)]
pub trait State {
    fn enter<F: FnOnce() -> R, R>(&mut self, f: F) -> R;
}

/// A `Shell<_, Unsendable>` is `!Send`.
pub struct Unsendable(ErasedNarc);

impl State for Unsendable {
    fn enter<F: FnOnce() -> R, R>(&mut self, f: F) -> R {
        f()
    }
}

/// A `Shell<_, Sendable>` is `Send`.
pub struct Sendable(ErasedSnarc);

impl State for Sendable {
    fn enter<F: FnOnce() -> R, R>(&mut self, f: F) -> R {
        self.0.enter(f)
    }
}

/// The [`Node`][crate::Node] implementation that's returned from
/// [`NodeBuilder`][crate::NodeBuilder]s.
///
/// This `Node` implementation processes requests from [`NodeHandle`]s. It
/// delegates to the outermost [`Decoration`][crate::decoration::Decoration],
/// thus giving decorations a chance to intercept.
pub struct Shell<N: NodeImpl, S: State = Sendable> {
    mid: Option<ShellMid<N, S>>,
}

pub(crate) struct ShellMid<N: NodeImpl, S: State = Sendable> {
    pub(crate) entered: EnteredShell<N>,
    state: S,
}

impl<N: NodeImpl, S: State> Shell<N, S> {
    pub(crate) fn mid(&self) -> &ShellMid<N, S> {
        self.mid.as_ref().expect("mid")
    }

    fn mid_mut(&mut self) -> &mut ShellMid<N, S> {
        self.mid.as_mut().expect("mid")
    }

    fn into_parts(mut self) -> (EnteredShell<N>, S) {
        let mid = self.mid.take().expect("mid");

        (mid.entered, mid.state)
    }
}

impl<N: NodeImpl, S: State> Drop for Shell<N, S> {
    fn drop(&mut self) {
        if let Some(mut mid) = self.mid.take() {
            mid.state.enter(|| {
                drop(mid.entered);
            });
        }
    }
}

impl<N: NodeImpl> Shell<N, Unsendable> {
    /// Turns this node into a sendable one.
    ///
    /// A sendable node can be more cumbersome to use but is `Send`.
    pub fn into_send(self) -> Shell<N, Sendable> {
        let (entered, state) = self.into_parts();

        Shell {
            mid: Some(ShellMid {
                entered,
                state: Sendable(state.0.into()),
            }),
        }
    }
}

impl<N: NodeImpl> Shell<N, Sendable> {
    pub(crate) fn new(
        wrapped: N,
        context: ErasedSnarc,
        receiver: mpsc::Receiver<super::handle::RequestAndResponseSender<InvocationOf<N>>>,
    ) -> Self {
        Self {
            mid: Some(ShellMid {
                entered: EnteredShell(ShellInner {
                    wrapped,
                    receiver,
                    appends: FuturesUnordered::new(),
                }),

                state: Sendable(context),
            }),
        }
    }

    /// Evaluates `f`, providing access to the full `Node` API through the
    /// argument passed to `f`.
    pub fn enter<F, R>(&mut self, f: F) -> R
    where
        F: for<'a> FnOnce(&'a mut EnteredShellRef<'a, N>) -> R,
    {
        let mid = self.mid_mut();

        mid.state.enter(|| {
            f(&mut EnteredShellRef(
                &mut mid.entered,
                std::marker::PhantomData,
            ))
        })
    }

    /// Returns a wrapper around the future or stream returned by `f` that
    /// calls `enter` when polled.
    pub fn enter_on_poll<'a, F, R>(&'a mut self, f: F) -> EnterOnPoll<'a, R>
    where
        F: FnOnce(&'a mut EnteredShell<N>) -> R,
        R: 'a,
    {
        let mid = self.mid_mut();

        let fut = mid.state.enter(|| f(&mut mid.entered));

        EnterOnPoll(&mut mid.state.0, fut)
    }

    /// Returns a wrapper around the future or stream returned by `f` that
    /// calls `enter` when polled.
    pub fn into_enter_on_poll<F, R>(self, f: F) -> EnterOnPollOwned<R>
    where
        F: FnOnce(EnteredShell<N>) -> R,
    {
        let (entered, mut state) = self.into_parts();

        let fut = state.enter(|| f(entered));

        EnterOnPollOwned(state.0, fut)
    }

    /// Turns this node into an unsendable one.
    ///
    /// An unsendable node is easier to use but is `!Send`.
    pub fn into_unsend(self) -> Shell<N, Unsendable> {
        let (entered, state) = self.into_parts();

        Shell {
            mid: Some(ShellMid {
                entered,
                state: Unsendable(state.0.into()),
            }),
        }
    }

    /// This node's identifier.
    pub fn id(&self) -> NodeIdOf<N> {
        self.mid().entered.id()
    }

    /// Node's current status.
    pub fn status(&self) -> NodeStatus {
        self.mid().entered.status()
    }

    /// Node's current mode of participation.
    pub fn participation(&self) -> Participation<RoundNumOf<N>> {
        self.mid().entered.participation()
    }

    /// Polls the node's event stream.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    pub fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<super::EventFor<N>> {
        let mid = self.mid_mut();

        mid.state.enter(|| mid.entered.poll_events(cx))
    }

    /// Returns a future that polls this node for events until the next one is
    /// returned.
    pub fn next_event(&mut self) -> impl Future<Output = Event<N::Invocation>> + '_ {
        self.enter_on_poll(|n| n.next_event())
    }

    /// Returns a stream that polls this node for events.
    pub fn events(&mut self) -> impl Stream<Item = Event<N::Invocation>> + '_ {
        self.enter_on_poll(|n| n.events())
    }

    /// Returns a [handle][NodeHandle] for this node.
    ///
    /// A node handle can be freely sent between threads.
    pub fn handle(&self) -> NodeHandle<N::Invocation> {
        self.mid().entered.handle()
    }

    /// Begins a graceful shutdown of this node.
    pub fn shut_down(self) -> ShutdownOf<N> {
        let (entered, mut state) = self.into_parts();

        state.enter(|| entered.shut_down())
    }
}

/// Wrapper returned by [`Shell::enter_on_poll`].
#[pin_project]
pub struct EnterOnPoll<'a, T>(&'a mut ErasedSnarc, #[pin] T);

impl<'a, F> Future for EnterOnPoll<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let projected = self.project();

        projected.0.enter(|| projected.1.poll(cx))
    }
}

impl<'a, S> Stream for EnterOnPoll<'a, S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let projected = self.project();

        projected.0.enter(|| projected.1.poll_next(cx))
    }
}

/// Wrapper returned by [`Shell::into_enter_on_poll`].
#[pin_project]
pub struct EnterOnPollOwned<F>(ErasedSnarc, #[pin] F);

impl<F> Future for EnterOnPollOwned<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let projected = self.project();

        projected.0.enter(|| projected.1.poll(cx))
    }
}

impl<S> Stream for EnterOnPollOwned<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let projected = self.project();

        projected.0.enter(|| projected.1.poll_next(cx))
    }
}

impl<N> Node for Shell<N, Unsendable>
where
    N: NodeImpl,
{
    type Invocation = InvocationOf<N>;

    type Shutdown = ShutdownOf<N>;

    fn id(&self) -> NodeIdOf<Self> {
        self.mid().entered.id()
    }

    fn status(&self) -> NodeStatus {
        self.mid().entered.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.mid().entered.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<super::EventFor<Self>> {
        self.mid_mut().entered.poll_events(cx)
    }

    fn handle(&self) -> NodeHandle<Self::Invocation> {
        self.mid().entered.handle()
    }

    fn prepare_snapshot(&self) -> BoxFuture<'static, SnapshotFor<Self>> {
        self.mid().entered.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), AffirmSnapshotError>> {
        self.mid().entered.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), InstallSnapshotError>> {
        self.mid().entered.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> BoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.mid().entered.read_stale(f)
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> BoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.mid().entered.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> BoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.mid().entered.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> BoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.mid().entered.read_stale_scoped_infallibly(f)
    }

    fn append<A, P, R>(
        &mut self,
        applicable: A,
        args: P,
    ) -> futures::future::BoxFuture<'static, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.mid_mut().entered.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        let (entered, _state) = self.into_parts();

        entered.shut_down()
    }
}

/// Deref target of the argument to the closure passed to [`Shell::enter`].
pub struct EnteredShell<N: NodeImpl>(pub(crate) ShellInner<N>);

impl<N> Node for EnteredShell<N>
where
    N: NodeImpl,
{
    type Invocation = InvocationOf<N>;

    type Shutdown = ShutdownOf<N>;

    fn id(&self) -> NodeIdOf<Self> {
        self.0.wrapped.id()
    }

    fn status(&self) -> NodeStatus {
        self.0.wrapped.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.0.wrapped.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<super::EventFor<Self>> {
        while let std::task::Poll::Ready(Some((req, send))) = self.0.receiver.poll_next_unpin(cx) {
            self.0.process_handle_req(req, send);
        }

        while let std::task::Poll::Ready(Some(())) = self.0.appends.poll_next_unpin(cx) {
            // keep going
        }

        self.0.wrapped.poll_events(cx)
    }

    fn handle(&self) -> NodeHandle<Self::Invocation> {
        self.0.wrapped.handle()
    }

    fn prepare_snapshot(&self) -> BoxFuture<'static, SnapshotFor<Self>> {
        self.0.wrapped.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), AffirmSnapshotError>> {
        self.0.wrapped.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), InstallSnapshotError>> {
        self.0.wrapped.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> BoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.0.wrapped.read_stale(f)
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> BoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.0.wrapped.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> BoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.0.wrapped.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> BoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.0.wrapped.read_stale_scoped_infallibly(f)
    }

    fn append<A, P, R>(
        &mut self,
        applicable: A,
        args: P,
    ) -> futures::future::BoxFuture<'static, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.0.wrapped.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.0.wrapped.shut_down()
    }
}

pub type PhantomUnsend = PhantomData<MutexGuard<'static, ()>>;

/// Argument to the closure passed to [`Shell::enter`].
pub struct EnteredShellRef<'a, N: NodeImpl>(&'a mut EnteredShell<N>, PhantomUnsend);

impl<'a, N: NodeImpl> Deref for EnteredShellRef<'a, N> {
    type Target = EnteredShell<N>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, N: NodeImpl> DerefMut for EnteredShellRef<'a, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

pub(crate) struct ShellInner<N: NodeImpl> {
    pub(crate) wrapped: N,

    receiver: mpsc::Receiver<super::handle::RequestAndResponseSender<InvocationOf<N>>>,
    appends: FuturesUnordered<HandleAppend<N>>,
}

impl<N: NodeImpl> ShellInner<N> {
    fn process_handle_req(
        &mut self,
        req: NodeHandleRequest<InvocationOf<N>>,
        send: oneshot::Sender<NodeHandleResponse<InvocationOf<N>>>,
    ) {
        match req {
            NodeHandleRequest::Status => {
                let status = self.wrapped.status();
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

struct HandleAppend<N: Node> {
    append: BoxFuture<
        'static,
        ImplAppendResultFor<N, LogEntryOf<N>, BoxedRetryPolicy<InvocationOf<N>>>,
    >,
    send: Option<oneshot::Sender<NodeHandleResponse<InvocationOf<N>>>>,
}

// TODO this should become unnecessary with TAIT and async trait fn
unsafe impl<N: Node> Sync for HandleAppend<N> {}

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
