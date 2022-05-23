#![allow(missing_docs)]

use std::any::Any;
use std::sync::Arc;
use std::task::Poll;

use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use futures::TryFutureExt;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::append::Importance;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::PollError;
use crate::error::ShutDown;
use crate::error::ShutDownOr;
use crate::invocation;
use crate::leadership::track::LeadershipAwareNode;
use crate::node::AppendResultFor;
use crate::node::CommitFor;
use crate::node::EventFor;
use crate::node::ImplAppendResultFor;
use crate::node::InvocationOf;
use crate::node::LogEntryOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::StaticAppendResultFor;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::DoNotRetry;
use crate::retry::RetryPolicy;
use crate::Invocation;
use crate::LogEntry;
use crate::Node;

pub trait Config {
    /// The node type that is decorated.
    type Node: Node;

    type DelegatedAppend: DelegatedAppend<Invocation = InvocationOf<Self::Node>>;

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    fn evaluate(
        &self,
        node: &Self::Node,
        log_entry: Arc<LogEntryOf<Self::Node>>,
        args: AppendArgs<InvocationOf<Self::Node>>,
    ) -> LocalBoxFuture<'static, Evaluation<Self::DelegatedAppend>>;
}

// TODO implement Try
pub enum Evaluation<D: DelegatedAppend> {
    Delegate(D),
    DoNotDelegate,
    Error(AppendError<D::Invocation>),
}

pub trait DelegatedAppend:
    std::future::Future<Output = Result<(), AppendError<Self::Invocation>>>
{
    type Invocation: Invocation;

    fn allows_short_circuiting(&self) -> bool;
}

pub trait Delegator {
    type Invocation: Invocation;

    type DelegatedAppend: DelegatedAppend<Invocation = Self::Invocation>;

    fn delegate(
        &self,
        node_id: invocation::NodeIdOf<Self::Invocation>,
        log_entry: Arc<invocation::LogEntryOf<Self::Invocation>>,
        args: AppendArgs<Self::Invocation>,
    ) -> LocalBoxFuture<'static, Evaluation<Self::DelegatedAppend>>;
}

pub struct ToLeader<N, I, D> {
    delegator: D,

    _p: std::marker::PhantomData<(N, I)>,
}

impl<N, I, D> ToLeader<N, I, D> {
    pub fn new(delegator: D) -> Self {
        Self {
            delegator,

            _p: std::marker::PhantomData,
        }
    }
}

impl<N, I, D> Config for ToLeader<N, I, D>
where
    N: LeadershipAwareNode<I>,
    D: Delegator<Invocation = InvocationOf<N>>,
    D::DelegatedAppend: 'static,
{
    type Node = N;
    type DelegatedAppend = D::DelegatedAppend;

    fn evaluate(
        &self,
        node: &Self::Node,
        log_entry: Arc<LogEntryOf<Self::Node>>,
        args: AppendArgs<InvocationOf<Self::Node>>,
    ) -> LocalBoxFuture<'static, Evaluation<Self::DelegatedAppend>> {
        if args.importance == Importance::GainLeadership {
            // TODO consider round
            if let Some(delegatee) = node.strict_leadership().get(0).map(|l| l.leader) {
                if delegatee != node.id() {
                    return self.delegator.delegate(delegatee, log_entry, args);
                }
            }
        }

        futures::future::ready(Evaluation::DoNotDelegate).boxed_local()
    }
}

/// Extends `NodeBuilder` to conveniently decorate a node with `Delegate`.
pub trait DelegateBuilderExt<I = ()> {
    /// Node type to be decorated.
    type Node: LeadershipAwareNode<I> + NodeImpl + 'static;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>;

    /// Decorates the node with `Delegate` using the given configuration.
    fn delegate<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<I, B> DelegateBuilderExt<I> for B
where
    I: 'static,
    B: ExtensibleNodeBuilder,
    B::Node: LeadershipAwareNode<I> + 'static,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static> =
        B::DecoratedBuilder<Delegate<B::Node, C, I>>;

    fn delegate<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

type BoxedCommitResult<N> = Result<CommitFor<N>, Box<dyn Any>>;

enum Append<C: Config> {
    EvaluatedDelegation {
        log_entry: Arc<LogEntryOf<C::Node>>,
        args: AppendArgs<InvocationOf<C::Node>, BoxedRetryPolicy<InvocationOf<C::Node>>>,
        commit: LocalBoxFuture<'static, Result<CommitFor<C::Node>, ShutDown>>,
        sender: oneshot::Sender<BoxedCommitResult<C::Node>>,
        result: Evaluation<C::DelegatedAppend>,
    },
    Delegated {
        log_entry: Arc<LogEntryOf<C::Node>>,
        args: AppendArgs<InvocationOf<C::Node>, BoxedRetryPolicy<InvocationOf<C::Node>>>,
        commit: LocalBoxFuture<'static, Result<CommitFor<C::Node>, ShutDown>>,
        sender: oneshot::Sender<BoxedCommitResult<C::Node>>,
        result: Result<(), AppendError<InvocationOf<C::Node>>>,
    },
    PerformedLocally {
        log_entry: Arc<LogEntryOf<C::Node>>,
        args: AppendArgs<InvocationOf<C::Node>, BoxedRetryPolicy<InvocationOf<C::Node>>>,
        commit: LocalBoxFuture<'static, Result<CommitFor<C::Node>, ShutDown>>,
        sender: oneshot::Sender<BoxedCommitResult<C::Node>>,
        result: Result<CommitFor<C::Node>, AppendError<InvocationOf<C::Node>>>,
    },
    EvaluatedRetry {
        log_entry: Arc<LogEntryOf<C::Node>>,
        args: AppendArgs<InvocationOf<C::Node>, BoxedRetryPolicy<InvocationOf<C::Node>>>,
        commit: LocalBoxFuture<'static, Result<CommitFor<C::Node>, ShutDown>>,
        sender: oneshot::Sender<BoxedCommitResult<C::Node>>,
        result: Result<(), Box<dyn Any>>,
    },
    Completed,
}

/// Delegate decoration.
pub struct Delegate<N, C, I = ()>
where
    N: LeadershipAwareNode<I> + 'static,
    C: Config<Node = N>,
{
    decorated: N,
    config: C,

    waker: std::task::Waker,
    appends: FuturesUnordered<LocalBoxFuture<'static, Append<C>>>,

    _p: std::marker::PhantomData<I>,
}

impl<N, C, I> Delegate<N, C, I>
where
    N: NodeImpl + LeadershipAwareNode<I> + 'static,
    C: Config<Node = N> + 'static,
{
    fn prepare_append(
        &self,
        log_entry: Arc<LogEntryOf<N>>,
        args: AppendArgs<InvocationOf<N>, BoxedRetryPolicy<InvocationOf<N>>>,
        commit: LocalBoxFuture<'static, Result<CommitFor<N>, ShutDown>>,
        sender: oneshot::Sender<Result<CommitFor<N>, Box<dyn Any>>>,
    ) {
        let args_prime = AppendArgs {
            round: args.round.clone(),
            importance: args.importance,
            retry_policy: DoNotRetry::new(),
        };

        let evaluation = self
            .config
            .evaluate(&self.decorated, Arc::clone(&log_entry), args_prime);

        self.appends.push(
            async move {
                Append::EvaluatedDelegation {
                    log_entry,
                    args,
                    commit,
                    sender,
                    result: evaluation.await,
                }
            }
            .boxed_local(),
        );

        self.waker.wake_by_ref();
    }
}

impl<N, C, I> Decoration for Delegate<N, C, I>
where
    N: NodeImpl + LeadershipAwareNode<I> + 'static,
    C: Config<Node = N> + 'static,
{
    type Arguments = C;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        mut arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        arguments.init(&decorated);

        Ok(Self {
            decorated,
            config: arguments,

            waker: futures::task::noop_waker(),
            appends: FuturesUnordered::new(),

            _p: std::marker::PhantomData,
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, C, I> Node for Delegate<N, C, I>
where
    N: LeadershipAwareNode<I> + NodeImpl + 'static,
    C: Config<Node = N> + 'static,
{
    type Invocation = InvocationOf<N>;
    type Shutdown = <N as Node>::Shutdown;

    fn id(&self) -> NodeIdOf<Self> {
        self.decorated.id()
    }

    fn status(&self) -> crate::NodeStatus {
        self.decorated.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.decorated.participation()
    }

    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> Poll<EventFor<Self>> {
        if !cx.waker().will_wake(&self.waker) {
            self.waker = cx.waker().clone();
        }

        while let std::task::Poll::Ready(Some(append)) = self.appends.poll_next_unpin(cx) {
            let (log_entry, mut args, commit, sender, err) = match append {
                Append::EvaluatedDelegation {
                    log_entry,
                    args,
                    commit,
                    sender,
                    result,
                } => match result {
                    Evaluation::Delegate(append) => {
                        self.appends.push(
                            async move {
                                Append::Delegated {
                                    log_entry,
                                    args,
                                    commit,
                                    sender,
                                    result: append.await,
                                }
                            }
                            .boxed_local(),
                        );

                        continue;
                    }

                    Evaluation::DoNotDelegate => {
                        let append = self
                            .decorated
                            .append_impl(
                                Arc::clone(&log_entry),
                                AppendArgs {
                                    round: args.round.clone(),
                                    importance: args.importance,
                                    retry_policy: DoNotRetry::new(),
                                },
                            )
                            .map_err(AppendError::from);

                        self.appends.push(
                            async move {
                                Append::PerformedLocally {
                                    log_entry,
                                    args,
                                    commit,
                                    sender,
                                    result: append.await,
                                }
                            }
                            .boxed_local(),
                        );

                        continue;
                    }

                    Evaluation::Error(err) => (log_entry, args, commit, sender, err),
                },
                Append::Delegated {
                    log_entry,
                    args,
                    commit,
                    sender,
                    result,
                } => match result {
                    Ok(_) => {
                        self.appends.push(
                            async move {
                                let _ = sender.send(Ok(commit
                                    .await
                                    .expect("Node is unexpectedly shut down.")));

                                Append::Completed
                            }
                            .boxed_local(),
                        );

                        continue;
                    }
                    Err(err) => (log_entry, args, commit, sender, err),
                },
                Append::PerformedLocally {
                    log_entry,
                    args,
                    commit,
                    sender,
                    result,
                } => match result {
                    Ok(commit) => {
                        let _ = sender.send(Ok(commit));

                        continue;
                    }
                    Err(err) => (log_entry, args, commit, sender, err),
                },
                Append::EvaluatedRetry {
                    log_entry,
                    args,
                    commit,
                    sender,
                    result,
                } => {
                    match result {
                        Ok(_) => self.prepare_append(log_entry, args, commit, sender),
                        Err(err) => {
                            let _ = sender.send(Err(err));
                        }
                    }

                    continue;
                }

                Append::Completed => {
                    // nothing to do
                    continue;
                }
            };

            self.appends.push(
                async move {
                    let result = args.retry_policy.eval(err).await;

                    Append::EvaluatedRetry {
                        log_entry,
                        args,
                        commit,
                        sender,
                        result,
                    }
                }
                .boxed_local(),
            )
        }

        let event = match self.decorated.poll_events(cx) {
            Poll::Ready(e) => e,
            Poll::Pending => return Poll::Pending,
        };

        self.config.update(&event);

        std::task::Poll::Ready(event)
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(&self) -> LocalBoxFuture<'static, SnapshotFor<Self>> {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> LocalBoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale(f)
    }

    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'_, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        self.append_impl(applicable, args)
            .map_err(|e| e.expect_other())
            .boxed_local()
    }

    fn append_static<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'static, StaticAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.append_impl(applicable, args)
            .map_err(|e| e.into())
            .boxed_local()
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N, C, I> NodeImpl for Delegate<N, C, I>
where
    N: NodeImpl + LeadershipAwareNode<I> + 'static,
    C: Config<Node = N> + 'static,
{
    fn append_impl<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, ImplAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        let log_entry = applicable.into_log_entry();
        let args = args.into();
        let args = AppendArgs {
            round: args.round,
            importance: args.importance,
            retry_policy: Box::new(BoxingRetryPolicy::from(args.retry_policy))
                as BoxedRetryPolicy<InvocationOf<N>>,
        };

        // TODO short circuit if allowed
        let commit = self.await_commit_of(log_entry.id());
        let (s, r) = oneshot::channel();

        self.prepare_append(log_entry, args, commit, s);

        async move {
            let commit = r
                .await
                .map(|r| r.map_err(|_| ShutDownOr::ShutDown))
                .map_err(|_| ShutDownOr::ShutDown)??;

            Ok(commit.projected())
        }
        .boxed_local()
    }

    fn await_commit_of(
        &self,
        log_entry_id: crate::node::LogEntryIdOf<Self>,
    ) -> LocalBoxFuture<'static, Result<crate::node::CommitFor<Self>, crate::error::ShutDown>> {
        self.decorated.await_commit_of(log_entry_id)
    }

    fn eject(
        &self,
        reason: crate::node::EjectionOf<Self>,
    ) -> LocalBoxFuture<'static, Result<bool, crate::error::ShutDown>> {
        self.decorated.eject(reason)
    }

    fn poll(
        &self,
        round_num: RoundNumOf<Self>,
        additional_nodes: Vec<crate::node::NodeOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<bool, PollError<Self::Invocation>>> {
        self.decorated.poll(round_num, additional_nodes)
    }
}

type BoxedRetryPolicy<I> = Box<
    dyn RetryPolicy<
        Invocation = I,
        Error = Box<dyn Any>,
        StaticError = Box<dyn Any>,
        Future = LocalBoxFuture<'static, Result<(), Box<dyn Any>>>,
    >,
>;

struct BoxingRetryPolicy<R> {
    delegate: R,
}

impl<R> From<R> for BoxingRetryPolicy<R> {
    fn from(delegate: R) -> Self {
        BoxingRetryPolicy { delegate }
    }
}

impl<R> RetryPolicy for BoxingRetryPolicy<R>
where
    R: RetryPolicy,
{
    type Invocation = <R as RetryPolicy>::Invocation;
    type Error = Box<dyn Any>;
    type StaticError = Box<dyn Any>;
    type Future = LocalBoxFuture<'static, Result<(), Self::Error>>;

    fn eval(&mut self, error: AppendError<Self::Invocation>) -> Self::Future {
        let f = self.delegate.eval(error);

        async move { f.await.map_err(|err| Box::new(err) as Self::Error) }.boxed_local()
    }
}
