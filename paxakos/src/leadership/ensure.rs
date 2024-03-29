use std::task::Poll;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::node::AppendResultFor;
use crate::node::DelegatingNodeImpl;
use crate::node::EventFor;
use crate::node::InvocationOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::RetryPolicy;
use crate::Node;

/// Ensure leadership configuration.
pub trait Config {
    /// The node type that is decorated.
    type Node: Node;

    /// The applicable that is used to take leadership, usually a no-op.
    type Applicable: ApplicableTo<StateOf<Self::Node>> + 'static;

    /// Type of retry policy to be used.
    ///
    /// See [`retry_policy`][Config::retry_policy].
    type RetryPolicy: RetryPolicy<
        Invocation = InvocationOf<Self::Node>,
        Error = AppendError<InvocationOf<Self::Node>>,
        StaticError = AppendError<InvocationOf<Self::Node>>,
    >;

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    /// Interval after which leadership is taken.
    fn interval(&self) -> Option<Duration> {
        None
    }

    /// Creates a new value to take leadership.
    fn new_leadership_taker(&self) -> Self::Applicable;

    /// Creates a retry policy.
    fn retry_policy(&self) -> Self::RetryPolicy;
}

pub trait EnsureLeadershipBuilderExt {
    type Node: Node + 'static;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>;

    fn ensure_leadership<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<B> EnsureLeadershipBuilderExt for B
where
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static> =
        B::DecoratedBuilder<EnsureLeadership<B::Node, C>>;

    fn ensure_leadership<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

#[derive(Debug)]
pub struct EnsureLeadership<N, C>
where
    N: Node,
    C: Config,
{
    decorated: N,
    config: C,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<BoxFuture<'static, ()>>,
}

impl<N, C> EnsureLeadership<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn ensure_leadership(&mut self) {
        let log_entry = self.config.new_leadership_taker();

        let append = self
            .decorated
            .append(log_entry, self.config.retry_policy())
            .map(|_| ())
            .boxed();

        self.appends.push(append);
    }
}

impl<N, C> Decoration for EnsureLeadership<N, C>
where
    N: NodeImpl + 'static,
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

            timer: None,

            appends: futures::stream::FuturesUnordered::new(),
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, C> Node for EnsureLeadership<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
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
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            self.config.update(event);

            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.timer = match (new_status, self.config.interval()) {
                        (NodeStatus::Disoriented, _) | (_, None) => None,
                        (_, Some(i)) => Some(futures_timer::Delay::new(i)),
                    };
                }

                crate::Event::Install { .. } | crate::Event::Apply { .. } => {
                    self.timer = self.config.interval().map(futures_timer::Delay::new);
                }

                _ => {}
            }
        }

        while let Some(timer) = &mut self.timer {
            if timer.poll_unpin(cx).is_pending() {
                break;
            }

            self.ensure_leadership();

            self.timer = self.config.interval().map(futures_timer::Delay::new);
        }

        let _ = self.appends.poll_next_unpin(cx);

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(&self) -> BoxFuture<'static, SnapshotFor<Self>> {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> BoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale(f)
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> BoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> BoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> BoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped_infallibly(f)
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
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N, C> DelegatingNodeImpl for EnsureLeadership<N, C>
where
    N: NodeImpl + 'static,
    C: Config<Node = N>,
{
    type Delegate = N;

    fn delegate(&mut self) -> &mut Self::Delegate {
        &mut self.decorated
    }
}
