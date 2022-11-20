#![allow(missing_docs)]

use std::task::Poll;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use num_traits::One;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::PollError;
use crate::error::ShutDownOr;
use crate::node::AppendResultFor;
use crate::node::DelegatingNodeImpl;
use crate::node::EventFor;
use crate::node::InvocationOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeOf;
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

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    /// Additional nodes to poll from.
    fn additional_nodes(&self) -> Vec<NodeOf<Self::Node>> {
        Vec::new()
    }
}

pub struct StaticConfig<N>(std::marker::PhantomData<N>);

impl<N: Node> Config for StaticConfig<N> {
    type Node = N;
}

impl<N: Node> Default for StaticConfig<N> {
    fn default() -> Self {
        Self(Default::default())
    }
}

pub trait CatchUpBuilderExt {
    type Node: Node + 'static;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>;

    fn catch_up<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<B> CatchUpBuilderExt for B
where
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static> =
        B::DecoratedBuilder<CatchUp<B::Node, C>>;

    fn catch_up<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

pub struct CatchUp<N, C>
where
    N: Node,
    C: Config,
{
    decorated: N,
    config: C,

    // TODO allow for batched catch-up
    poll: Option<BoxFuture<'static, Option<RoundNumOf<N>>>>,
}

impl<N, C> CatchUp<N, C>
where
    N: NodeImpl + 'static,
    C: Config<Node = N>,
{
    fn initiate_poll(&mut self, round_num: RoundNumOf<N>) {
        self.poll = Some(
            self.poll(round_num, self.config.additional_nodes())
                .then(move |r| async move {
                    match r {
                        Ok(true) => Some(round_num + One::one()),

                        // TODO back off in some fashion
                        Ok(false) => {
                            futures_timer::Delay::new(instant::Duration::from_secs(1)).await;
                            Some(round_num)
                        }

                        // TODO back off properly
                        Err(PollError::Decoration(_)) | Err(PollError::UselessResponses { .. }) => {
                            futures_timer::Delay::new(instant::Duration::from_secs(2)).await;
                            Some(round_num)
                        }

                        Err(PollError::Disoriented)
                        | Err(PollError::LocallyConverged)
                        | Err(PollError::NotRetained)
                        | Err(PollError::ShutDown) => None,
                    }
                })
                .boxed(),
        );
    }
}

impl<N, C> Decoration for CatchUp<N, C>
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

            poll: None,
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, C> Node for CatchUp<N, C>
where
    N: NodeImpl + 'static,
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
                    round,
                    state: Some(_),
                    ..
                }
                | crate::Event::Install {
                    round,
                    state: Some(_),
                    ..
                } => {
                    let next_round = *round + One::one();
                    self.initiate_poll(next_round);
                }

                _ => {}
            }
        }

        if let Some(p) = self.poll.as_mut() {
            match p.poll_unpin(cx) {
                Poll::Ready(Some(n)) => self.initiate_poll(n),
                Poll::Ready(None) => self.poll = None,
                Poll::Pending => {}
            }
        }

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

impl<N, C> DelegatingNodeImpl for CatchUp<N, C>
where
    N: NodeImpl + 'static,
    C: Config<Node = N>,
{
    type Delegate = N;

    fn delegate(&mut self) -> &mut Self::Delegate {
        &mut self.decorated
    }
}
