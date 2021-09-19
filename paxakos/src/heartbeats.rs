use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::append::DoNotRetry;
use crate::append::Importance;
use crate::append::Peeryness;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::leadership::track::MaybeLeadershipAwareNode;
use crate::node::builder::NodeBuilder;
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::EventOf;
use crate::node::InvocationOf;
use crate::node::NayOf;
use crate::node::NodeIdOf;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::YeaOf;
use crate::voting::Voter;
use crate::Node;

pub trait Config {
    type Node: Node;
    type Applicable: ApplicableTo<StateOf<Self::Node>> + 'static;

    fn init(&mut self, state: &StateOf<Self::Node>);

    fn update(&mut self, event: &EventOf<Self::Node>);

    fn new_heartbeat(&self, node: &Self::Node) -> Self::Applicable;

    fn interval(&self, node: &Self::Node) -> Option<Duration>;
}

pub struct StaticConfig<N, A, I = ()> {
    interval: Duration,
    leader_interval: Option<Duration>,

    _p: std::marker::PhantomData<(N, A, I)>,
}

impl<N, A, I> StaticConfig<N, A, I>
where
    N: Node + MaybeLeadershipAwareNode<I>,
    A: ApplicableTo<StateOf<N>> + Default + 'static,
{
    pub fn with_interval(interval: Duration) -> Self {
        Self {
            interval,
            leader_interval: None,

            _p: std::marker::PhantomData,
        }
    }

    pub fn when_leading(self, leader_interval: Duration) -> Self {
        Self {
            interval: self.interval,
            leader_interval: Some(leader_interval),

            _p: std::marker::PhantomData,
        }
    }
}

impl<N, A, I> Config for StaticConfig<N, A, I>
where
    N: Node + MaybeLeadershipAwareNode<I>,
    A: ApplicableTo<StateOf<N>> + Default + 'static,
{
    type Node = N;
    type Applicable = A;

    fn init(&mut self, _state: &StateOf<Self::Node>) {}

    fn update(&mut self, _event: &EventOf<Self::Node>) {}

    fn new_heartbeat(&self, _node: &Self::Node) -> Self::Applicable {
        Self::Applicable::default()
    }

    fn interval(&self, node: &Self::Node) -> Option<Duration> {
        Some(if let Some(interval) = self.leader_interval {
            let leadership = node.leadership().expect("leadership not tracked");

            if leadership.first().map(|l| l.leader) == Some(node.id()) {
                interval
            } else {
                self.interval
            }
        } else {
            self.interval
        })
    }
}

pub trait SendHeartbeatsBuilderExt {
    type Node: Node + 'static;
    type Voter: Voter;

    fn send_heartbeats<C>(
        self,
        config: C,
    ) -> NodeBuilder<SendHeartbeats<Self::Node, C>, Self::Voter>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<N, V> SendHeartbeatsBuilderExt for NodeBuilder<N, V>
where
    N: Node + 'static,
    V: Voter<
        State = StateOf<N>,
        RoundNum = RoundNumOf<N>,
        CoordNum = CoordNumOf<N>,
        Abstain = AbstainOf<N>,
        Yea = YeaOf<N>,
        Nay = NayOf<N>,
    >,
{
    type Node = N;
    type Voter = V;

    fn send_heartbeats<C>(
        self,
        config: C,
    ) -> NodeBuilder<SendHeartbeats<Self::Node, C>, Self::Voter>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

#[derive(Debug)]
pub struct SendHeartbeats<N, C>
where
    N: Node,
    C: Config<Node = N>,
{
    decorated: N,
    config: C,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, ()>>,
}

impl<N, C> SendHeartbeats<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn new_timer(&self) -> Option<futures_timer::Delay> {
        self.config
            .interval(&self.decorated)
            .map(futures_timer::Delay::new)
    }

    fn send_heartbeat(&mut self) {
        let append = self
            .decorated
            .append(
                self.config.new_heartbeat(&self.decorated),
                AppendArgs {
                    importance: Importance::MaintainLeadership(Peeryness::Peery),
                    retry_policy: Box::new(DoNotRetry::new()),
                    ..Default::default()
                },
            )
            .map(|_| ())
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, C> Decoration for SendHeartbeats<N, C>
where
    N: Node + 'static,
    C: Config<Node = N> + 'static,
{
    type Arguments = C;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
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

impl<N, C> Node for SendHeartbeats<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    type Invocation = InvocationOf<N>;
    type Communicator = CommunicatorOf<N>;
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
            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.timer = match new_status {
                        NodeStatus::Disoriented => None,
                        _ => self.new_timer(),
                    };
                }

                crate::Event::Install { .. } | crate::Event::Apply { .. } => {
                    self.timer = self.new_timer();
                }

                _ => {}
            }
        }

        while let Some(timer) = &mut self.timer {
            if timer.poll_unpin(cx).is_pending() {
                break;
            }

            self.send_heartbeat();

            self.timer = self.new_timer();
        }

        let _ = self.appends.poll_next_unpin(cx);

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>
    {
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

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<StateOf<Self>> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<Self::Invocation>,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}
