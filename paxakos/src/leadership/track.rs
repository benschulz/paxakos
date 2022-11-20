use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::task::Poll;

use futures::future::LocalBoxFuture;
use num_traits::Bounded;
use num_traits::One;
use num_traits::Zero;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::communicator::Communicator;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::event::DirectiveKind;
use crate::invocation;
use crate::invocation::Invocation;
use crate::node::CoordNumOf;
use crate::node::Core;
use crate::node::DelegatingNodeImpl;
use crate::node::EventFor;
use crate::node::InvocationOf;
use crate::node::Node;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeInfo;
use crate::node::NodeOf;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::StaticAppendResultFor;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::RetryPolicy;
use crate::RoundNum;
use crate::Shell;

pub type LeadershipFor<N> = Leadership<NodeIdOf<N>, RoundNumOf<N>, CoordNumOf<N>>;

pub trait LeadershipAwareNode<I>: Node {
    /// Leadership as assumed by this node.
    ///
    /// Note: This information is unreliable. A node will assume itself or
    /// another node to be leader when that node "owns" the greatest
    /// coordination number observed for that round. That means neither that
    /// the node managed to achieve a quorum with that number, nor that no other
    /// node achieved a quorum with an even higher number.
    fn lax_leadership(&self) -> &[LeadershipFor<Self>];

    /// Leadership as assumed by this node.
    ///
    /// Note: This information is unreliable. Strict leadership is inferred when
    /// a node sent a proposal or a commit message. That means the node was
    /// leader at some point, but anothor node may have supplanted it by now.
    fn strict_leadership(&self) -> &[LeadershipFor<Self>];
}

pub trait MaybeLeadershipAwareNode<I>: Node {
    fn lax_leadership(&self) -> Option<&[LeadershipFor<Self>]>;

    fn strict_leadership(&self) -> Option<&[LeadershipFor<Self>]>;
}

pub trait TrackLeadershipBuilderExt {
    type Node: Node;
    type DecoratedBuilder;

    fn track_leadership(self) -> Self::DecoratedBuilder;
}

impl<B> TrackLeadershipBuilderExt for B
where
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
{
    type Node = B::Node;
    type DecoratedBuilder = B::DecoratedBuilder<TrackLeadership<B::Node>>;

    fn track_leadership(self) -> Self::DecoratedBuilder {
        self.decorated_with(())
    }
}

pub struct TrackLeadership<N: Node> {
    decorated: N,
    suspended: bool,
    min_round: RoundNumOf<N>,
    lax_inferrer: Inferrer<N>,
    strict_inferrer: Inferrer<N>,
}

struct Inferrer<N: Node> {
    mandates: BTreeMap<RoundNumOf<N>, Mandate<N>>,
    leadership: Vec<LeadershipFor<N>>,
}

struct Mandate<N: Node> {
    mandate: CoordNumOf<N>,
    leader: NodeIdOf<N>,
    last_directive_at: instant::Instant,
}

pub struct Leadership<N, R: RoundNum, C> {
    pub leader: N,
    pub rounds: RangeInclusive<R>,
    pub mandate: C,
    pub last_directive_at: instant::Instant,
}

impl<N: Node> Inferrer<N> {
    fn new() -> Self {
        Self {
            mandates: BTreeMap::new(),
            leadership: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.mandates.clear();
        self.leadership.clear();
    }

    fn react_to_apply(&mut self, round: RoundNumOf<N>) {
        let affects_first_mandate = self
            .mandates
            .iter()
            .next()
            .filter(|(r, _)| **r <= round)
            .is_some();

        if affects_first_mandate {
            let next_round = round + One::one();
            let first_key = *self.mandates.keys().next().unwrap();
            let mandate = self.mandates.remove(&first_key).unwrap();

            let obsolete = self
                .mandates
                .iter()
                .next()
                .filter(|(r, _)| **r == next_round)
                .is_some();

            if obsolete {
                self.leadership.remove(0);
            } else {
                self.mandates.insert(next_round, mandate);
                self.leadership[0].rounds = next_round..=*self.leadership[0].rounds.end();
            }
        }
    }

    fn react_to_directive(
        &mut self,
        leader: &NodeOf<N>,
        round_num: RoundNumOf<N>,
        coord_num: CoordNumOf<N>,
        timestamp: instant::Instant,
    ) {
        let leader = leader.id();

        let known_mandate = self
            .mandates
            .range_mut(..=round_num)
            .last()
            .filter(|(_, m)| m.mandate >= coord_num);

        if let Some((_, m)) = known_mandate {
            if m.mandate == coord_num {
                m.last_directive_at = timestamp;

                for l in &mut self.leadership {
                    if l.mandate == coord_num {
                        l.last_directive_at = timestamp;
                    }
                }
            }
        } else {
            // retain mandates that aren't overridden
            self.mandates
                .retain(|r, m| *r < round_num || m.mandate > coord_num);

            self.mandates.insert(
                round_num,
                Mandate {
                    mandate: coord_num,
                    leader,
                    last_directive_at: timestamp,
                },
            );

            let mandates = self.mandates.iter().collect::<Vec<_>>();

            self.leadership.clear();
            for ms in mandates.windows(2) {
                let (r1, m) = ms[0];
                let (r2, _) = ms[1];

                self.leadership.push(Leadership {
                    leader: m.leader,
                    rounds: *r1..=(*r2 - One::one()),
                    mandate: m.mandate,
                    last_directive_at: m.last_directive_at,
                });
            }

            let (r, m) = mandates.last().unwrap();
            self.leadership.push(Leadership {
                leader: m.leader,
                rounds: **r..=Bounded::max_value(),
                mandate: m.mandate,
                last_directive_at: m.last_directive_at,
            });
        }
    }
}

impl<N> Decoration for TrackLeadership<N>
where
    N: NodeImpl + 'static,
{
    type Arguments = ();

    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        _arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            decorated,
            suspended: false,
            min_round: Zero::zero(),
            lax_inferrer: Inferrer::new(),
            strict_inferrer: Inferrer::new(),
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N> Node for TrackLeadership<N>
where
    N: Node,
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
            match event {
                crate::Event::Init { round, .. } | crate::Event::Install { round, .. } => {
                    self.suspended = true;
                    self.min_round = *round + One::one();
                    self.lax_inferrer.reset();
                    self.strict_inferrer.reset();
                }
                crate::Event::StatusChange { new_status, .. } => match new_status {
                    NodeStatus::Lagging | NodeStatus::Disoriented => {
                        if !self.suspended {
                            self.suspended = true;
                            self.lax_inferrer.reset();
                            self.strict_inferrer.reset();
                        }
                    }
                    NodeStatus::Following | NodeStatus::Leading => self.suspended = false,
                },
                crate::Event::Apply { round, .. } => {
                    self.min_round = *round + One::one();

                    if !self.suspended {
                        self.lax_inferrer.react_to_apply(*round);
                        self.strict_inferrer.react_to_apply(*round);
                    }
                }
                crate::Event::Directive {
                    kind,
                    leader,
                    round_num,
                    coord_num,
                    timestamp,
                } => {
                    if !self.suspended && *round_num >= self.min_round {
                        self.lax_inferrer
                            .react_to_directive(leader, *round_num, *coord_num, *timestamp);

                        if *kind != DirectiveKind::Prepare {
                            self.strict_inferrer
                                .react_to_directive(leader, *round_num, *coord_num, *timestamp);
                        }
                    }
                }
                _ => {}
            }
        }

        event
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

    fn read_stale_infallibly<F, T>(&self, f: F) -> LocalBoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> LocalBoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> LocalBoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped_infallibly(f)
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
        self.decorated.append_static(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N> DelegatingNodeImpl for TrackLeadership<N>
where
    N: NodeImpl,
{
    type Delegate = N;

    fn delegate(&self) -> &Self::Delegate {
        &self.decorated
    }
}

impl<D, I> LeadershipAwareNode<(I,)> for D
where
    D: Decoration,
    <D as Decoration>::Decorated: LeadershipAwareNode<I>,
{
    fn lax_leadership(&self) -> &[LeadershipFor<D>] {
        Decoration::peek_into(self).lax_leadership()
    }

    fn strict_leadership(&self) -> &[LeadershipFor<D>] {
        Decoration::peek_into(self).strict_leadership()
    }
}

impl<N, I> LeadershipAwareNode<(I,)> for Shell<N>
where
    N: NodeImpl + LeadershipAwareNode<I>,
{
    fn lax_leadership(&self) -> &[LeadershipFor<N>] {
        self.wrapped.lax_leadership()
    }

    fn strict_leadership(&self) -> &[LeadershipFor<N>] {
        self.wrapped.strict_leadership()
    }
}

impl<N: Node> LeadershipAwareNode<()> for TrackLeadership<N> {
    fn lax_leadership(&self) -> &[LeadershipFor<N>] {
        &self.lax_inferrer.leadership
    }

    fn strict_leadership(&self) -> &[LeadershipFor<N>] {
        &self.strict_inferrer.leadership
    }
}

impl<D, I> MaybeLeadershipAwareNode<(I,)> for D
where
    D: Decoration + 'static,
    <D as Decoration>::Decorated: MaybeLeadershipAwareNode<I>,
{
    fn lax_leadership(&self) -> Option<&[LeadershipFor<D>]> {
        if std::any::Any::type_id(self)
            == std::any::TypeId::of::<TrackLeadership<<D as Decoration>::Decorated>>()
        {
            let any: &dyn std::any::Any = self;
            let this = any
                .downcast_ref::<TrackLeadership<<D as Decoration>::Decorated>>()
                .unwrap();
            Some(LeadershipAwareNode::lax_leadership(this))
        } else {
            Decoration::peek_into(self).lax_leadership()
        }
    }

    fn strict_leadership(&self) -> Option<&[LeadershipFor<D>]> {
        if std::any::Any::type_id(self)
            == std::any::TypeId::of::<TrackLeadership<<D as Decoration>::Decorated>>()
        {
            let any: &dyn std::any::Any = self;
            let this = any
                .downcast_ref::<TrackLeadership<<D as Decoration>::Decorated>>()
                .unwrap();
            Some(LeadershipAwareNode::strict_leadership(this))
        } else {
            Decoration::peek_into(self).strict_leadership()
        }
    }
}

impl<I, C> MaybeLeadershipAwareNode<()> for Core<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
{
    fn lax_leadership(&self) -> Option<&[LeadershipFor<Self>]> {
        None
    }

    fn strict_leadership(&self) -> Option<&[LeadershipFor<Self>]> {
        None
    }
}
