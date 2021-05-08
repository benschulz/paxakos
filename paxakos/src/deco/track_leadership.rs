use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::task::Poll;

use futures::future::LocalBoxFuture;
use num_traits::{Bounded, One};

use crate::append::{AppendArgs, AppendError};
use crate::applicable::ApplicableTo;
use crate::communicator::Communicator;
use crate::node::builder::{NodeBuilder, NodeBuilderWithAll};
use crate::node::{CommitFor, CommunicatorOf, CoordNumOf, Node, NodeIdOf, NodeInfo, NodeKernel};
use crate::node::{NodeStatus, Participation, RoundNumOf, Snapshot, SnapshotFor, StateOf};
use crate::state::State;
use crate::RoundNum;

use super::Decoration;

pub trait LeadershipAwareNode<I>: Node {
    /// Leadership as assumed by this node.
    ///
    /// Note: This information is unreliable. A node will assume itself or
    /// another node to be leader when that node "owns" the highest coordination
    /// number observed for that round. That means neither that the node managed
    /// to achieve a quorum with that number, nor that no other node hasn't
    /// achieved a quorum with an even higher number.
    // TODO consider renaming to "lax_leadership" and introducing a strict version
    //      which disregards prepare messages
    fn leadership(&self) -> &[Leadership<NodeIdOf<Self>, RoundNumOf<Self>, CoordNumOf<Self>>];
}

pub trait MaybeLeadershipAwareNode<I>: Node {
    fn leadership(
        &self,
    ) -> Option<&[Leadership<NodeIdOf<Self>, RoundNumOf<Self>, CoordNumOf<Self>>]>;
}

pub trait TrackLeadershipBuilderExt: NodeBuilder {
    fn track_leadership(self) -> NodeBuilderWithAll<TrackLeadership<Self::Node>>;
}

impl<B> TrackLeadershipBuilderExt for B
where
    B: NodeBuilder,
{
    fn track_leadership(self) -> NodeBuilderWithAll<TrackLeadership<Self::Node>> {
        self.decorated_with(())
    }
}

pub struct TrackLeadership<N: Node> {
    decorated: N,
    suspended: bool,
    mandates: BTreeMap<RoundNumOf<N>, Mandate<N>>,
    leadership: Vec<Leadership<NodeIdOf<N>, RoundNumOf<N>, CoordNumOf<N>>>,
}

struct Mandate<N: Node> {
    mandate: CoordNumOf<N>,
    leader: NodeIdOf<N>,
    last_directive_at: std::time::Instant,
}

pub struct Leadership<N, R: RoundNum, C> {
    pub leader: N,
    pub rounds: RangeInclusive<R>,
    pub mandate: C,
    pub last_directive_at: std::time::Instant,
}

impl<N> Decoration for TrackLeadership<N>
where
    N: Node + 'static,
{
    type Arguments = ();

    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        _arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            suspended: false,
            mandates: BTreeMap::new(),
            leadership: Vec::new(),
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
    type State = StateOf<N>;
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

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<crate::Event<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            if let crate::Event::Init { .. } | crate::Event::Install { .. } = event {
                self.suspended = true;
                self.mandates.clear();
                self.leadership.clear();
            }

            if let crate::Event::StatusChange { new_status, .. } = event {
                match new_status {
                    NodeStatus::Lagging | NodeStatus::Disoriented => {
                        if !self.suspended {
                            self.suspended = true;
                            self.mandates.clear();
                            self.leadership.clear();
                        }
                    }
                    NodeStatus::Following | NodeStatus::Leading => self.suspended = false,
                }
            }

            if !self.suspended {
                if let crate::Event::Directive {
                    leader,
                    round_num,
                    coord_num,
                } = event
                {
                    let leader = leader.id();
                    let round_num = *round_num;
                    let coord_num = *coord_num;

                    let now = std::time::Instant::now();

                    let known_mandate = self
                        .mandates
                        .range_mut(..=round_num)
                        .last()
                        .filter(|(_, m)| m.mandate >= coord_num);

                    if let Some((_, m)) = known_mandate {
                        if m.mandate == coord_num {
                            m.last_directive_at = now;

                            for l in &mut self.leadership {
                                if l.mandate == coord_num {
                                    l.last_directive_at = now;
                                }
                            }
                        }
                    } else {
                        self.mandates
                            .drain_filter(|r, m| *r >= round_num && coord_num >= m.mandate);

                        self.mandates.insert(
                            round_num,
                            Mandate {
                                mandate: coord_num,
                                leader,
                                last_directive_at: now,
                            },
                        );

                        let mandates = self.mandates.iter().collect::<Vec<_>>();

                        self.leadership.clear();
                        for [(r1, m), (r2, _)] in mandates.array_windows::<2>() {
                            self.leadership.push(Leadership {
                                leader: m.leader,
                                rounds: **r1..=(**r2 - One::one()),
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

                if let crate::Event::Apply { round, .. } = event {
                    let round = *round;

                    let affects_first_mandate = self
                        .mandates
                        .first_key_value()
                        .filter(|(r, _)| {
                            if **r <= round {
                                assert_eq!(**r, round);
                                true
                            } else {
                                false
                            }
                        })
                        .is_some();

                    if affects_first_mandate {
                        let next_round = round + One::one();
                        let mandate = self.mandates.remove(&round).unwrap();

                        let obsolete = self
                            .mandates
                            .first_key_value()
                            .filter(|(r, _)| **r == next_round)
                            .is_some();

                        if obsolete {
                            self.leadership.remove(0);
                        } else {
                            self.mandates.insert(next_round, mandate);
                            self.leadership[0].rounds =
                                next_round..=*self.leadership[0].rounds.end();
                        }
                    }
                }
            }
        }

        event
    }

    fn handle(&self) -> crate::node::NodeHandle<Self::State, RoundNumOf<Self>, CoordNumOf<Self>> {
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
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'_, Result<std::sync::Arc<Self::State>, ()>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<RoundNumOf<Self>>,
    ) -> futures::future::LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<D, I> LeadershipAwareNode<(I,)> for D
where
    D: Decoration
        + Node<
            State = StateOf<<D as Decoration>::Decorated>,
            Communicator = CommunicatorOf<<D as Decoration>::Decorated>,
        >,
    <D as Decoration>::Decorated: LeadershipAwareNode<I>,
{
    fn leadership(&self) -> &[Leadership<NodeIdOf<D>, RoundNumOf<D>, CoordNumOf<D>>] {
        Decoration::peek_into(self).leadership()
    }
}

impl<N: Node> LeadershipAwareNode<()> for TrackLeadership<N> {
    fn leadership(&self) -> &[Leadership<NodeIdOf<N>, RoundNumOf<N>, CoordNumOf<N>>] {
        &self.leadership
    }
}

impl<D, I> MaybeLeadershipAwareNode<(I,)> for D
where
    D: Decoration
        + Node<
            State = StateOf<<D as Decoration>::Decorated>,
            Communicator = CommunicatorOf<<D as Decoration>::Decorated>,
        > + 'static,
    <D as Decoration>::Decorated: MaybeLeadershipAwareNode<I>,
{
    fn leadership(&self) -> Option<&[Leadership<NodeIdOf<D>, RoundNumOf<D>, CoordNumOf<D>>]> {
        if std::any::Any::type_id(self)
            == std::any::TypeId::of::<TrackLeadership<<D as Decoration>::Decorated>>()
        {
            let any: &dyn std::any::Any = self;
            let this = any
                .downcast_ref::<TrackLeadership<<D as Decoration>::Decorated>>()
                .unwrap();
            Some(LeadershipAwareNode::leadership(this))
        } else {
            Decoration::peek_into(self).leadership()
        }
    }
}

impl<S, C> MaybeLeadershipAwareNode<()> for NodeKernel<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    fn leadership(
        &self,
    ) -> Option<
        &[Leadership<
            crate::state::NodeIdOf<S>,
            crate::communicator::RoundNumOf<C>,
            crate::communicator::CoordNumOf<C>,
        >],
    > {
        None
    }
}
