use std::convert::Infallible;
use std::task::Poll;

use either::Either;
use futures::channel::mpsc;
use futures::SinkExt;
use futures::StreamExt;
use num_traits::Zero;
use smallvec::SmallVec;

use crate::state::LogEntryOf;
use crate::state::NodeIdOf;
use crate::state::NodeOf;
use crate::voting::AbstainOf;
use crate::voting::CoordNumOf;
use crate::voting::Decision;
use crate::voting::IndiscriminateVoter;
use crate::voting::NayOf;
use crate::voting::RoundNumOf;
use crate::voting::Voter;
use crate::voting::YeaOf;
use crate::CoordNum;
use crate::NodeInfo;
use crate::RoundNum;
use crate::State;

#[derive(Clone, Copy, Debug)]
pub struct Lease<I> {
    pub lessee: Option<I>,
    pub end: instant::Instant,
}

#[derive(Debug)]
pub struct Subscription<I>(mpsc::Receiver<Lease<I>>);

impl<I> futures::stream::Stream for Subscription<I> {
    type Item = Lease<I>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(lease)) => Poll::Ready(Some(lease)),
            _ => Poll::Pending,
        }
    }
}

pub struct LeaseGrantingVoter<S: State, V: Voter> {
    delegate: GeneralLeaseGrantingVoter<S, V>,
}

impl<S, R, C, N> LeaseGrantingVoter<S, IndiscriminateVoter<S, R, C, Infallible, (), N>>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    N: std::fmt::Debug + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::from(IndiscriminateVoter::new())
    }
}

impl<S, V> LeaseGrantingVoter<S, V>
where
    S: State,
    V: Voter<Yea = (), Abstain = Infallible>,
{
    pub fn from(voter: V) -> Self {
        Self {
            delegate: GeneralLeaseGrantingVoter::from(voter),
        }
    }

    pub fn subscribe(&mut self) -> Subscription<NodeIdOf<S>> {
        self.delegate.subscribe()
    }
}

impl<S, R, C, N> Default for LeaseGrantingVoter<S, IndiscriminateVoter<S, R, C, Infallible, (), N>>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    N: std::fmt::Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, V> Voter for LeaseGrantingVoter<S, V>
where
    S: State,
    V: Voter<State = S, Yea = (), Abstain = Infallible>,
{
    type State = S;

    type RoundNum = RoundNumOf<V>;
    type CoordNum = CoordNumOf<V>;

    type Yea = std::time::Duration;
    type Nay = NayOf<V>;
    type Abstain = std::time::Duration;

    fn contemplate_candidate(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<(), std::convert::Infallible, Self::Abstain> {
        match self
            .delegate
            .contemplate_candidate(round_num, coord_num, candidate, state)
        {
            Decision::Abstain(a) => Decision::Abstain(a.left().unwrap()),
            Decision::Nay(n) => Decision::Nay(n),
            Decision::Yea(y) => Decision::Yea(y),
        }
    }

    fn contemplate_proposal(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Self::Yea, Self::Nay, std::convert::Infallible> {
        match self
            .delegate
            .contemplate_proposal(round_num, coord_num, log_entry, leader, state)
        {
            Decision::Abstain(a) => Decision::Abstain(a),
            Decision::Nay(n) => Decision::Nay(n),
            Decision::Yea(y) => Decision::Yea(y.0),
        }
    }

    fn observe_commit(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
    ) {
        self.delegate
            .observe_commit(round_num, coord_num, log_entry, leader);
    }
}

pub struct GeneralLeaseGrantingVoter<S: State, V: Voter> {
    created_at: instant::Instant,
    leader_coord_num: CoordNumOf<V>,
    leader_id: Option<NodeIdOf<S>>,
    moratorium: Option<instant::Instant>,
    delegate: V,
    subscriptions: SmallVec<[mpsc::Sender<Lease<NodeIdOf<S>>>; 1]>,
}

impl<S, V> GeneralLeaseGrantingVoter<S, V>
where
    S: State,
    V: Voter,
{
    pub fn from(voter: V) -> Self {
        Self {
            created_at: instant::Instant::now(),
            leader_coord_num: Zero::zero(),
            leader_id: None,
            moratorium: None,
            delegate: voter,
            subscriptions: SmallVec::new(),
        }
    }

    pub fn subscribe(&mut self) -> Subscription<NodeIdOf<S>> {
        let (send, recv) = mpsc::channel(16);

        self.subscriptions.push(send);

        Subscription(recv)
    }
}

impl<S, V> Voter for GeneralLeaseGrantingVoter<S, V>
where
    S: State,
    V: Voter<State = S>,
{
    type State = S;

    type RoundNum = RoundNumOf<V>;
    type CoordNum = CoordNumOf<V>;

    type Yea = (std::time::Duration, YeaOf<V>);
    type Nay = NayOf<V>;
    type Abstain = Either<std::time::Duration, AbstainOf<V>>;

    fn contemplate_candidate(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&crate::state::NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<(), std::convert::Infallible, Self::Abstain> {
        match self
            .delegate
            .contemplate_candidate(round_num, coord_num, candidate, state)
        {
            Decision::Abstain(a) => Decision::Abstain(Either::Right(a)),
            Decision::Nay(n) => Decision::Nay(n),
            Decision::Yea(()) => match self.moratorium {
                Some(deadline) => {
                    let now = instant::Instant::now();

                    if now >= deadline
                        || self.leader_id.is_some() && self.leader_id == candidate.map(|l| l.id())
                    {
                        Decision::Yea(())
                    } else {
                        Decision::Abstain(Either::Left(deadline - now))
                    }
                }
                None => Decision::Yea(()),
            },
        }
    }

    fn contemplate_proposal(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &crate::state::LogEntryOf<Self::State>,
        leader: Option<&crate::state::NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Self::Yea, Self::Nay, std::convert::Infallible> {
        match self
            .delegate
            .contemplate_proposal(round_num, coord_num, log_entry, leader, state)
        {
            Decision::Abstain(a) => Decision::Abstain(a),
            Decision::Nay(n) => Decision::Nay(n),
            Decision::Yea(y) => {
                // TODO allow deriving from State
                let duration = std::time::Duration::from_millis(1500);
                let now = instant::Instant::now();

                // Make "sure" there can be no overlap with a lease from a previous run.
                //
                // TODO This is crude and, once lease durations may be derived, prone to
                //      double-lending when lease durations are reduced.
                //
                //      I don't see any way around this except through documentation. When
                //      durations are reduced, this must happen continuously rather than
                //      discretely, i.e. reducing by 500ms will take 500ms.
                let no_potential_conflicts = self.created_at + duration < now;

                if no_potential_conflicts && coord_num == self.leader_coord_num {
                    let lessee = leader.map(|l| l.id());
                    let end = now + duration;
                    let lease = Lease { lessee, end };

                    self.moratorium = self.moratorium.map(|e| std::cmp::max(e, end)).or(Some(end));

                    self.subscriptions
                        .retain(move |s| futures::executor::block_on(s.send(lease)).is_ok());

                    Decision::Yea((duration, y))
                } else {
                    Decision::Yea((std::time::Duration::ZERO, y))
                }
            }
        }
    }

    fn observe_commit(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
    ) {
        self.delegate
            .observe_commit(round_num, coord_num, log_entry, leader);

        if self.leader_coord_num < coord_num {
            self.leader_coord_num = coord_num;
        }

        if coord_num == self.leader_coord_num && self.leader_id.is_none() && leader.is_some() {
            self.leader_id = leader.map(|l| l.id())
        }
    }
}
