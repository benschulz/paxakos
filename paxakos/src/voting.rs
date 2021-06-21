use crate::node::{self, CoordNumOf, NodeInfo, RoundNumOf, StateOf};
use crate::state::{LogEntryOf, NodeIdOf, NodeOf, State};
use crate::{CoordNum, RoundNum};

pub type AbstentionOf<V> = <V as Voter>::Abstention;

pub trait Voter: Send + 'static {
    type State: State;
    type RoundNum: RoundNum;
    type CoordNum: CoordNum;
    type Abstention: std::fmt::Debug + Send + Sync;

    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate(
        &self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Self::Abstention>;

    #[allow(unused_variables)]
    fn observe_accept(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
    ) {
    }

    #[allow(unused_variables)]
    fn observe_commit(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
    ) {
    }
}

pub enum Decision<J> {
    Abstain(J),
    Vote,
}

pub type IndiscriminateVoterFor<N> =
    IndiscriminateVoter<StateOf<N>, RoundNumOf<N>, CoordNumOf<N>, node::AbstentionOf<N>>;

#[derive(Default)]
pub struct IndiscriminateVoter<S, R, C, A>(std::marker::PhantomData<(S, R, C, A)>);

impl<S, R, C, A> IndiscriminateVoter<S, R, C, A> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<S, R, C, A> Voter for IndiscriminateVoter<S, R, C, A>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
{
    type State = S;
    type RoundNum = R;
    type CoordNum = C;
    type Abstention = A;

    fn contemplate(
        &self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _candidate: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<Self::Abstention> {
        Decision::Vote
    }
}

pub type AuthoritarianVoterFor<N> = AuthoritarianVoter<StateOf<N>, RoundNumOf<N>, CoordNumOf<N>>;

#[derive(Default)]
pub struct AuthoritarianVoter<S: State, R, C> {
    last_commit: Option<(NodeIdOf<S>, std::time::Instant)>,
    timeout: std::time::Duration,
    _p: std::marker::PhantomData<(R, C)>,
}

impl<S: State, R, C> AuthoritarianVoter<S, R, C> {
    pub fn with_timeout_of(timeout: std::time::Duration) -> Self {
        Self {
            last_commit: None,
            timeout,
            _p: std::marker::PhantomData,
        }
    }
}

impl<S, R, C> Voter for AuthoritarianVoter<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
{
    type State = S;
    type RoundNum = R;
    type CoordNum = C;
    type Abstention = std::time::Duration;

    fn contemplate(
        &self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<Self::Abstention> {
        if candidate.map(|c| c.id()) == self.last_commit.map(|c| c.0) {
            Decision::Vote
        } else if let Some((_, t)) = self.last_commit {
            let now = std::time::Instant::now();

            if now > t + self.timeout {
                Decision::Vote
            } else {
                Decision::Abstain(t + self.timeout - now)
            }
        } else {
            Decision::Vote
        }
    }

    fn observe_commit(
        &mut self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
    ) {
        self.last_commit = leader.map(|l| (l.id(), std::time::Instant::now()))
    }
}
