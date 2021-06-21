use std::convert::Infallible;

use crate::node;
use crate::node::CoordNumOf;
use crate::node::RoundNumOf;
use crate::node::StateOf;
use crate::state::LogEntryOf;
use crate::state::NodeOf;
use crate::state::State;
use crate::CoordNum;
use crate::RoundNum;

pub type AbstentionOf<V> = <V as Voter>::Abstention;

pub trait Voter: Send + 'static {
    type State: State;
    type RoundNum: RoundNum;
    type CoordNum: CoordNum;
    type Abstention: std::fmt::Debug + Send + Sync;
    type Rejection: std::fmt::Debug + Send + Sync;

    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate_candidate(
        &self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Self::Abstention, Infallible>;

    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate_proposal(
        &self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Infallible, Self::Rejection>;

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

pub enum Decision<A, R> {
    Abstain(A),
    Reject(R),
    Vote,
}

pub type IndiscriminateVoterFor<N> = IndiscriminateVoter<
    StateOf<N>,
    RoundNumOf<N>,
    CoordNumOf<N>,
    node::AbstentionOf<N>,
    node::RejectionOf<N>,
>;

#[derive(Default)]
pub struct IndiscriminateVoter<S, R, C, A, J>(std::marker::PhantomData<(S, R, C, A, J)>);

impl<S, R, C, A, J> IndiscriminateVoter<S, R, C, A, J> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<S, R, C, A, J> Voter for IndiscriminateVoter<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
    J: std::fmt::Debug + Send + Sync + 'static,
{
    type State = S;
    type RoundNum = R;
    type CoordNum = C;
    type Abstention = A;
    type Rejection = J;

    fn contemplate_candidate(
        &self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _candidate: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<Self::Abstention, Infallible> {
        Decision::Vote
    }

    fn contemplate_proposal(
        &self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _log_entry: &LogEntryOf<Self::State>,
        _leader: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<Infallible, Self::Rejection> {
        Decision::Vote
    }
}
