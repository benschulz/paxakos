use std::convert::Infallible;

use crate::state::LogEntryOf;
use crate::state::NodeOf;
use crate::state::State;
use crate::CoordNum;
use crate::RoundNum;

/// Shorthand to extract `Abstain` type out of `V`.
pub type AbstainOf<V> = <V as Voter>::Abstain;
/// Shorthand to extract `CoordNum` type out of `V`.
pub type CoordNumOf<V> = <V as Voter>::CoordNum;
/// Shorthand to extract `Nay` type out of `V`.
pub type NayOf<V> = <V as Voter>::Nay;
/// Shorthand to extract `RoundNum` type out of `V`.
pub type RoundNumOf<V> = <V as Voter>::RoundNum;
/// Shorthand to extract `State` type out of `V`.
pub type StateOf<V> = <V as Voter>::State;
/// Shorthand to extract `Yea` type out of `V`.
pub type YeaOf<V> = <V as Voter>::Yea;

pub trait Voter: Send + 'static {
    type State: State;
    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type Yea: std::fmt::Debug + Send + Sync;
    type Nay: std::fmt::Debug + Send + Sync;
    type Abstain: std::fmt::Debug + Send + Sync;

    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate_candidate(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<(), Infallible, Self::Abstain>;

    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate_proposal(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: &LogEntryOf<Self::State>,
        leader: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<Self::Yea, Self::Nay, Infallible>;

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

pub enum Decision<Y, N, A> {
    Abstain(A),
    Nay(N),
    Yea(Y),
}

#[derive(Default)]
pub struct IndiscriminateVoter<S, R, C, A, Y, N>(std::marker::PhantomData<(S, R, C, A, Y, N)>);

impl<S, R, C, A, Y, N> IndiscriminateVoter<S, R, C, A, Y, N> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<S, R, C, A, Y, N> Voter for IndiscriminateVoter<S, R, C, A, Y, N>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
    Y: std::fmt::Debug + Default + Send + Sync + 'static,
    N: std::fmt::Debug + Send + Sync + 'static,
{
    type State = S;
    type RoundNum = R;
    type CoordNum = C;

    type Yea = Y;
    type Nay = N;
    type Abstain = A;

    fn contemplate_candidate(
        &mut self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _candidate: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<(), Infallible, Self::Abstain> {
        Decision::Yea(())
    }

    fn contemplate_proposal(
        &mut self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _log_entry: &LogEntryOf<Self::State>,
        _leader: Option<&NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> Decision<Self::Yea, Self::Nay, Infallible> {
        Decision::Yea(Default::default())
    }
}
