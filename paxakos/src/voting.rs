//! The paxakos consensus protocol has nodes elect leaders and vote on
//! proposals. Nodes will always vote 'no', if voting 'yes' would compromise
//! correctness. In all other cases, nodes will vote using their [`Voter`]
//! strategy. The default strategy, [`IndiscriminateVoter`] always votes 'yes'.
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

/// Strategy to vote on candidates and proposals.
pub trait Voter: Send + 'static {
    /// Type of shared state.
    type State: State;
    /// Round number type.
    type RoundNum: RoundNum;
    /// Coordination number type.
    type CoordNum: CoordNum;

    /// Data that will be added to any `Yea` vote on proposals.
    type Yea: std::fmt::Debug + Send + Sync;
    /// Data that will be added to any `Nay` vote on proposals.
    type Nay: std::fmt::Debug + Send + Sync;
    /// Data that will be added to any `Abstain` from elections.
    type Abstain: std::fmt::Debug + Send + Sync;

    /// Contemplate a bid for leadership.
    ///
    /// `candidate` will be `None` if the node cannot be inferred due to missing
    /// or outdated state.
    ///
    /// *Careful*: `state` is the current applied state and independent of
    /// `round_num`.
    fn contemplate_candidate(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        candidate: Option<&NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> Decision<(), Infallible, Self::Abstain>;

    /// Contemplate a proposed log entry.
    ///
    /// `leader` will be `None` if the node cannot be inferred due to missing
    /// or outdated state.
    ///
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

    /// Called for every commit.
    ///
    /// `leader` will be `None` if the node cannot be inferred due to missing
    /// or outdated state.
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

/// Voting decision, either `Yea`, `Nay` or `Abstain`.
pub enum Decision<Y, N, A> {
    /// Abstain, i.e. do not vote at all.
    Abstain(A),
    /// Nay, i.e. vote 'no'.
    Nay(N),
    /// Yea, i.e. vote 'yes'.
    Yea(Y),
}

/// A voter that always votes `Yea`.
#[derive(Default)]
pub struct IndiscriminateVoter<S, R, C, A, Y, N>(std::marker::PhantomData<(S, R, C, A, Y, N)>);

unsafe impl<S, R, C, A, Y, N> Send for IndiscriminateVoter<S, R, C, A, Y, N> {}

impl<S, R, C, A, Y, N> IndiscriminateVoter<S, R, C, A, Y, N> {
    /// Constructs a new `IndiscriminateVoter`.
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
