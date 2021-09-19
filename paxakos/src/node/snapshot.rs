use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use num_traits::One;
use num_traits::Zero;

use crate::state::LogEntryOf;
use crate::state::State;
use crate::CoordNum;
use crate::RoundNum;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
pub struct Snapshot<S: State, R: RoundNum, C: CoordNum> {
    identity: Arc<()>,
    inner: Arc<SnapshotInner<S, R, C>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
struct SnapshotInner<S: State, R: RoundNum, C: CoordNum> {
    meta_state: MetaState<S, R, C>,
    state: Arc<S>,
}

/// See [super::Participation].
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(deserialize = ""))] // *sadface*
pub(crate) enum Participation<R: RoundNum, C: CoordNum> {
    Active,
    PartiallyActive(R),
    Passive { observed_proposals: HashSet<(R, C)> },
}

impl<'a, R: RoundNum, C: CoordNum> From<&'a Participation<R, C>> for super::Participation<R> {
    fn from(p: &'a Participation<R, C>) -> Self {
        match p {
            Participation::Active => super::Participation::Active,
            Participation::PartiallyActive(r) => super::Participation::PartiallyActive(*r),
            Participation::Passive { .. } => super::Participation::Passive,
        }
    }
}

impl<R: RoundNum, C: CoordNum> Participation<R, C> {
    pub(crate) fn passive() -> Self {
        Self::Passive {
            observed_proposals: HashSet::new(),
        }
    }
}

pub(crate) struct DeconstructedSnapshot<S: State, R: RoundNum, C: CoordNum> {
    pub(crate) state_round: R,
    pub(crate) state: Arc<S>,
    pub(crate) highest_observed_round_num: Option<R>,
    pub(crate) highest_observed_coord_num: C,
    pub(crate) participation: Participation<R, C>,
    pub(crate) promises: BTreeMap<R, C>,
    pub(crate) accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
struct MetaState<S: State, R: RoundNum, C: CoordNum> {
    state_round: R,
    highest_observed_round_num: Option<R>,
    highest_observed_coord_num: C,
    participation: Participation<R, C>,
    promises: BTreeMap<R, C>,
    accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

impl<S: State, R: RoundNum, C: CoordNum> Snapshot<S, R, C> {
    pub(crate) fn new(
        round: R,
        state: Arc<S>,
        highest_observed_round_num: Option<R>,
        highest_observed_coord_num: C,
        participation: Participation<R, C>,
        promises: BTreeMap<R, C>,
        accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
    ) -> Self {
        Self {
            identity: Arc::new(()),
            inner: Arc::new(SnapshotInner {
                meta_state: MetaState {
                    state_round: round,
                    highest_observed_round_num,
                    highest_observed_coord_num,
                    participation,
                    promises,
                    accepted_entries,
                },
                state,
            }),
        }
    }

    pub fn initial(state: impl Into<Arc<S>>) -> Self {
        let mut promises = BTreeMap::new();
        promises.insert(Zero::zero(), One::one());

        Self::new(
            Zero::zero(),
            state.into(),
            None,
            One::one(),
            Participation::Active,
            promises,
            BTreeMap::new(),
        )
    }

    pub fn round(&self) -> R {
        self.inner.meta_state.state_round
    }

    pub fn state(&self) -> &Arc<S> {
        &self.inner.state
    }

    pub(crate) fn deconstruct(self) -> DeconstructedSnapshot<S, R, C> {
        Arc::try_unwrap(self.inner)
            .map(|inner| DeconstructedSnapshot {
                state_round: inner.meta_state.state_round,
                state: inner.state,
                highest_observed_round_num: inner.meta_state.highest_observed_round_num,
                highest_observed_coord_num: inner.meta_state.highest_observed_coord_num,
                participation: inner.meta_state.participation,
                promises: inner.meta_state.promises,
                accepted_entries: inner.meta_state.accepted_entries,
            })
            .unwrap_or_else(|s| DeconstructedSnapshot {
                state_round: s.meta_state.state_round,
                state: Arc::clone(&s.state),
                highest_observed_round_num: s.meta_state.highest_observed_round_num,
                highest_observed_coord_num: s.meta_state.highest_observed_coord_num,
                participation: s.meta_state.participation.clone(),
                promises: s.meta_state.promises.clone(),
                accepted_entries: s.meta_state.accepted_entries.clone(),
            })
    }
}
