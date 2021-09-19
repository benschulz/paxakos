use std::collections::BTreeMap;
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

pub(crate) struct DeconstructedSnapshot<S: State, R: RoundNum, C: CoordNum> {
    pub(crate) state_round: R,
    pub(crate) state: Arc<S>,
    pub(crate) highest_observed_coord_num: C,
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
    highest_observed_coord_num: C,
    promises: BTreeMap<R, C>,
    accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

impl<S: State, R: RoundNum, C: CoordNum> Snapshot<S, R, C> {
    pub(crate) fn new(
        round: R,
        state: Arc<S>,
        highest_observed_coord_num: C,
        promises: BTreeMap<R, C>,
        accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
    ) -> Self {
        Self {
            identity: Arc::new(()),
            inner: Arc::new(SnapshotInner {
                meta_state: MetaState {
                    promises,
                    accepted_entries,
                    state_round: round,
                    highest_observed_coord_num,
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
            One::one(),
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
                highest_observed_coord_num: inner.meta_state.highest_observed_coord_num,
                promises: inner.meta_state.promises,
                accepted_entries: inner.meta_state.accepted_entries,
            })
            .unwrap_or_else(|s| DeconstructedSnapshot {
                state_round: s.meta_state.state_round,
                state: Arc::clone(&s.state),
                highest_observed_coord_num: s.meta_state.highest_observed_coord_num,
                promises: s.meta_state.promises.clone(),
                accepted_entries: s.meta_state.accepted_entries.clone(),
            })
    }
}
