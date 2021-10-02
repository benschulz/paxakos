use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use num_traits::One;
use num_traits::Zero;

use crate::state::LogEntryOf;
use crate::state::State;
use crate::CoordNum;
use crate::RoundNum;

/// A snapshot of a node's state.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
pub struct Snapshot<S: State, R: RoundNum, C: CoordNum>(VersionedSnapshot<S, R, C>);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
pub enum VersionedSnapshot<S: State, R: RoundNum, C: CoordNum> {
    V1(SnapshotV1<S, R, C>),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
pub struct SnapshotV1<S: State, R: RoundNum, C: CoordNum> {
    round: R,
    state: Arc<S>,
    greatest_observed_round_num: Option<R>,
    greatest_observed_coord_num: C,
    participation: Participation<R, C>,
    promises: BTreeMap<R, C>,
    accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
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
    pub(crate) round: R,
    pub(crate) state: Arc<S>,
    pub(crate) greatest_observed_round_num: Option<R>,
    pub(crate) greatest_observed_coord_num: C,
    pub(crate) participation: Participation<R, C>,
    pub(crate) promises: BTreeMap<R, C>,
    pub(crate) accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

impl<S: State, R: RoundNum, C: CoordNum> Snapshot<S, R, C> {
    pub(crate) fn new(
        round: R,
        state: Arc<S>,
        greatest_observed_round_num: Option<R>,
        greatest_observed_coord_num: C,
        participation: Participation<R, C>,
        promises: BTreeMap<R, C>,
        accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
    ) -> Self {
        Self(VersionedSnapshot::V1(SnapshotV1 {
            round,
            state,
            greatest_observed_round_num,
            greatest_observed_coord_num,
            participation,
            promises,
            accepted_entries,
        }))
    }

    /// Creates an "initial" snapshot, i.e. a snapshot for round `0`.
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

    /// The round the node was in when the snapshot was taken.
    pub fn round(&self) -> R {
        match &self.0 {
            VersionedSnapshot::V1(s) => s.round,
        }
    }

    /// The state the node was in when the snapshot was taken.
    pub fn state(&self) -> &Arc<S> {
        match &self.0 {
            VersionedSnapshot::V1(s) => &s.state,
        }
    }

    pub(crate) fn deconstruct(self) -> DeconstructedSnapshot<S, R, C> {
        match self.0 {
            VersionedSnapshot::V1(s) => DeconstructedSnapshot {
                round: s.round,
                state: s.state,
                greatest_observed_round_num: s.greatest_observed_round_num,
                greatest_observed_coord_num: s.greatest_observed_coord_num,
                participation: s.participation,
                promises: s.promises,
                accepted_entries: s.accepted_entries,
            },
        }
    }
}
