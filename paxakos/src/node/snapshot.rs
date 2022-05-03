use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use num_traits::One;
use num_traits::Zero;

use crate::state::FrozenOf;
use crate::state::LogEntryOf;
use crate::state::State;
use crate::CoordNum;
use crate::RoundNum;

/// A snapshot of a node's state.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize, S::Frozen: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>, S::Frozen: serde::Deserialize<'de>"
))]
pub struct Snapshot<S: State, R: RoundNum, C: CoordNum>(VersionedSnapshot<S, R, C>);

impl<S: State, R: RoundNum, C: CoordNum> Clone for Snapshot<S, R, C> {
    fn clone(&self) -> Self {
        Snapshot(match &self.0 {
            VersionedSnapshot::V1(v1) => VersionedSnapshot::V3(SnapshotV3 {
                round: v1.round,
                state: Some(Arc::new(v1.state.freeze())),
                greatest_observed_round_num: v1.greatest_observed_round_num,
                greatest_observed_coord_num: v1.greatest_observed_coord_num,
                participation: v1.participation.clone(),
                promises: v1.promises.clone(),
                accepted_entries: v1.accepted_entries.clone(),
            }),
            VersionedSnapshot::V2(v2) => VersionedSnapshot::V3(SnapshotV3 {
                round: v2.round,
                state: v2.state.as_deref().map(|s| Arc::new(s.freeze())),
                greatest_observed_round_num: v2.greatest_observed_round_num,
                greatest_observed_coord_num: v2.greatest_observed_coord_num,
                participation: v2.participation.clone(),
                promises: v2.promises.clone(),
                accepted_entries: v2.accepted_entries.clone(),
            }),
            VersionedSnapshot::V3(v3) => VersionedSnapshot::V3(SnapshotV3 {
                round: v3.round,
                state: v3.state.as_ref().map(Arc::clone),
                greatest_observed_round_num: v3.greatest_observed_round_num,
                greatest_observed_coord_num: v3.greatest_observed_coord_num,
                participation: v3.participation.clone(),
                promises: v3.promises.clone(),
                accepted_entries: v3.accepted_entries.clone(),
            }),
        })
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize, S::Frozen: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>, S::Frozen: serde::Deserialize<'de>"
))]
pub enum VersionedSnapshot<S: State, R: RoundNum, C: CoordNum> {
    V1(SnapshotV1<S, R, C>),
    V2(SnapshotV2<S, R, C>),
    V3(SnapshotV3<S, R, C>),
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S: serde::Serialize",
    deserialize = "S: serde::Deserialize<'de>"
))]
pub struct SnapshotV2<S: State, R: RoundNum, C: CoordNum> {
    round: R,
    state: Option<Arc<S>>,
    greatest_observed_round_num: Option<R>,
    greatest_observed_coord_num: C,
    participation: Participation<R, C>,
    promises: BTreeMap<R, C>,
    accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S::Frozen: serde::Serialize",
    deserialize = "S::Frozen: serde::Deserialize<'de>"
))]
pub struct SnapshotV3<S: State, R: RoundNum, C: CoordNum> {
    round: R,
    state: Option<Arc<S::Frozen>>,
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
    pub(crate) state: Option<Arc<FrozenOf<S>>>,
    pub(crate) greatest_observed_round_num: Option<R>,
    pub(crate) greatest_observed_coord_num: C,
    pub(crate) participation: Participation<R, C>,
    pub(crate) promises: BTreeMap<R, C>,
    pub(crate) accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
}

impl<S: State, R: RoundNum, C: CoordNum> Snapshot<S, R, C> {
    pub(crate) fn new(
        round: R,
        state: Option<Arc<S::Frozen>>,
        greatest_observed_round_num: Option<R>,
        greatest_observed_coord_num: C,
        participation: Participation<R, C>,
        promises: BTreeMap<R, C>,
        accepted_entries: BTreeMap<R, (C, Arc<LogEntryOf<S>>)>,
    ) -> Self {
        Self(VersionedSnapshot::V3(SnapshotV3 {
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
    pub fn initial_with(state: impl Into<Arc<S::Frozen>>) -> Self {
        Self::initial(Some(state.into()))
    }

    /// Creates an "initial" snapshot, i.e. a snapshot for round `0`.
    pub fn initial_without_state() -> Self {
        Self::initial(None)
    }

    /// Creates a passive snapshot without state.
    ///
    /// Because a state
    pub fn stale_without_state() -> Self {
        Self::initial_without_state().into_stale()
    }

    /// Creates an "initial" snapshot, i.e. a snapshot for round `0`.
    fn initial(state: Option<Arc<S::Frozen>>) -> Self {
        let mut promises = BTreeMap::new();
        promises.insert(Zero::zero(), One::one());

        Self::new(
            Zero::zero(),
            state,
            None,
            One::one(),
            Participation::Active,
            promises,
            BTreeMap::new(),
        )
    }

    pub(crate) fn into_fresh(self) -> Self {
        self.with_participation(Participation::Active)
    }

    /// Returns a stale version of this snapshot, i.e. one which requires
    /// passive participation.
    pub fn into_stale(self) -> Self {
        self.with_participation(Participation::passive())
    }

    fn with_participation(self, participation: Participation<R, C>) -> Self {
        match self.0 {
            VersionedSnapshot::V1(v1) => Self(VersionedSnapshot::V3(SnapshotV3 {
                round: v1.round,
                state: Some(Arc::new(v1.state.freeze())),
                greatest_observed_round_num: v1.greatest_observed_round_num,
                greatest_observed_coord_num: v1.greatest_observed_coord_num,
                participation,
                promises: v1.promises,
                accepted_entries: v1.accepted_entries,
            })),
            VersionedSnapshot::V2(v2) => Self(VersionedSnapshot::V3(SnapshotV3 {
                round: v2.round,
                state: v2.state.as_deref().map(|s| Arc::new(s.freeze())),
                greatest_observed_round_num: v2.greatest_observed_round_num,
                greatest_observed_coord_num: v2.greatest_observed_coord_num,
                participation,
                promises: v2.promises,
                accepted_entries: v2.accepted_entries,
            })),
            VersionedSnapshot::V3(v3) => Self(VersionedSnapshot::V3(SnapshotV3 {
                participation,

                ..v3
            })),
        }
    }

    /// The round the node was in when the snapshot was taken.
    pub fn round(&self) -> R {
        match &self.0 {
            VersionedSnapshot::V1(s) => s.round,
            VersionedSnapshot::V2(s) => s.round,
            VersionedSnapshot::V3(s) => s.round,
        }
    }

    /// The state the node was in when the snapshot was taken.
    pub fn state(&self) -> Option<Arc<S::Frozen>> {
        match &self.0 {
            VersionedSnapshot::V1(s) => Some(Arc::new(s.state.freeze())),
            VersionedSnapshot::V2(s) => s.state.as_ref().map(|s| Arc::new(s.freeze())),
            VersionedSnapshot::V3(s) => s.state.as_ref().map(Arc::clone),
        }
    }

    pub(crate) fn deconstruct(self) -> DeconstructedSnapshot<S, R, C> {
        match self.0 {
            VersionedSnapshot::V1(s) => DeconstructedSnapshot {
                round: s.round,
                state: Some(Arc::new(s.state.freeze())),
                greatest_observed_round_num: s.greatest_observed_round_num,
                greatest_observed_coord_num: s.greatest_observed_coord_num,
                participation: s.participation,
                promises: s.promises,
                accepted_entries: s.accepted_entries,
            },
            VersionedSnapshot::V2(s) => DeconstructedSnapshot {
                round: s.round,
                state: s.state.map(|s| Arc::new(s.freeze())),
                greatest_observed_round_num: s.greatest_observed_round_num,
                greatest_observed_coord_num: s.greatest_observed_coord_num,
                participation: s.participation,
                promises: s.promises,
                accepted_entries: s.accepted_entries,
            },
            VersionedSnapshot::V3(s) => DeconstructedSnapshot {
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
