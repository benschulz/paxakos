use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::Read;
use std::sync::Arc;

use futures::io::{self, AsyncRead, AsyncReadExt};
use num_traits::Zero;
use serde::{Deserialize, Serialize};

use crate::state::{LogEntryOf, State};
use crate::{CoordNum, RoundNum};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "S: Serialize, S::LogEntry: Serialize",
    deserialize = "S: Deserialize<'de>, S::LogEntry: Deserialize<'de>"
))]
pub struct Snapshot<S: State, R: RoundNum, C: CoordNum> {
    identity: Arc<()>,
    inner: Arc<SnapshotInner<S, R, C>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "S: Serialize, S::LogEntry: Serialize",
    deserialize = "S: Deserialize<'de>, S::LogEntry: Deserialize<'de>"
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "S::LogEntry: Serialize",
    deserialize = "S::LogEntry: Deserialize<'de>"
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
        promises.insert(Zero::zero(), Zero::zero());

        Self::new(
            Zero::zero(),
            state.into(),
            Zero::zero(),
            promises,
            BTreeMap::new(),
        )
    }

    pub async fn from_reader<T: AsyncRead + Unpin>(
        mut read: T,
    ) -> Result<Self, crate::error::BoxError>
    where
        LogEntryOf<S>: serde::de::DeserializeOwned,
    {
        let mut meta_state_len = [0; 64 / 8];
        read.read_exact(&mut meta_state_len).await?;
        let meta_state_len = u64::from_be_bytes(meta_state_len);

        let mut meta_state = vec![0; meta_state_len.try_into().unwrap()];
        read.read_exact(&mut meta_state).await?;

        let meta_state: MetaState<S, R, C> = bincode::deserialize(&meta_state)?;

        let state = <S as State>::from_reader(read).await?;

        Ok(Self {
            identity: Arc::new(()),
            inner: Arc::new(SnapshotInner {
                meta_state,
                state: Arc::new(state),
            }),
        })
    }

    pub fn to_reader(&self) -> SnapshotReader<S>
    where
        LogEntryOf<S>: Serialize,
    {
        // TODO eliminate bincode dependency
        let meta_state = bincode::serialize(&self.inner.meta_state).unwrap();
        let meta_state_len: u64 = meta_state.len().try_into().unwrap();
        let meta_state_len = meta_state_len.to_be_bytes();
        let meta_state_len = std::io::Cursor::new(meta_state_len);
        let meta_state = std::io::Cursor::new(meta_state);
        let state = self.inner.state.to_reader();

        SnapshotReader(meta_state_len.chain(meta_state.chain(state)))
    }

    pub(crate) fn identity(&self) -> &Arc<()> {
        &self.identity
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
                highest_observed_coord_num: s.meta_state.highest_observed_coord_num.clone(),
                promises: s.meta_state.promises.clone(),
                accepted_entries: s.meta_state.accepted_entries.clone(),
            })
    }
}

pub struct SnapshotReader<S: State>(
    std::io::Chain<
        std::io::Cursor<[u8; 64 / 8]>,
        std::io::Chain<std::io::Cursor<Vec<u8>>, <S as State>::Reader>,
    >,
);

impl<S: State> Read for SnapshotReader<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
