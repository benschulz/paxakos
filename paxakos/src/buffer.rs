use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

use crate::CoordNum;
use crate::LogEntry;
use crate::RoundNum;

// TODO provide an (optional) persistent implementation
pub trait Buffer: Send + 'static {
    type RoundNum: RoundNum;
    type CoordNum: CoordNum;
    type Entry: LogEntry;
    type Error: std::error::Error + Send + Sync + 'static;

    fn insert(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        entry: Arc<Self::Entry>,
    );

    #[allow(clippy::type_complexity)]
    fn get(
        &self,
        round_num: Self::RoundNum,
    ) -> Result<Option<(Self::CoordNum, Arc<Self::Entry>)>, Self::Error>;
}

pub struct InMemoryBuffer<R, C, E: LogEntry> {
    capacity: usize,
    buffer: VecDeque<(R, C, Arc<E>)>,
}

impl<R, C, E: LogEntry> InMemoryBuffer<R, C, E> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);

        Self {
            capacity,
            buffer: VecDeque::new(),
        }
    }
}

impl<R, C, E> Buffer for InMemoryBuffer<R, C, E>
where
    R: RoundNum,
    C: CoordNum,
    E: LogEntry,
{
    type RoundNum = R;
    type CoordNum = C;
    type Entry = E;
    type Error = Infallible;

    fn insert(
        &mut self,
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        entry: Arc<Self::Entry>,
    ) {
        if self.capacity <= self.buffer.len() {
            self.buffer.pop_front();
        }

        self.buffer.push_back((round_num, coord_num, entry));
    }

    #[allow(clippy::type_complexity)]
    fn get(
        &self,
        round_num: Self::RoundNum,
    ) -> Result<Option<(Self::CoordNum, Arc<Self::Entry>)>, Self::Error> {
        // hot path
        if let Some((r, _, _)) = self.buffer.get(0) {
            if round_num >= *r {
                if let Some(delta) = crate::util::try_usize_delta(round_num, *r) {
                    if let Some((r, c, e)) = self.buffer.get(delta) {
                        if *r == round_num {
                            return Ok(Some((*c, Arc::clone(e))));
                        }
                    }
                }
            }
        }

        // cold path
        Ok(self
            .buffer
            .iter()
            .find(|(r, _, _)| *r == round_num)
            .map(|(_, c, e)| (*c, Arc::clone(e))))
    }
}
