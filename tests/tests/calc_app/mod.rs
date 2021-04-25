use std::collections::HashSet;
use std::convert::Infallible;

use async_trait::async_trait;
use futures::io::AsyncRead;
use paxakos::prototyping::PrototypingNode;
use paxakos::{LogEntry, State};
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize)]
#[allow(dead_code)]
pub enum CalcOp {
    Add(f64, Uuid),
    Div(f64, Uuid),
    Mul(f64, Uuid),
    Sub(f64, Uuid),
}

#[async_trait]
impl LogEntry for CalcOp {
    type Id = Uuid;
    type Reader = std::io::Cursor<Vec<u8>>;
    type ReadError = Infallible;

    async fn from_reader<R: AsyncRead + Send + Unpin>(_read: R) -> Result<Self, Self::ReadError> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn to_reader(&self) -> Self::Reader {
        unimplemented!()
    }

    fn id(&self) -> Self::Id {
        match self {
            CalcOp::Add(_, id) | CalcOp::Div(_, id) | CalcOp::Mul(_, id) | CalcOp::Sub(_, id) => {
                *id
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CalcState {
    applied: HashSet<Uuid>,
    value: f64,
    concurrency: std::num::NonZeroUsize,
    nodes: Vec<PrototypingNode>,
    hasher: blake3::Hasher,
}

impl CalcState {
    pub fn new(nodes: Vec<PrototypingNode>, concurrency: usize) -> Self {
        Self {
            applied: HashSet::new(),
            value: 0f64,
            concurrency: std::num::NonZeroUsize::new(concurrency).unwrap(),
            nodes,
            hasher: blake3::Hasher::new(),
        }
    }

    #[allow(dead_code)]
    pub fn checksum(&self) -> blake3::Hash {
        self.hasher.finalize()
    }

    #[allow(dead_code)]
    pub fn value(&self) -> f64 {
        self.value
    }
}

#[async_trait]
impl State for CalcState {
    type Context = ();

    type Reader = std::io::Cursor<Vec<u8>>;
    type ReadError = Infallible;

    type LogEntry = CalcOp;
    type Outcome = f64;
    type Event = (f64, blake3::Hash);

    type Node = PrototypingNode;

    async fn from_reader<R: AsyncRead + Send + Unpin>(_read: R) -> Result<Self, Self::ReadError> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn to_reader(&self) -> Self::Reader {
        unimplemented!()
    }

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut (),
    ) -> (Self::Outcome, Self::Event) {
        let bytes = bincode::serialize(log_entry).unwrap();
        self.hasher.update(&bytes);

        if self.applied.insert(log_entry.id()) {
            match log_entry {
                CalcOp::Add(v, _) => {
                    self.value += v;
                }
                CalcOp::Div(v, _) => {
                    self.value /= v;
                }
                CalcOp::Mul(v, _) => {
                    self.value *= v;
                }
                CalcOp::Sub(v, _) => {
                    self.value -= v;
                }
            }
        }

        (self.value, (self.value, self.hasher.finalize()))
    }

    fn concurrency(&self) -> std::num::NonZeroUsize {
        self.concurrency
    }

    fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.nodes.clone()
    }
}
