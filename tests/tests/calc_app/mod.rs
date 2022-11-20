use std::collections::HashSet;
use std::convert::Infallible;

use paxakos::applicable::ApplicableTo;
use paxakos::applicable::Identity;
use paxakos::invocation::Invocation;
use paxakos::prototyping::DirectCommunicatorError;
use paxakos::prototyping::PrototypingNode;
use paxakos::LogEntry;
use paxakos::State;
use uuid::Uuid;

pub struct CalcInvocation;

impl Invocation for CalcInvocation {
    type RoundNum = u64;
    type CoordNum = u32;

    type State = CalcState;

    type Yea = ();
    type Nay = Infallible;
    type Abstain = Infallible;

    type Ejection = Infallible;

    type CommunicationError = DirectCommunicatorError;
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub enum CalcOp {
    Add(f64, Uuid),
    Div(f64, Uuid),
    Mul(f64, Uuid),
    Sub(f64, Uuid),
}

#[derive(Default)]
pub struct PlusZero;

impl ApplicableTo<CalcState> for PlusZero {
    type Projection = Identity;

    fn into_log_entry(self) -> std::sync::Arc<paxakos::state::LogEntryOf<CalcState>> {
        CalcOp::Add(0.0, Uuid::new_v4()).into()
    }
}

impl LogEntry for CalcOp {
    type Id = Uuid;

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

impl State for CalcState {
    type Frozen = Self;

    type Context = ();

    type LogEntry = CalcOp;
    type Outcome = f64;
    type Effect = (f64, blake3::Hash);

    type Error = Infallible;

    type Node = PrototypingNode;

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut Self::Context,
    ) -> Result<(Self::Outcome, Self::Effect), Self::Error> {
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

        Ok((self.value, (self.value, self.hasher.finalize())))
    }

    fn concurrency(this: Option<&Self>) -> Option<std::num::NonZeroUsize> {
        this.map(|s| s.concurrency)
    }

    fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.nodes.clone()
    }

    fn freeze(&self, _context: &mut Self::Context) -> Self::Frozen {
        self.clone()
    }
}
