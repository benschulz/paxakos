use std::collections::{BTreeSet, BinaryHeap, HashSet, VecDeque};
use std::task::Poll;
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::StreamExt;

use crate::append::{AppendArgs, AppendError, DoNotRetry, Importance, Peeryness};
use crate::communicator::{CoordNumOf, RoundNumOf};
use crate::node::builder::NodeBuilderWithAll;
use crate::node::{Commit, CommunicatorOf, EventFor, NodeStatus, Snapshot, SnapshotFor, StateOf};
use crate::state::LogEntryOf;
use crate::{Node, NodeBuilder, RoundNum};

use super::Decoration;

pub trait AutoFillGapsBuilderExt: NodeBuilder {
    fn fill_gaps<C, F>(self, configure: C) -> NodeBuilderWithAll<AutoFillGaps<Self::Node, F>>
    where
        C: FnOnce(AutoFillGapsBuilderBlank<Self::Node>) -> AutoFillGapsBuilder<Self::Node, F>,
        F: 'static + Fn() -> LogEntryOf<StateOf<Self::Node>>;

    fn fill_gaps_with<F>(self, entry_producer: F) -> NodeBuilderWithAll<AutoFillGaps<Self::Node, F>>
    where
        F: 'static + Fn() -> LogEntryOf<StateOf<Self::Node>>,
    {
        self.fill_gaps(|b| b.with_entry(entry_producer))
    }
}

pub struct AutoFillGapsBuilder<N, F>
where
    N: Node,
    F: Fn() -> LogEntryOf<StateOf<N>>,
{
    entry_producer: F,
    batch_size: Option<usize>,
    delay: Option<Duration>,
    retry_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

impl<B> AutoFillGapsBuilderExt for B
where
    B: NodeBuilder,
{
    fn fill_gaps<C, F>(self, configure: C) -> NodeBuilderWithAll<AutoFillGaps<Self::Node, F>>
    where
        C: FnOnce(AutoFillGapsBuilderBlank<Self::Node>) -> AutoFillGapsBuilder<Self::Node, F>,
        F: 'static + Fn() -> LogEntryOf<StateOf<Self::Node>>,
    {
        self.decorated_with(configure(AutoFillGapsBuilderBlank::new()).build())
    }
}

pub struct AutoFillGapsBuilderBlank<N: Node>(std::marker::PhantomData<N>);

impl<N: Node> AutoFillGapsBuilderBlank<N> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn with_entry<F>(self, entry_producer: F) -> AutoFillGapsBuilder<N, F>
    where
        F: Fn() -> LogEntryOf<StateOf<N>>,
    {
        AutoFillGapsBuilder {
            entry_producer,
            batch_size: None,
            delay: None,
            retry_interval: None,

            _node: std::marker::PhantomData,
        }
    }
}

impl<N, F> AutoFillGapsBuilder<N, F>
where
    N: Node,
    F: 'static + Fn() -> LogEntryOf<StateOf<N>>,
{
    pub fn in_batches_of(self, size: usize) -> Self {
        Self {
            batch_size: Some(size),

            ..self
        }
    }

    pub fn after(self, delay: Duration) -> Self {
        Self {
            delay: Some(delay),

            ..self
        }
    }

    pub fn retry_every<I: Into<Option<Duration>>>(self, interval: I) -> Self {
        Self {
            retry_interval: interval.into(),

            ..self
        }
    }

    fn build(self) -> AutoFillGapsArgs<N, F> {
        AutoFillGapsArgs {
            entry_producer: self.entry_producer,
            batch_size: self.batch_size.unwrap_or(1),
            delay: self.delay.unwrap_or_else(|| Duration::from_millis(400)),
            retry_interval: self.retry_interval,

            _node: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AutoFillGapsArgs<N: Node, F: 'static + Fn() -> LogEntryOf<StateOf<N>>> {
    entry_producer: F,
    batch_size: usize,
    delay: Duration,
    retry_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

#[derive(Debug)]
pub struct AutoFillGaps<N: Node, F: 'static + Fn() -> LogEntryOf<StateOf<N>>> {
    decorated: N,
    arguments: AutoFillGapsArgs<N, F>,

    suspended: bool,

    known_gaps: HashSet<RoundNumOf<CommunicatorOf<N>>>,
    queued_gaps: BinaryHeap<QueuedGap<RoundNumOf<CommunicatorOf<N>>>>,
    fillable_gaps: BTreeSet<RoundNumOf<CommunicatorOf<N>>>,

    timer: Option<futures_timer::Delay>,
    time_out_corner: VecDeque<std::time::Instant>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, bool>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueuedGap<R: RoundNum> {
    round: R,
    due_time: std::time::Instant,
}

impl<R: RoundNum> Ord for QueuedGap<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.round.cmp(&other.round)
    }
}

impl<R: RoundNum> PartialOrd for QueuedGap<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<N, F> AutoFillGaps<N, F>
where
    N: Node,
    F: 'static + Fn() -> LogEntryOf<StateOf<N>>,
{
    fn fill_gaps(&mut self, cx: &mut std::task::Context<'_>) {
        let now = std::time::Instant::now();

        while self
            .queued_gaps
            .peek()
            .filter(|queued| queued.due_time <= now)
            .is_some()
        {
            let dequeued = self.queued_gaps.pop().expect("dequeue gap");

            if self.known_gaps.contains(&dequeued.round) {
                self.fillable_gaps.insert(dequeued.round);
            }
        }

        while self
            .time_out_corner
            .front()
            .filter(|i| **i <= now)
            .is_some()
        {
            self.time_out_corner.pop_front();
        }

        loop {
            self.initiate_appends();

            let mut filled_some = false;

            while let Poll::Ready(Some(success)) = self.appends.poll_next_unpin(cx) {
                if success {
                    filled_some = true;
                } else if let Some(retry_interval) = self.arguments.retry_interval {
                    self.time_out_corner.push_back(now + retry_interval);
                }
            }

            if !filled_some {
                break;
            }
        }

        if let Some(ref mut timer) = self.timer {
            if let Poll::Ready(()) = timer.poll_unpin(cx) {
                self.timer = None;
            }
        }

        if self.timer.is_none()
            && (!self.time_out_corner.is_empty() || !self.queued_gaps.is_empty())
        {
            let time_a = self.time_out_corner.front().copied();
            let time_b = self.queued_gaps.peek().map(|queued| queued.due_time);

            let wake_time = time_a
                .map(|a| time_b.map(|b| std::cmp::min(a, b)).unwrap_or(a))
                .or(time_b)
                .expect("min wake time");

            let mut timer = futures_timer::Delay::new(wake_time - now);

            // register with reactor
            if let Poll::Ready(_) = timer.poll_unpin(cx) {
                cx.waker().wake_by_ref();
            }

            self.timer = Some(timer);
        }
    }

    fn all_being_filled(&self) -> bool {
        self.appends.len() >= self.fillable_gaps.len()
    }

    fn batch_size_exhausted(&self) -> bool {
        self.appends.len() >= self.arguments.batch_size - self.time_out_corner.len()
    }

    fn initiate_appends(&mut self) {
        if !self.all_being_filled() && !self.batch_size_exhausted() {
            let batch_start = *self.fillable_gaps.first().expect("first gap");
            let batch_end = *self
                .fillable_gaps
                .iter()
                .nth(self.arguments.batch_size - 1)
                .unwrap_or_else(|| self.fillable_gaps.last().expect("last gap"));
            let batch = batch_start..=batch_end;

            tracing::debug!("Attempting to fill gaps in rounds `{:?}`.", batch);

            while !self.all_being_filled() && !self.batch_size_exhausted() {
                let log_entry = (self.arguments.entry_producer)();

                let append = self
                    .decorated
                    .append(
                        log_entry,
                        AppendArgs {
                            round: batch.clone(),
                            importance: Importance::MaintainLeadership(Peeryness::Peery),
                            retry_policy: Box::new(DoNotRetry),
                        },
                    )
                    .map(|res| {
                        match res {
                            Ok(_) => true,
                            Err(AppendError::Converged) => true,
                            // TODO this may be too pessimistic
                            // TODO what about Passive?
                            Err(_) => false,
                        }
                    })
                    .boxed_local();

                self.appends.push(append);
            }
        }
    }
}

impl<N, F> Decoration for AutoFillGaps<N, F>
where
    N: 'static + Node,
    F: 'static + Fn() -> LogEntryOf<StateOf<N>>,
{
    type Arguments = AutoFillGapsArgs<N, F>;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            arguments,

            suspended: false,

            known_gaps: HashSet::new(),
            fillable_gaps: BTreeSet::new(),
            queued_gaps: BinaryHeap::new(),

            timer: None,
            time_out_corner: VecDeque::new(),

            appends: futures::stream::FuturesUnordered::new(),
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, F> Node for AutoFillGaps<N, F>
where
    N: Node,
    F: 'static + Fn() -> LogEntryOf<StateOf<N>>,
{
    type State = StateOf<N>;
    type Communicator = CommunicatorOf<N>;
    type Shutdown = <N as Node>::Shutdown;

    fn status(&self) -> crate::NodeStatus {
        self.decorated.status()
    }

    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> Poll<EventFor<Self>> {
        let e = self.decorated.poll_events(cx);

        match e {
            Poll::Ready(crate::Event::Init {
                status: new_status, ..
            })
            | Poll::Ready(crate::Event::StatusChange { new_status, .. }) => {
                self.suspended = matches!(new_status, NodeStatus::Disoriented);

                if new_status == NodeStatus::Following {
                    self.queued_gaps.clear();
                    self.known_gaps.clear();
                    self.fillable_gaps.clear();
                }
            }

            Poll::Ready(crate::Event::Gaps(ref gaps)) => {
                let delay = self.arguments.delay;

                self.queued_gaps = gaps
                    .iter()
                    .flat_map(|g| {
                        g.rounds.clone().map(move |r| QueuedGap {
                            round: r,
                            due_time: g.since + delay,
                        })
                    })
                    .collect();

                self.known_gaps = self.queued_gaps.iter().map(|queued| queued.round).collect();
                self.fillable_gaps.clear();
            }

            Poll::Ready(crate::Event::Apply { round, .. }) => {
                if self.known_gaps.remove(&round) {
                    self.fillable_gaps.remove(&round);
                }
            }

            _ => {}
        }

        if !self.suspended {
            self.fill_gaps(cx);
        }

        e
    }

    fn handle(
        &self,
    ) -> crate::node::NodeHandle<
        Self::State,
        crate::communicator::RoundNumOf<Self::Communicator>,
        crate::communicator::CoordNumOf<Self::Communicator>,
    > {
        self.decorated.handle()
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>
    {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: Snapshot<
            Self::State,
            RoundNumOf<Self::Communicator>,
            CoordNumOf<Self::Communicator>,
        >,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: Snapshot<
            Self::State,
            RoundNumOf<Self::Communicator>,
            CoordNumOf<Self::Communicator>,
        >,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'static, Result<std::sync::Arc<Self::State>, ()>> {
        self.decorated.read_stale()
    }

    fn append(
        &self,
        log_entry: impl Into<std::sync::Arc<crate::state::LogEntryOf<Self::State>>>,
        args: AppendArgs<RoundNumOf<Self::Communicator>>,
    ) -> futures::future::LocalBoxFuture<'static, Result<Commit<Self::State>, AppendError>> {
        self.decorated.append(log_entry, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}
