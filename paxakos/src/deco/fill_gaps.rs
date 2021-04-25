use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::task::Poll;
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::StreamExt;

use crate::append::{AppendArgs, AppendError, DoNotRetry, Importance, Peeryness};
use crate::applicable::ApplicableTo;
use crate::node::builder::NodeBuilderWithAll;
use crate::node::{CommitFor, CommunicatorOf, CoordNumOf, LogEntryOf, NodeIdOf};
use crate::node::{NodeStatus, Participation, RoundNumOf, Snapshot, SnapshotFor, StateOf};
use crate::{Node, NodeBuilder, RoundNum};

use super::Decoration;

pub trait FillGapsBuilderExt: NodeBuilder {
    fn fill_gaps<C, P>(self, configure: C) -> NodeBuilderWithAll<FillGaps<Self::Node, P>>
    where
        C: FnOnce(FillGapsBuilderBlank<Self::Node>) -> FillGapsBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<Self::Node> + 'static;

    fn fill_gaps_with<P>(self, entry_producer: P) -> NodeBuilderWithAll<FillGaps<Self::Node, P>>
    where
        P: 'static + Fn() -> LogEntryOf<Self::Node>,
    {
        self.fill_gaps(|b| b.with_entry(entry_producer))
    }
}

pub struct FillGapsBuilder<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N>,
{
    entry_producer: P,
    batch_size: Option<usize>,
    delay: Option<Duration>,
    retry_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

impl<B> FillGapsBuilderExt for B
where
    B: NodeBuilder,
{
    fn fill_gaps<C, P>(self, configure: C) -> NodeBuilderWithAll<FillGaps<Self::Node, P>>
    where
        C: FnOnce(FillGapsBuilderBlank<Self::Node>) -> FillGapsBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<<B as NodeBuilder>::Node> + 'static,
    {
        self.decorated_with(configure(FillGapsBuilderBlank::new()).build())
    }
}

pub struct FillGapsBuilderBlank<N: Node>(std::marker::PhantomData<N>);

impl<N: Node> FillGapsBuilderBlank<N> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn with_entry<P>(self, entry_producer: P) -> FillGapsBuilder<N, P>
    where
        P: Fn() -> LogEntryOf<N>,
    {
        FillGapsBuilder {
            entry_producer,
            batch_size: None,
            delay: None,
            retry_interval: None,

            _node: std::marker::PhantomData,
        }
    }
}

impl<N, P> FillGapsBuilder<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
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

    fn build(self) -> FillGapsArgs<N, P> {
        FillGapsArgs {
            entry_producer: self.entry_producer,
            batch_size: self.batch_size.unwrap_or(1),
            delay: self.delay.unwrap_or_else(|| Duration::from_millis(400)),
            retry_interval: self.retry_interval,

            _node: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct FillGapsArgs<N, P>
where
    N: Node,
    P: 'static + Fn() -> LogEntryOf<N>,
{
    entry_producer: P,
    batch_size: usize,
    delay: Duration,
    retry_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

#[derive(Debug)]
pub struct FillGaps<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    decorated: N,
    arguments: FillGapsArgs<N, P>,

    disoriented: bool,

    known_gaps: HashSet<RoundNumOf<N>>,
    queued_gaps: BinaryHeap<QueuedGap<RoundNumOf<N>>>,

    timer: Option<futures_timer::Delay>,
    time_out_corner: VecDeque<std::time::Instant>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, Option<RoundNumOf<N>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueuedGap<R: RoundNum> {
    round: R,
    due_time: std::time::Instant,
}

impl<R: RoundNum> Ord for QueuedGap<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.due_time
            .cmp(&other.due_time)
            .then_with(|| self.round.cmp(&other.round))
            .reverse()
    }
}

impl<R: RoundNum> PartialOrd for QueuedGap<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<N, P> FillGaps<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    fn fill_gaps(&mut self, cx: &mut std::task::Context<'_>) {
        let now = std::time::Instant::now();

        while self
            .time_out_corner
            .front()
            .filter(|i| **i <= now)
            .is_some()
        {
            self.time_out_corner.pop_front();
        }

        loop {
            while let Some(&QueuedGap { round, .. }) =
                self.queued_gaps.peek().filter(|g| g.due_time <= now)
            {
                if self.appends.len() >= self.arguments.batch_size - self.time_out_corner.len() {
                    break;
                }

                self.queued_gaps.pop();
                self.initiate_append(round);
            }

            let mut none = true;
            while let Poll::Ready(Some(result)) = self.appends.poll_next_unpin(cx) {
                none = false;

                if let (Some(round), Some(retry_interval)) = (result, self.arguments.retry_interval)
                {
                    if self.known_gaps.contains(&round) {
                        self.queued_gaps.push(QueuedGap {
                            round,
                            due_time: now + retry_interval,
                        });
                    }
                }
            }

            if none {
                break;
            }
        }

        let time_a = self.time_out_corner.front().copied();
        let time_b = self
            .queued_gaps
            .peek()
            .filter(|_| self.appends.len() < self.arguments.batch_size - self.time_out_corner.len())
            .map(|queued| queued.due_time);

        let wake_time = time_a
            .map(|a| time_b.map(|b| std::cmp::min(a, b)).unwrap_or(a))
            .or(time_b);

        if let Some(wake_time) = wake_time {
            if wake_time <= now {
                self.timer = None;
                cx.waker().wake_by_ref();
            } else {
                let mut timer = futures_timer::Delay::new(wake_time - now);

                // register with reactor
                if timer.poll_unpin(cx).is_ready() {
                    cx.waker().wake_by_ref();
                } else {
                    self.timer = Some(timer);
                }
            }
        } else {
            self.timer = None;
        }
    }

    fn initiate_append(&mut self, round_num: RoundNumOf<N>) {
        let log_entry = (self.arguments.entry_producer)();

        let append = self
            .decorated
            .append(
                log_entry,
                AppendArgs {
                    round: round_num..=round_num,
                    importance: Importance::MaintainLeadership(Peeryness::Peery),
                    retry_policy: Box::new(DoNotRetry),
                },
            )
            .map(move |res| {
                match res {
                    Ok(_) => None,
                    Err(AppendError::Converged) => None,
                    // TODO this may be too pessimistic
                    // TODO what about Passive?
                    Err(_) => Some(round_num),
                }
            })
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, P> Decoration for FillGaps<N, P>
where
    N: Node + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    type Arguments = FillGapsArgs<N, P>;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            arguments,

            disoriented: false,

            known_gaps: HashSet::new(),
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

impl<N, P> Node for FillGaps<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    type State = StateOf<N>;
    type Communicator = CommunicatorOf<N>;
    type Shutdown = <N as Node>::Shutdown;

    fn id(&self) -> NodeIdOf<Self> {
        self.decorated.id()
    }

    fn status(&self) -> crate::NodeStatus {
        self.decorated.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.decorated.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<crate::Event<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.disoriented = matches!(new_status, NodeStatus::Disoriented);

                    if self.disoriented {
                        self.queued_gaps.clear();
                        self.known_gaps.clear();
                    }
                }

                crate::Event::Install { .. } => {
                    self.queued_gaps.clear();
                    self.known_gaps.clear();
                }

                crate::Event::Gaps(ref gaps) => {
                    let now = std::time::Instant::now();
                    let delay = self.arguments.delay;

                    let new_gaps = gaps
                        .iter()
                        .flat_map(|g| g.rounds.clone())
                        .filter(|r| !self.known_gaps.contains(r))
                        .collect::<Vec<_>>();
                    for gap in &new_gaps {
                        self.queued_gaps.push(QueuedGap {
                            round: *gap,
                            due_time: now + delay,
                        });
                    }

                    self.known_gaps.extend(new_gaps);
                }

                crate::Event::Apply { round, .. } => {
                    self.known_gaps.remove(&round);
                }

                _ => {}
            }
        }

        if !self.disoriented && !self.known_gaps.is_empty() {
            self.fill_gaps(cx);
        }

        event
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
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'static, Result<std::sync::Arc<Self::State>, ()>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<RoundNumOf<Self>>,
    ) -> futures::future::LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}
