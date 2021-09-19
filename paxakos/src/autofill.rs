use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::append::Importance;
use crate::append::Peeryness;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::node::builder::NodeBuilder;
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::EventOf;
use crate::node::InvocationOf;
use crate::node::NayOf;
use crate::node::NodeIdOf;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::YeaOf;
use crate::retry::DoNotRetry;
use crate::retry::RetryPolicy;
use crate::util::NumberIter;
use crate::voting::Voter;
use crate::Node;
use crate::RoundNum;

pub trait Config {
    type Node: Node;
    type Applicable: ApplicableTo<StateOf<Self::Node>> + 'static;

    fn init(&mut self, node: &Self::Node, state: &StateOf<Self::Node>);

    fn update(&mut self, event: &EventOf<Self::Node>);

    fn batch_size(&self) -> usize;

    fn delay(&self) -> Duration;

    fn new_filler(&self) -> Self::Applicable;

    fn retry_policy(&self) -> Box<dyn RetryPolicy<Invocation = InvocationOf<Self::Node>> + Send> {
        Box::new(DoNotRetry::new())
    }
}

pub struct StaticConfig<N, A> {
    batch_size: usize,
    delay: Duration,
    _p: std::marker::PhantomData<(N, A)>,
}

impl<N, A> StaticConfig<N, A>
where
    N: Node,
    A: ApplicableTo<StateOf<N>> + Default + 'static,
{
    pub fn new(batch_size: usize, delay: Duration) -> Self {
        Self {
            batch_size,
            delay,
            _p: std::marker::PhantomData,
        }
    }
}

impl<N, A> Config for StaticConfig<N, A>
where
    N: Node,
    A: ApplicableTo<StateOf<N>> + Default + 'static,
{
    type Node = N;
    type Applicable = A;

    fn init(&mut self, _node: &Self::Node, _state: &StateOf<Self::Node>) {}

    fn update(&mut self, _event: &EventOf<Self::Node>) {}

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn delay(&self) -> Duration {
        self.delay
    }

    fn new_filler(&self) -> Self::Applicable {
        Self::Applicable::default()
    }
}

pub trait AutofillBuilderExt: Sized {
    type Node: Node;
    type Voter: Voter;

    fn fill_gaps<C>(self, config: C) -> NodeBuilder<Autofill<Self::Node, C>, Self::Voter>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<N, V> AutofillBuilderExt for NodeBuilder<N, V>
where
    N: Node + 'static,
    V: Voter<
        State = StateOf<N>,
        RoundNum = RoundNumOf<N>,
        CoordNum = CoordNumOf<N>,
        Abstain = AbstainOf<N>,
        Yea = YeaOf<N>,
        Nay = NayOf<N>,
    >,
{
    type Node = N;
    type Voter = V;

    fn fill_gaps<C>(self, config: C) -> NodeBuilder<Autofill<N, C>, V>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

#[derive(Debug)]
pub struct Autofill<N: Node, C> {
    decorated: N,
    config: C,

    disoriented: bool,

    known_gaps: HashSet<RoundNumOf<N>>,
    queued_gaps: BinaryHeap<QueuedGap<RoundNumOf<N>>>,

    timer: Option<futures_timer::Delay>,
    time_out_corner: VecDeque<std::time::Instant>,

    appends: futures::stream::FuturesUnordered<
        LocalBoxFuture<'static, Option<AppendError<InvocationOf<N>>>>,
    >,
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

impl<N, C> Autofill<N, C>
where
    N: Node,
    C: Config<Node = N>,
{
    fn handle_new_status(&mut self, new_status: NodeStatus) {
        self.disoriented = matches!(new_status, NodeStatus::Disoriented);

        if self.disoriented {
            self.queued_gaps.clear();
            self.known_gaps.clear();
        }
    }

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

        let batch_size = self.config.batch_size();

        loop {
            while let Some(&QueuedGap { round, .. }) =
                self.queued_gaps.peek().filter(|g| g.due_time <= now)
            {
                if self.appends.len() >= batch_size - self.time_out_corner.len() {
                    break;
                }

                self.queued_gaps.pop();
                self.initiate_append(round);
            }

            let mut none = true;

            while let Poll::Ready(Some(result)) = self.appends.poll_next_unpin(cx) {
                none = false;

                if let Some(err) = result {
                    match err {
                        AppendError::Converged { caught_up: true } => {
                            // this is the hoped for result for follower nodes
                        }
                        err => {
                            tracing::debug!("Failed to close gap: {}", err);
                        }
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
            .filter(|_| self.appends.len() < batch_size - self.time_out_corner.len())
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
        let log_entry = self.config.new_filler();

        let append = self
            .decorated
            .append(
                log_entry,
                AppendArgs {
                    round: round_num..=round_num,
                    importance: Importance::MaintainLeadership(Peeryness::Peery),
                    retry_policy: self.config.retry_policy(),
                },
            )
            .map(Result::err)
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, C> Decoration for Autofill<N, C>
where
    N: Node,
    C: Config<Node = N> + 'static,
{
    type Arguments = C;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            config: arguments,

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

impl<N, C> Node for Autofill<N, C>
where
    N: Node,
    C: Config<Node = N>,
{
    type Invocation = InvocationOf<N>;
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

    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> Poll<EventFor<Self>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            match event {
                crate::Event::Init { status, state, .. } => {
                    self.handle_new_status(*status);

                    if let Some(state) = state {
                        self.config.init(&self.decorated, &**state);
                    }
                }

                crate::Event::StatusChange { new_status, .. } => {
                    self.handle_new_status(*new_status);
                }

                crate::Event::Install { state, .. } => {
                    self.queued_gaps.clear();
                    self.known_gaps.clear();

                    self.config.init(&self.decorated, &**state);
                }

                crate::Event::Gaps(ref gaps) => {
                    let now = std::time::Instant::now();
                    let delay = self.config.delay();

                    let new_gaps = gaps
                        .iter()
                        .flat_map(|g| NumberIter::from_range(g.rounds.clone()))
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

                crate::Event::Apply { round, result, .. } => {
                    self.known_gaps.remove(round);

                    self.config.update(result);
                }

                _ => {}
            }
        }

        if !self.disoriented && !self.known_gaps.is_empty() {
            self.fill_gaps(cx);
        }

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
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
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<StateOf<Self>> + 'static, P: Into<AppendArgs<Self::Invocation>>>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}
