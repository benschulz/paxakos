mod error;
mod handle;
mod msg;

use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use num_traits::Bounded;
use num_traits::One;
use num_traits::Zero;
use pin_project::pin_project;
use smallvec::SmallVec;
use tracing::debug;
use tracing::info;

use crate::buffer::Buffer;
use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::ReadStaleError;
use crate::event::DirectiveKind;
use crate::event::Event;
use crate::event::ShutdownEvent;
use crate::executor;
use crate::executor::Executor;
use crate::invocation::AbstainOf;
use crate::invocation::ContextOf;
use crate::invocation::CoordNumOf;
use crate::invocation::FrozenStateOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::OutcomeOf;
use crate::invocation::PromiseFor;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::invocation::YeaOf;
use crate::log_entry::LogEntry;
use crate::node::NodeInfo;
use crate::state;
use crate::state::Frozen;
use crate::state::FrozenOf;
use crate::state::State;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::util::NumberIter;
use crate::voting;
use crate::voting::Voter;
use crate::Condition;
use crate::CoordNum;
use crate::Promise;
use crate::RoundNum;

use super::snapshot::DeconstructedSnapshot;
use super::snapshot::Participation;
use super::snapshot::Snapshot;
use super::status::NodeStatus;
use super::SpawnArgs;

use error::AcquireRoundNumError;
use error::ClusterError;
pub use handle::StateKeeperHandle;
use msg::Release;
use msg::Request;
use msg::Response;

type RequestAndResponseSender<I> = (Request<I>, ResponseSender<I>);
type ResponseSender<I> = oneshot::Sender<Response<I>>;
type Awaiter<I> = oneshot::Sender<(RoundNumOf<I>, OutcomeOf<I>)>;

// this should become unnecessary with stabilization of `!`
macro_rules! assert_unreachable {
    ($v:expr) => {{
        h($v);

        fn h(_: std::convert::Infallible) -> ! {
            unreachable!()
        }
    }};
}

enum AcceptPolicy<I: Invocation> {
    Irrejectable,
    Rejectable(Option<NodeOf<I>>),
}

type SpawnResult<I> = (NodeStatus, super::Participation<RoundNumOf<I>>);
type RoundNumRequest<I> = (RangeInclusive<RoundNumOf<I>>, ResponseSender<I>);
type AcceptedEntry<I> = (CoordNumOf<I>, Arc<LogEntryOf<I>>);
type PendingCommit<I> = (instant::Instant, CoordNumOf<I>, Arc<LogEntryOf<I>>);

struct Init<S: State, R: RoundNum, C: CoordNum> {
    state_round: R,
    state: Option<Arc<FrozenOf<S>>>,
    greatest_observed_round_num: Option<R>,
    greatest_observed_coord_num: C,
    participation: Participation<R, C>,
    promises: BTreeMap<R, C>,
    accepted_entries: BTreeMap<R, (C, Arc<state::LogEntryOf<S>>)>,
}

impl<S: State, R: RoundNum, C: CoordNum> Default for Init<S, R, C> {
    fn default() -> Self {
        Self {
            state_round: Zero::zero(),
            state: None,
            greatest_observed_round_num: None,
            greatest_observed_coord_num: One::one(),
            participation: Participation::Active,
            promises: {
                let mut promises = BTreeMap::new();
                promises.insert(Zero::zero(), One::one());
                promises
            },
            accepted_entries: BTreeMap::new(),
        }
    }
}

impl<S: State, R: RoundNum, C: CoordNum> From<DeconstructedSnapshot<S, R, C>> for Init<S, R, C> {
    fn from(s: DeconstructedSnapshot<S, R, C>) -> Self {
        Self {
            state_round: s.round,
            state: s.state,
            greatest_observed_round_num: s.greatest_observed_round_num,
            greatest_observed_coord_num: s.greatest_observed_coord_num,
            participation: s.participation,
            promises: s.promises,
            accepted_entries: s.accepted_entries,
        }
    }
}

pub struct StateKeeperKit<I: Invocation> {
    handle: StateKeeperHandle<I>,
    receiver: mpsc::Receiver<RequestAndResponseSender<I>>,
}

impl<I: Invocation> StateKeeperKit<I> {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(32);

        Self {
            handle: StateKeeperHandle::new(sender),
            receiver,
        }
    }

    pub fn handle(&self) -> StateKeeperHandle<I> {
        self.handle.clone()
    }
}

pub struct StateKeeper<I, V, B>
where
    I: Invocation,
    V: Voter<
        State = StateOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
    B: Buffer<RoundNum = RoundNumOf<I>, CoordNum = CoordNumOf<I>, Entry = LogEntryOf<I>>,
{
    context: ContextOf<I>,

    node_id: NodeIdOf<I>,
    voter: V,

    receiver: mpsc::Receiver<RequestAndResponseSender<I>>,

    release_sender: mpsc::UnboundedSender<Release<RoundNumOf<I>>>,
    release_receiver: mpsc::UnboundedReceiver<Release<RoundNumOf<I>>>,

    /// The greatest _observed_ round number so far.
    ///
    /// This number is used to determine whether this node is lagging.
    greatest_observed_round_num: Option<RoundNumOf<I>>,

    /// The greatest _observed_  coordination number so far.
    ///
    /// Naively it might make sense to track these in a BTreeMap, similar to
    /// promises. However, the greatest observed coordination number is only
    /// used for one thing: To determine the which coordination number to use in
    /// a bid for leader. This coordination number will then be compared against
    /// the _strongest_ promise (see prepare_entry). Therefore it makes no sense
    /// to track observations by round number.
    greatest_observed_coord_num: CoordNumOf<I>,

    /// If promises were made, promises will be kept.
    ///
    /// Promises are at the heart of Paxos and _essential_ to correctness.
    /// Whenever a promise is made, an enry `(r1, c1)` is added, to enable us to
    /// keep the promise. Going forward
    ///  - no additional promises `(r2, c2)` with `r2 >= r1` and `c2 <= c1` are
    ///    made and
    ///  - any proposals `(r2, c2, e)` with `r2 >= r1` and `c2 < c1` are
    ///    rejected.
    ///
    /// It should be noted that previous are _not_ bound by the promise.
    ///
    /// When the [concurrency level] is greater than one, a promise `(r1, c1)`
    /// may be made _after_ another promise `(r2, c2)` despite `r1 < r2`. It may
    /// even happen that `r1 < r2` _and_ `c1 > c2`. The two rules above must
    /// still hold, i.e. `(r1, c1)` must override `(r2, c2)`, including all
    /// rounds `r >= r2`. To allow efficient lookups, the following invariant is
    /// imposed on entries `(r1, c1)`, `(r2, c2)` of this map.
    ///
    ///  - `r2 >= r1 => c2 >= c1`
    ///
    /// For space efficiency, we also allow ourselves to override "obsolete"
    /// promises, i.e. promises for rounds that have already converged on a
    /// value. This is done by looking up the promise `c` that is in effect for
    /// the first unconverged round, inserting an entry `(0, c)` and then
    /// enforcing the invariant above.
    ///
    /// [concurrency]: crate::state::State::concurrency
    promises: BTreeMap<RoundNumOf<I>, CoordNumOf<I>>,
    accepted_entries: BTreeMap<RoundNumOf<I>, AcceptedEntry<I>>,
    leadership: (RoundNumOf<I>, CoordNumOf<I>),

    /// Round number of the last applied log entry, initially `Zero::zero()`.
    state_round: RoundNumOf<I>,
    /// Current state or `None` iff the node is `Disoriented`.
    state: Option<StateOf<I>>,

    concurrency_bound: RoundNumOf<I>,
    round_num_requests: VecDeque<RoundNumRequest<I>>,
    available_round_nums: BTreeSet<RoundNumOf<I>>,
    acquired_round_nums: HashSet<RoundNumOf<I>>,

    pending_commits: BTreeMap<RoundNumOf<I>, PendingCommit<I>>,
    awaiters: HashMap<LogEntryIdOf<I>, Vec<Awaiter<I>>>,

    participation: Participation<RoundNumOf<I>, CoordNumOf<I>>,
    /// last status that was observable from the outside
    status: NodeStatus,

    queued_events: VecDeque<ShutdownEvent<I>>,
    event_emitter: mpsc::Sender<ShutdownEvent<I>>,
    warn_counter: u16,

    applied_entry_buffer: B,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<I>>>,
}

impl<I, V, B> StateKeeper<I, V, B>
where
    I: Invocation,
    V: Voter<
        State = StateOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
    B: Buffer<RoundNum = RoundNumOf<I>, CoordNum = CoordNumOf<I>, Entry = LogEntryOf<I>>,
{
    pub(crate) async fn spawn<E: Executor>(
        kit: StateKeeperKit<I>,
        args: SpawnArgs<I, V, B, E>,
    ) -> Result<
        (
            NodeStatus,
            super::Participation<RoundNumOf<I>>,
            EventStream<I>,
            ProofOfLife,
        ),
        executor::ErrorOf<E>,
    > {
        let (evt_send, evt_recv) = mpsc::channel(32);

        let proof_of_life = ProofOfLife::new();

        let (send, recv) = oneshot::channel();

        Self::start_and_run_new(args, send, kit.receiver, evt_send)?;

        let (initial_status, initial_participation) =
            recv.await.expect("StateKeeper failed to start");

        Ok((
            initial_status,
            initial_participation,
            EventStream { delegate: evt_recv },
            proof_of_life,
        ))
    }

    fn start_and_run_new<E: Executor>(
        spawn_args: SpawnArgs<I, V, B, E>,
        start_result_sender: oneshot::Sender<SpawnResult<I>>,
        receiver: mpsc::Receiver<RequestAndResponseSender<I>>,
        event_emitter: mpsc::Sender<ShutdownEvent<I>>,
    ) -> Result<(), executor::ErrorOf<E>> {
        spawn_args.executor.execute(async move {
            let context = spawn_args.context;
            let node_id = spawn_args.node_id;
            let voter = spawn_args.voter;
            let snapshot = spawn_args.snapshot;
            let applied_entry_buffer = spawn_args.buffer;
            #[cfg(feature = "tracer")]
            let tracer = spawn_args.tracer;

            // assume we're lagging
            let initial_status = snapshot
                .state()
                .map(|_| NodeStatus::Lagging)
                .unwrap_or(NodeStatus::Disoriented);

            let Init {
                state_round,
                state,
                greatest_observed_round_num,
                greatest_observed_coord_num,
                participation,
                promises,
                accepted_entries,
            } = Init::from(snapshot.deconstruct());

            let _ = start_result_sender
                .send((initial_status, super::Participation::from(&participation)));

            let (rel_send, rel_recv) = mpsc::unbounded();

            let state_keeper = StateKeeper {
                context,

                node_id,
                voter,

                receiver,

                release_sender: rel_send,
                release_receiver: rel_recv,

                promises,
                accepted_entries,
                leadership: (Zero::zero(), Zero::zero()),

                state_round,
                state: state.as_deref().map(|s| s.thaw()),

                concurrency_bound: Zero::zero(),
                round_num_requests: VecDeque::new(),
                available_round_nums: BTreeSet::new(),
                acquired_round_nums: HashSet::new(),

                greatest_observed_round_num,
                greatest_observed_coord_num,

                pending_commits: BTreeMap::new(),
                awaiters: HashMap::new(),

                participation,
                status: initial_status,

                queued_events: VecDeque::new(),
                event_emitter,
                warn_counter: 0,

                applied_entry_buffer,

                #[cfg(feature = "tracer")]
                tracer,
            };

            state_keeper.init_and_run(state).await;
        })
    }

    async fn init_and_run(mut self, state: Option<Arc<FrozenStateOf<I>>>) {
        self.emit(Event::Init {
            status: self.status,
            round: self.state_round,
            state,
        });

        self.run().await;
    }

    async fn run(mut self) {
        while self.await_and_handle_next_request().await {
            self.apply_commits();
            self.release_round_nums();
            self.hand_out_round_nums();
            self.detect_and_emit_status_change();

            if !self.flush_event_queue().await {
                tracing::warn!("Node was not properly shut down but simply dropped.");
                return;
            }
        }

        tracing::info!("Shutting down.");
        self.shut_down().await
    }

    async fn await_and_handle_next_request(&mut self) -> bool {
        let next_request = self.receiver.next().await;

        match next_request {
            Some((req, resp_sender)) => {
                if let Request::Shutdown = req {
                    let _ = resp_sender.send(Response::Shutdown(Ok(())));

                    false
                } else {
                    self.handle_request_msg(req, resp_sender);
                    true
                }
            }
            None => false,
        }
    }

    fn release_round_nums(&mut self) {
        loop {
            match self.release_receiver.try_next() {
                Ok(Some(req)) => match req {
                    Release::RoundNum(n) => {
                        self.acquired_round_nums.remove(&n);

                        if !self.is_round_num_unavailable(n) {
                            self.available_round_nums.insert(n);
                        }
                    }
                },
                Ok(None) => unreachable!(),
                Err(_) => return,
            }
        }
    }

    fn is_round_num_unavailable(&self, round_num: RoundNumOf<I>) -> bool {
        round_num <= self.state_round
            || round_num > self.concurrency_bound
            || self.acquired_round_nums.contains(&round_num)
            || self.pending_commits.contains_key(&round_num)
    }

    fn hand_out_round_nums(&mut self) {
        let round = self.state_round;

        let requests = &mut self.round_num_requests;
        let availables = &mut self.available_round_nums;
        let acquireds = &mut self.acquired_round_nums;
        let sender = &self.release_sender;

        match self.state.as_ref() {
            Some(state) => {
                let concurrency = into_round_num(crate::state::concurrency_of(state));
                let concurrency_bound = round + concurrency;

                if self.concurrency_bound < concurrency_bound {
                    for r in NumberIter::from_range(
                        (self.concurrency_bound + One::one())..=concurrency_bound,
                    ) {
                        availables.insert(r);
                    }

                    self.concurrency_bound = concurrency_bound;
                }

                let mut ix = 0;

                while let Some(next_available) = availables.iter().next() && ix < requests.len() {
                    let (range, _) = &requests[ix];

                    // Have rounds up to and including `range.end()` have already converged?
                    // If so, we'll never be able to provide a reservation.
                    if range.end() < next_available {
                        let (_, send) = requests.remove(ix).unwrap();
                        let _ = send.send(Response::AcquireRoundNum(Err(
                            AcquireRoundNumError::Converged,
                        )));
                        continue;
                    }

                    if let Some(round_num) = availables.range(range.clone()).next().copied() {
                        acquireds.insert(round_num);

                        let (_, send) = requests.remove(ix).unwrap();
                        let _ = send.send(Response::AcquireRoundNum(Ok(RoundNumReservation {
                            round_num,
                            sender: sender.clone(),
                        })));

                        availables.remove(&round_num);
                        continue;
                    }

                    ix += 1;
                }
            }
            None => {
                while let Some((_, send)) = requests.pop_front() {
                    let _ = send.send(Response::AcquireRoundNum(Err(
                        AcquireRoundNumError::Disoriented,
                    )));
                }
            }
        }
    }

    async fn flush_event_queue(&mut self) -> bool {
        while let Some(event) = self.queued_events.pop_front() {
            let mut timeout = 1;

            let mut send = self.event_emitter.send(event);

            loop {
                timeout *= 10;

                let timeout = std::time::Duration::from_millis(timeout);
                let delay = futures_timer::Delay::new(timeout);

                match futures::future::select(send, delay).await {
                    futures::future::Either::Left((Ok(_), _)) => break,
                    futures::future::Either::Left((Err(_), _)) => return false,

                    futures::future::Either::Right((_, s)) => {
                        self.warn_counter += 1;

                        const LIMIT: u16 = 10;

                        if self.warn_counter <= LIMIT
                            || std::time::Duration::from_secs(60) < timeout
                        {
                            tracing::warn!(
                                "`{}` events have been queued for over `{:?}`.",
                                self.queued_events.len() + 1,
                                timeout
                            );
                        }

                        if self.warn_counter == LIMIT {
                            tracing::warn!(
                                "Limit of `{}` warnings reached, further warnings will be suppressed.",
                                LIMIT
                            );
                        }

                        send = s;
                    }
                }
            }
        }

        true
    }

    async fn shut_down(mut self) {
        let snapshot = self.prepare_snapshot();
        let mut emitter = self.event_emitter;

        let _ = emitter.send(ShutdownEvent::Final { snapshot }).await;
    }

    fn handle_request_msg(&mut self, req: Request<I>, resp_sender: oneshot::Sender<Response<I>>) {
        let resp = match req {
            Request::PrepareSnapshot => Response::PrepareSnapshot(Ok(self.prepare_snapshot())),

            Request::AffirmSnapshot { snapshot } => {
                Response::AffirmSnapshot(self.affirm_snapshot(snapshot))
            }

            Request::InstallSnapshot { snapshot } => {
                Response::InstallSnapshot(self.install_snapshot(snapshot))
            }

            Request::ReadStale { f } => Response::ReadStale(match &self.state {
                Some(s) => Ok(f.0(s)),
                None => Err(ReadStaleError::Disoriented),
            }),

            Request::AwaitCommitOf { entry_id } => {
                let (s, r) = oneshot::channel();

                self.awaiters.entry(entry_id).or_default().push(s);

                Response::AwaitCommitOf(Ok(r))
            }

            Request::AcquireRoundNum { range } => {
                return self.round_num_requests.push_back((range, resp_sender));
            }

            Request::AcceptedEntryOf { round_num } => Response::AcceptedEntryOf(Ok(self
                .accepted_entries
                .get(&round_num)
                .map(|(_, e)| Arc::clone(e)))),

            Request::Cluster { round_num } => Response::Cluster(
                self.state
                    .as_ref()
                    .ok_or(ClusterError::Disoriented)
                    .and_then(|state| {
                        if round_num <= self.state_round {
                            Err(ClusterError::Converged(round_num))
                        } else {
                            // unwrap must succeed because `!(round_num <= self.state_round)`
                            let offset = crate::util::usize_delta(round_num, self.state_round);
                            let offset = std::num::NonZeroUsize::new(offset).unwrap();

                            let bound = crate::state::concurrency_of(state);

                            // We don't give out reservations past the concurrency bound.
                            assert!(offset <= bound);

                            Ok(state.cluster_at(offset))
                        }
                    }),
            ),

            Request::ObservedCoordNum { coord_num } => {
                self.observe_coord_num(coord_num);

                Response::ObservedCoordNum(Ok(()))
            }
            Request::GreatestObservedCoordNum => {
                Response::GreatestObservedCoordNum(Ok(self.greatest_observed_coord_num))
            }

            Request::PrepareEntry {
                round_num,
                coord_num,
            } => {
                self.observe_round_num(round_num);
                self.observe_coord_num(coord_num);

                let result = self.prepare_entry(round_num, coord_num);

                if result.is_ok() {
                    self.emit_directive(DirectiveKind::Prepare, round_num, coord_num);
                }

                Response::PrepareEntry(result)
            }

            Request::AcceptEntry {
                round_num,
                coord_num,
                entry,
            } => {
                self.observe_round_num(round_num);
                self.observe_coord_num(coord_num);

                let result = self.accept_entry(round_num, coord_num, entry);

                if result.is_ok() {
                    self.emit_directive(DirectiveKind::Accept, round_num, coord_num);
                }

                Response::AcceptEntry(result)
            }
            Request::AcceptEntries {
                coord_num,
                mut entries,
            } => {
                entries.sort_unstable_by_key(|(r, _)| *r);

                self.observe_round_num(entries[entries.len() - 1].0);
                self.observe_coord_num(coord_num);

                // No need to emit multiple events. If there are entries for whose round an
                // event would be emitted, then this round is among them.
                let round_num = entries
                    .iter()
                    .map(|(r, _)| *r)
                    .find(|r| *r > self.state_round);

                let leader = self.deduce_node(entries[0].0, coord_num);
                if let Some(leader) = &leader {
                    assert_eq!(leader.id(), self.node_id);
                }

                let result = self.accept_entries(coord_num, entries, AcceptPolicy::Irrejectable);

                if let Some(round_num) = round_num {
                    if result.is_ok() {
                        self.emit_directive(DirectiveKind::Accept, round_num, coord_num);
                    }
                }

                Response::AcceptEntries(result.map(|_| ()))
            }

            Request::CommitEntry {
                round_num,
                coord_num,
                entry,
            } => {
                self.observe_round_num(round_num);
                self.observe_coord_num(coord_num);

                let result = self.commit_entry(round_num, coord_num, entry);

                if result.is_ok() {
                    self.emit_directive(DirectiveKind::Commit, round_num, coord_num);
                }

                Response::CommitEntry(result)
            }

            Request::CommitEntryById {
                round_num,
                coord_num,
                entry_id,
            } => {
                self.observe_round_num(round_num);
                self.observe_coord_num(coord_num);

                // We _should_ only get a request to commit _by id_ if we accepted the entry.
                // It's unlikely that we would accept another entry between then and the commit.
                // That means we can just look the entry up by the round number.
                match self.accepted_entries.get(&round_num).cloned() {
                    Some((_, e)) if e.id() == entry_id => {
                        let result = self.commit_entry(round_num, coord_num, e);

                        if result.is_ok() {
                            self.emit_directive(DirectiveKind::Commit, round_num, coord_num);
                        }

                        Response::CommitEntryById(result)
                    }
                    _ => Response::CommitEntryById(Err(CommitError::InvalidEntryId(entry_id))),
                }
            }

            Request::AssumeLeadership {
                round_num,
                coord_num,
            } => {
                self.leadership = (round_num, coord_num);

                Response::AssumeLeadership(Ok(()))
            }

            Request::ForceActive => Response::ForceActive({
                if let Participation::Passive { .. } = self.participation {
                    info!("Node forced into active participation mode.");

                    self.participation = Participation::Active;

                    Ok(true)
                } else {
                    Ok(false)
                }
            }),

            Request::Shutdown => unreachable!(),
        };

        let _ = resp_sender.send(resp);
    }

    fn passive_for(&self, round_num: RoundNumOf<I>) -> bool {
        match self.participation {
            Participation::Active => false,
            Participation::PartiallyActive(r) => round_num < r,
            Participation::Passive { .. } => true,
        }
    }

    fn detect_and_emit_status_change(&mut self) {
        let expected_status = match &self.state {
            Some(state) => {
                if self
                    .greatest_observed_round_num
                    .unwrap_or_else(Bounded::max_value)
                    > self.state_round + into_round_num(crate::state::concurrency_of(state))
                {
                    NodeStatus::Lagging
                } else if self.greatest_observed_coord_num == self.leadership.1 {
                    NodeStatus::Leading
                } else {
                    NodeStatus::Following
                }
            }
            None => NodeStatus::Disoriented,
        };

        if self.status != expected_status {
            self.emit_status_change(expected_status);
        }
    }

    fn emit_status_change(&mut self, new_status: NodeStatus) {
        let old_status = self.status;
        self.status = new_status;

        self.emit(Event::StatusChange {
            old_status,
            new_status,
        });
    }

    fn emit_directive(
        &mut self,
        kind: DirectiveKind,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) {
        if let Some(leader) = self.deduce_node(round_num, coord_num) {
            self.emit(Event::Directive {
                kind,
                leader,
                round_num,
                coord_num,
                timestamp: instant::Instant::now(),
            });
        }
    }

    fn emit_gaps(&mut self, mut last_round: RoundNumOf<I>) {
        let mut gaps = Vec::new();

        for (&k, v) in self.pending_commits.iter() {
            if k > last_round + One::one() {
                gaps.push(crate::event::Gap {
                    since: v.0,
                    rounds: (last_round + One::one())..k,
                });
            }

            last_round = k;
        }

        self.emit(Event::Gaps(gaps));
    }

    fn emit(&mut self, event: impl Into<ShutdownEvent<I>> + std::fmt::Debug) {
        tracing::trace!("Emitting event {:?}.", event);

        let event = event.into();

        match self.event_emitter.try_send(event) {
            Ok(_) => {}
            Err(err) => {
                self.queued_events.push_back(err.into_inner());
            }
        }
    }

    fn deduce_node(&self, round_num: RoundNumOf<I>, coord_num: CoordNumOf<I>) -> Option<NodeOf<I>> {
        self.state.as_ref().and_then(|state| {
            if round_num > self.state_round
                && round_num
                    <= self.state_round + into_round_num(crate::state::concurrency_of(state))
            {
                // unwrap must succeed because `round_num > self.state_round`
                let round_offset = crate::util::usize_delta(round_num, self.state_round);
                let round_offset = std::num::NonZeroUsize::new(round_offset).unwrap();

                let cluster = state.cluster_at(round_offset);
                let cluster_size = crate::util::from_usize(cluster.len(), "cluster size");
                let leader_ix = crate::util::usize_remainder(coord_num, cluster_size);

                let leader = cluster.into_iter().nth(leader_ix).unwrap();

                Some(leader)
            } else {
                None
            }
        })
    }

    fn observe_round_num(&mut self, round_num: RoundNumOf<I>) {
        self.greatest_observed_round_num = self
            .greatest_observed_round_num
            .map(|r| std::cmp::max(r, round_num))
            .or(Some(round_num));
    }

    fn observe_coord_num(&mut self, coord_num: CoordNumOf<I>) {
        self.greatest_observed_coord_num =
            std::cmp::max(self.greatest_observed_coord_num, coord_num);
    }

    fn prepare_snapshot(&mut self) -> SnapshotFor<I> {
        Snapshot::new(
            self.state_round,
            self.state.as_ref().map(|s| Arc::new(s.freeze())),
            self.greatest_observed_round_num,
            self.greatest_observed_coord_num,
            self.participation.clone(),
            self.promises.clone(),
            self.accepted_entries.clone(),
        )
    }

    // TODO this doesn't do anything and probably never will, consider removal
    fn affirm_snapshot(&mut self, _snapshot: SnapshotFor<I>) -> Result<(), AffirmSnapshotError> {
        Ok(())
    }

    fn install_snapshot(&mut self, snapshot: SnapshotFor<I>) -> Result<(), InstallSnapshotError> {
        if self.state_round > snapshot.round() {
            return Err(InstallSnapshotError::Outdated);
        }

        let DeconstructedSnapshot {
            round: state_round,
            state,
            greatest_observed_round_num,
            greatest_observed_coord_num,
            accepted_entries,
            ..
        } = snapshot.deconstruct();

        self.state_round = state_round;
        self.state = state.as_deref().map(|s| s.thaw());
        self.greatest_observed_round_num = greatest_observed_round_num;
        self.greatest_observed_coord_num = greatest_observed_coord_num;
        self.accepted_entries = accepted_entries;

        self.emit(Event::Install {
            round: state_round,
            state,
        });

        let extraneous_commits: Vec<_> = self
            .pending_commits
            .range(..=state_round)
            .map(|(k, _)| *k)
            .collect();
        for k in extraneous_commits {
            self.pending_commits.remove(&k);
        }

        Ok(())
    }

    fn prepare_entry(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) -> Result<PromiseFor<I>, PrepareError<I>> {
        if self.passive_for(round_num) {
            debug!("In passive mode, rejecting prepare request.");
            return Err(PrepareError::Passive);
        }

        if round_num <= self.state_round {
            return Err(PrepareError::Converged(
                self.greatest_observed_coord_num,
                self.try_get_applied_entry(round_num),
            ));
        } else if let btree_map::Entry::Occupied(e) = self.pending_commits.entry(round_num) {
            let coord_num = e.get().1;
            let log_entry = Arc::clone(&e.get().2);

            return Err(PrepareError::Converged(
                self.greatest_observed_coord_num,
                Some((coord_num, log_entry)),
            ));
        }

        // We always compare against the strongest promise/greatest coordination number.
        // This is necessary to prevent inconsistencies caused by occurences like the
        // following in a three node cluster.
        //
        //  1. A is leader, the next unconverged round is `r0`.
        //  2. B asks for promise from B, C for round `r1` > `r0`.
        //  3. A accepts its own entry `e1` for `r1`
        //  3. B promises, requires nothing.
        //  3. C promises, requires nothing.
        //  3. B asks for promise from B, A for round `r0`.
        //  3. B accepts its own entry `e2` for `r1`.
        //  3. B sends `e2` to C.
        //  4. C accepts entry `e2` for `r1`, `r1` has converged on `e2`
        //  4. B promises, requires nothing.
        //  4. A promises, requires `e1` for `r1`, B implicitly accepts, `r1` has
        //     diverged.
        //
        // It's theoretically possible to prevent this by limiting promises. B should
        // not have promised unconditionally. It should have limited its promise to
        // rounds less than `r1`, its previous promise. However, this would require
        // changes to the protocol and incurr an overhead on all promises.
        //
        // What we give up here is the ability for a node to become leader for rounds
        // `r1..r2` while another node _remains_ leader for rounds `r2..`.
        let strongest_promise = *self.promises.values().rev().next().expect("last promise");

        // Paxos requires that we reject requests whose coordination number is less than
        // or equal to the one of the previously accepted proposal. Because of how we
        // determine coordination numbers, equal numbers imply the same proposer. That
        // alone is not enough to weaken this constraint to just "less than" because a
        // proposer may be inconsistent with itself.
        //
        // Consider the following scenario with two concurrently running appends in a
        // three node cluster.
        //
        //  1. (1) A asks for promise from A, B.
        //  1. (2) A asks for promise from A, C.
        //  2. (1) A promises, requires nothing.
        //  2. (2) A promises, requires nothing.
        //  2. (1) B promises, requires R and A implicitly accepts.
        //  3. (2) C promises, requires nothing.
        //  4. (2) A has no requirements, overwrites implicit accept of R.
        //
        // However, such scenarios have been ruled out by limiting nodes to one election
        // run at a time (see NodeInner::ensure_leadership). Building on top of those
        // two (intentional) implementation details we get to weaken "less than" to
        // "less" here.
        //
        // We exploit this weakened constraint in a very subtle way to preserve
        // liveness:
        //
        // It is possible for a node to become leader without learning that fact due to
        // a communication error. It will reuse the same coordination number in
        // subsequent attempts to become leader/learn that it is leader. With a stronger
        // constraint here, these attempts would fail. That would prevent the node from
        // making progress and thereby break liveness.
        if coord_num < strongest_promise {
            Err(PrepareError::Supplanted(strongest_promise))
        } else {
            let voting_decision = self.voter.contemplate_candidate(
                round_num,
                coord_num,
                self.deduce_node(round_num, coord_num).as_ref(),
                self.state.as_ref(),
            );

            match voting_decision {
                voting::Decision::Abstain(reason) => Err(PrepareError::Abstained(reason)),
                voting::Decision::Nay(never) => assert_unreachable!(never),
                voting::Decision::Yea(_) => {
                    overriding_insert(&mut self.promises, round_num, coord_num);

                    let promise = self
                        .accepted_entries
                        .range(round_num..)
                        .map(|(r, (c, e))| Condition::from((*r, *c, Arc::clone(e))))
                        .collect::<Vec<Condition<_, _, _>>>();

                    #[cfg(feature = "tracer")]
                    {
                        if let Some(tracer) = self.tracer.as_mut() {
                            tracer.record_promise(
                                round_num,
                                coord_num,
                                promise
                                    .iter()
                                    .map(|c| (c.round_num, c.coord_num, c.log_entry.id()))
                                    .collect(),
                            );
                        }
                    }

                    Ok(Promise(promise))
                }
            }
        }
    }

    fn try_get_applied_entry(
        &mut self,
        round_num: RoundNumOf<I>,
    ) -> Option<(CoordNumOf<I>, Arc<LogEntryOf<I>>)> {
        match self.applied_entry_buffer.get(round_num) {
            Ok(entry) => entry,
            Err(err) => {
                // TODO suppress after too many failures
                tracing::warn!("Failed to get applied entry from buffer: {}", err);
                None
            }
        }
    }

    fn accept_entry(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    ) -> Result<YeaOf<I>, AcceptError<I>> {
        if round_num <= self.state_round {
            Err(AcceptError::Converged(
                self.greatest_observed_coord_num,
                self.try_get_applied_entry(round_num),
            ))
        } else if let btree_map::Entry::Occupied(e) = self.pending_commits.entry(round_num) {
            let coord_num = e.get().1;
            let log_entry = Arc::clone(&e.get().2);

            Err(AcceptError::Converged(
                self.greatest_observed_coord_num,
                Some((coord_num, log_entry)),
            ))
        } else {
            let relevant_promise =
                overridable_lookup(&self.promises, round_num, "relevant promise");

            if relevant_promise > coord_num {
                Err(AcceptError::Supplanted(relevant_promise))
            } else {
                self.accept_entries(
                    coord_num,
                    vec![(round_num, entry)],
                    AcceptPolicy::Rejectable(self.deduce_node(round_num, coord_num)),
                )
                .map(|ys| {
                    assert!(ys.len() == 1);

                    ys.into_iter().next().unwrap()
                })
            }
        }
    }

    fn accept_entries(
        &mut self,
        coord_num: CoordNumOf<I>,
        entries: Vec<(RoundNumOf<I>, Arc<LogEntryOf<I>>)>,
        policy: AcceptPolicy<I>,
    ) -> Result<SmallVec<[YeaOf<I>; 1]>, AcceptError<I>> {
        let first_round = entries[0].0;

        if let Participation::Passive {
            observed_proposals, ..
        } = &mut self.participation
        {
            for r in entries.iter().map(|(r, _)| *r) {
                observed_proposals.insert((r, coord_num));
            }
        }

        if self.passive_for(first_round) {
            debug!("In passive mode, rejecting accept request.");
            return Err(AcceptError::Passive);
        }

        let round_range_start = self.state_round + One::one();
        let round_range_end = self
            .promises
            .iter()
            .rev()
            .filter(|(_, p)| **p > coord_num)
            .map(|(r, _)| *r)
            .last();

        let round_range = round_range_start
            ..=(round_range_end
                // TODO is the max here necessary? maybe add an assertion of r >= 1
                .map(|r| std::cmp::max(r, One::one()) - One::one())
                .unwrap_or_else(Bounded::max_value));

        let acceptable_entries = entries.into_iter().filter(|(r, _)| round_range.contains(r));

        let mut yeas = SmallVec::new();

        for (round_num, log_entry) in acceptable_entries {
            if let AcceptPolicy::Rejectable(leader) = &policy {
                match self.voter.contemplate_proposal(
                    round_num,
                    coord_num,
                    &*log_entry,
                    leader.as_ref(),
                    self.state.as_ref(),
                ) {
                    voting::Decision::Abstain(never) => assert_unreachable!(never),
                    voting::Decision::Nay(rejection) => {
                        return Err(AcceptError::Rejected(rejection))
                    }
                    voting::Decision::Yea(yea) => {
                        yeas.push(yea);
                    }
                }
            }

            #[cfg(feature = "tracer")]
            {
                if let Some(tracer) = self.tracer.as_mut() {
                    tracer.record_accept(round_num, coord_num, log_entry.id());
                }
            }

            match self.accepted_entries.entry(round_num) {
                btree_map::Entry::Occupied(ref mut e) => {
                    // At this point we've accepted the entry. However, when a node makes a promise
                    // it's only obligated to return "the greatest-numbered proposal (if any) that
                    // it has accepted". Therefore we need not store/can overwrite an entry with a
                    // lower coordination number.
                    if e.get().0 < coord_num {
                        e.insert((coord_num, log_entry));
                    }
                }
                btree_map::Entry::Vacant(e) => {
                    e.insert((coord_num, log_entry));
                }
            }
        }

        Ok(yeas)
    }

    fn commit_entry(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    ) -> Result<(), CommitError<I>> {
        #[cfg(feature = "tracer")]
        {
            if let Some(tracer) = self.tracer.as_mut() {
                tracer.record_commit(round_num, entry.id());
            }
        }

        // Ignore any entries we've already applied.
        if round_num <= self.state_round {
            return Ok(());
        }

        if self.state.is_none() {
            return Err(CommitError::Disoriented);
        }

        self.voter.observe_commit(
            round_num,
            coord_num,
            &entry,
            self.deduce_node(round_num, coord_num).as_ref(),
        );

        self.emit(Event::Commit {
            round: round_num,
            log_entry: Arc::clone(&entry),
        });

        // Make sure that a shrinking gap doesn't get younger.
        let gap_time = self
            .pending_commits
            .range(round_num..)
            .next()
            .map(|e| e.1 .0)
            .unwrap_or_else(instant::Instant::now);

        debug!("Queuing entry {:?} for round {}.", entry, round_num);
        self.pending_commits
            .insert(round_num, (gap_time, coord_num, entry));
        self.available_round_nums.remove(&round_num);

        if let Participation::Passive {
            observed_proposals, ..
        } = &mut self.participation
        {
            // To get out of passivity, we must fully observe `c` rounds, where `c` is the
            // level of concurrency that was applicable to the first fully observed round.
            //
            // (Applicable to a round means right before an entry is applied. If, for
            // instance, we fully observe round `r` with an applicable concurrency of `1`,
            // then we may immediately come out of passivity. Even if the entry applied in
            // `r` increases concurrency.)
            //
            // It is important to remember that our criteria for a "fully observed round"
            // are that the same round and coordination number are observed in an accept and
            // a commit. This allows us to deduce that the first fully observed round `r`
            // had not been committed before. This in turn allows us to become active in
            // round `r + c`, regardless of any potential concurrency increases between now
            // (`r`) and then (`r + c`) because no node could have seen them before (`r` was
            // not settled).
            if observed_proposals.contains(&(round_num, coord_num)) {
                if let Some(concurrency) = StateOf::<I>::concurrency(self.state.as_ref()) {
                    let first_active_round = round_num + into_round_num(concurrency);
                    self.participation = Participation::PartiallyActive(first_active_round);

                    self.emit(Event::Activate(first_active_round));
                }
            }
        }

        // Are we missing commits between this one and the last one applied?
        if round_num > self.state_round + One::one() {
            self.emit_gaps(self.state_round);
        }

        Ok(())
    }

    fn apply_commits(&mut self) {
        let state = match std::mem::replace(&mut self.state, None) {
            Some(state) => state,
            None => return,
        };

        let (round, state) = self.apply_commits_to(state);

        self.state_round = round;
        self.state = Some(state);
    }

    fn apply_commits_to(&mut self, mut state: StateOf<I>) -> (RoundNumOf<I>, StateOf<I>) {
        if !self
            .pending_commits
            .contains_key(&(self.state_round + One::one()))
        {
            return (self.state_round, state);
        }

        let mut round = self.state_round;

        while let Some((_, coord_num, entry)) = self.pending_commits.remove(&(round + One::one())) {
            round = round + One::one();

            debug!("Applying entry {:?} at round {}.", entry, round);

            let awaiters = self.awaiters.remove(&entry.id()).unwrap_or_default();

            let event = if awaiters.is_empty() {
                state.apply_unobserved(&*entry, &mut self.context)
            } else {
                let (outcome, event) = state.apply(&*entry, &mut self.context);

                awaiters.into_iter().for_each(|a| {
                    // avoid clone if possible
                    if !a.is_canceled() {
                        let _ = a.send((round, outcome.clone()));
                    }
                });

                event
            };

            self.applied_entry_buffer
                .insert(round, coord_num, Arc::clone(&entry));

            let new_concurrency = state::concurrency_of(&state);
            self.emit(Event::Apply {
                round,
                log_entry: entry,
                effect: event,
                new_concurrency,
            });
        }

        self.clean_up_after_applies();

        if !self.pending_commits.is_empty() {
            self.emit_gaps(round);
        }

        (round, state)
    }

    fn clean_up_after_applies(&mut self) {
        let next_state_round = self.state_round + One::one();
        assert!(next_state_round > self.state_round);

        let first_non_obsolete_promise = overridable_lookup(
            &self.promises,
            next_state_round,
            "first non obsolete promise",
        );
        overriding_insert(&mut self.promises, Zero::zero(), first_non_obsolete_promise);

        while let Some(first_accepted_key) = self.accepted_entries.keys().next().copied() {
            if first_accepted_key <= self.state_round {
                self.accepted_entries.remove(&first_accepted_key);
            } else {
                break;
            }
        }

        let obsolete_awaiter_keys = self
            .awaiters
            .iter_mut()
            .filter_map(|(eid, senders)| {
                senders.retain(|sender| !sender.is_canceled());

                if senders.is_empty() {
                    Some(*eid)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for k in obsolete_awaiter_keys {
            self.awaiters.remove(&k);
        }
    }
}

fn into_round_num<R: RoundNum>(concurrency: std::num::NonZeroUsize) -> R {
    let num = usize::from(concurrency);

    R::try_from(num)
        .unwrap_or_else(|_| panic!("Cannot convert concurrency `{}` into a round number.", num))
}

fn overridable_lookup<R: RoundNum, C: CoordNum>(
    map: &BTreeMap<R, C>,
    round_num: R,
    expectation: &'static str,
) -> C {
    *map.range(..=round_num).last().expect(expectation).1
}

fn overriding_insert<R: RoundNum, C: CoordNum>(
    map: &mut BTreeMap<R, C>,
    round_num: R,
    coord_num: C,
) {
    map.retain(|r, c| *r <= round_num || *c > coord_num);
    map.insert(round_num, coord_num);
}

#[derive(Debug)]
pub struct ProofOfLife {
    _prevent_construction: (),
}

impl ProofOfLife {
    fn new() -> Self {
        Self {
            _prevent_construction: (),
        }
    }
}

#[pin_project]
pub struct EventStream<I: Invocation> {
    #[pin]
    delegate: mpsc::Receiver<ShutdownEvent<I>>,
}

impl<I: Invocation> futures::stream::Stream for EventStream<I> {
    type Item = ShutdownEvent<I>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        this.delegate.poll_next(cx)
    }
}

#[derive(Debug)]
pub struct RoundNumReservation<R: RoundNum> {
    round_num: R,
    sender: mpsc::UnboundedSender<Release<R>>,
}

impl<R: RoundNum> RoundNumReservation<R> {
    pub fn round_num(&self) -> R {
        self.round_num
    }
}

impl<R: RoundNum> Drop for RoundNumReservation<R> {
    fn drop(&mut self) {
        let _ = self
            .sender
            .unbounded_send(Release::RoundNum(self.round_num));
    }
}
