mod error;
mod handle;
mod io;
mod msg;
mod working_dir;

use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Instant;

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

use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::PrepareSnapshotError;
use crate::error::ReadStaleError;
use crate::error::SpawnError;
use crate::event::Event;
use crate::event::ShutdownEvent;
use crate::invocation::AbstainOf;
use crate::invocation::ContextOf;
use crate::invocation::CoordNumOf;
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
use crate::log::LogEntry;
use crate::node::NodeInfo;
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
use super::snapshot::Snapshot;
use super::status::NodeStatus;
use super::SpawnArgs;

use error::AcquireRoundNumError;
use error::ClusterError;
pub use handle::StateKeeperHandle;
use msg::Release;
use msg::Request;
use msg::Response;
use working_dir::WorkingDir;

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

#[derive(Debug)]
enum Participation<R, C> {
    Active,
    PartiallyActive(R),
    Passive { observed_proposals: HashSet<(R, C)> },
}

impl<R, C> Participation<R, C> {
    pub fn passive() -> Self {
        Self::Passive {
            observed_proposals: HashSet::new(),
        }
    }
}

impl<R, C> From<super::Participation<R>> for Participation<R, C> {
    fn from(p: super::Participation<R>) -> Self {
        match p {
            super::Participation::Active => Self::Active,
            super::Participation::Passive => Self::passive(),
            super::Participation::PartiallyActive(_) => unreachable!(),
        }
    }
}

enum AcceptPolicy<I: Invocation> {
    Irrejectable,
    Rejectable(Option<NodeOf<I>>),
}

type SpawnResult<I> = Result<(NodeStatus, super::Participation<RoundNumOf<I>>), SpawnError>;
type RoundNumRequest<I> = (RangeInclusive<RoundNumOf<I>>, ResponseSender<I>);
type AcceptedEntry<I> = (CoordNumOf<I>, Arc<LogEntryOf<I>>);
type PendingCommit<I> = (Instant, CoordNumOf<I>, Arc<LogEntryOf<I>>);

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

pub struct StateKeeper<I, V>
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
{
    context: ContextOf<I>,
    working_dir: Option<WorkingDir<RoundNumOf<I>>>,

    node_id: NodeIdOf<I>,
    voter: V,

    receiver: mpsc::Receiver<RequestAndResponseSender<I>>,

    release_sender: mpsc::UnboundedSender<Release<RoundNumOf<I>>>,
    release_receiver: mpsc::UnboundedReceiver<Release<RoundNumOf<I>>>,

    // TODO persist in snapshots
    highest_observed_round_num: Option<RoundNumOf<I>>,

    /// The highest coordination _observed_ number so far.
    ///
    /// Naively it might make sense to track these in a BTreeMap, similar to
    /// promises. However, the highest observed coordination number is only used
    /// for one thing: To determine the which coordination number to use in a
    /// bid for leader. This coordination number will then be compared against
    /// the _strongest_ promise (see prepare_entry). Therefore it makes no sense
    /// to track observations by round number.
    highest_observed_coord_num: CoordNumOf<I>,

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
    state: Option<Arc<StateOf<I>>>,

    round_num_requests: VecDeque<RoundNumRequest<I>>,
    acquired_round_nums: HashSet<RoundNumOf<I>>,

    pending_commits: BTreeMap<RoundNumOf<I>, PendingCommit<I>>,
    awaiters: HashMap<LogEntryIdOf<I>, Vec<Awaiter<I>>>,

    // TODO persist in snapshots
    participation: Participation<RoundNumOf<I>, CoordNumOf<I>>,
    /// last status that was observable from the outside
    status: NodeStatus,

    queued_events: VecDeque<ShutdownEvent<I>>,
    event_emitter: mpsc::Sender<ShutdownEvent<I>>,
    warn_counter: u16,

    applied_entry_buffer: VecDeque<(CoordNumOf<I>, Arc<LogEntryOf<I>>)>,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<I>>>,
}

impl<I, V> StateKeeper<I, V>
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
{
    pub async fn spawn(
        kit: StateKeeperKit<I>,
        args: SpawnArgs<I, V>,
    ) -> Result<
        (
            NodeStatus,
            super::Participation<RoundNumOf<I>>,
            EventStream<I>,
            ProofOfLife,
        ),
        SpawnError,
    > {
        let (evt_send, evt_recv) = mpsc::channel(32);

        let proof_of_life = ProofOfLife::new();

        let (send, recv) = oneshot::channel();

        Self::start_and_run_new(args, send, kit.receiver, evt_send);

        let start_result = recv.await.expect("StateKeeper failed to start");

        start_result.map(move |(initial_status, initial_participation)| {
            (
                initial_status,
                initial_participation,
                EventStream { delegate: evt_recv },
                proof_of_life,
            )
        })
    }

    fn start_and_run_new(
        spawn_args: SpawnArgs<I, V>,
        start_result_sender: oneshot::Sender<SpawnResult<I>>,
        receiver: mpsc::Receiver<RequestAndResponseSender<I>>,
        event_emitter: mpsc::Sender<ShutdownEvent<I>>,
    ) {
        std::thread::spawn(move || {
            let context = spawn_args.context;
            let working_dir = spawn_args.working_dir;
            let node_id = spawn_args.node_id;
            let voter = spawn_args.voter;
            let snapshot = spawn_args.snapshot;
            let participation = spawn_args.participation;
            let log_keeping = spawn_args.log_keeping;
            #[cfg(feature = "tracer")]
            let tracer = spawn_args.tracer;

            // assume we're lagging
            let initial_status = snapshot
                .as_ref()
                .map(|_| NodeStatus::Lagging)
                .unwrap_or(NodeStatus::Disoriented);

            let (state_round, state, highest_observed_coord_num, promises, accepted_entries) =
                snapshot
                    .map(|s| {
                        let DeconstructedSnapshot {
                            state_round,
                            state,
                            highest_observed_coord_num,
                            promises,
                            accepted_entries,
                        } = s.deconstruct();

                        (
                            state_round,
                            Some(state),
                            highest_observed_coord_num,
                            promises,
                            accepted_entries,
                        )
                    })
                    .unwrap_or_else(|| {
                        (
                            Zero::zero(),
                            None,
                            One::one(),
                            {
                                let mut promises = BTreeMap::new();
                                promises.insert(Zero::zero(), One::one());
                                promises
                            },
                            BTreeMap::new(),
                        )
                    });

            let (start_result, working_dir) = match working_dir {
                Some(working_dir) => match WorkingDir::init(working_dir, log_keeping) {
                    Ok(d) => (Ok(()), Some(d)),
                    Err(err) => (Err(err), None),
                },
                None => (Ok(()), None),
            };

            let failed = start_result.is_err();
            let _ = start_result_sender.send(start_result.map(|_| (initial_status, participation)));

            if failed {
                return;
            }

            let (rel_send, rel_recv) = mpsc::unbounded();

            let state_keeper = StateKeeper {
                context,
                working_dir,

                node_id,
                voter,

                receiver,

                release_sender: rel_send,
                release_receiver: rel_recv,

                promises,
                accepted_entries,
                leadership: (Zero::zero(), Zero::zero()),

                state_round,
                state,

                round_num_requests: VecDeque::new(),
                acquired_round_nums: HashSet::new(),

                highest_observed_round_num: None,
                highest_observed_coord_num,

                pending_commits: BTreeMap::new(),
                awaiters: HashMap::new(),

                participation: participation.into(),
                status: initial_status,

                queued_events: VecDeque::new(),
                event_emitter,
                warn_counter: 0,

                // TODO the capacity should be configurable
                applied_entry_buffer: VecDeque::with_capacity(1024),

                #[cfg(feature = "tracer")]
                tracer,
            };

            state_keeper.init_and_run();
        });
    }

    fn init_and_run(mut self) {
        self.emit(Event::Init {
            status: self.status,
            round: self.state_round,
            state: self.state.as_ref().map(Arc::clone),
        });

        self.run();
    }

    fn run(mut self) {
        while self.await_and_handle_next_request() {
            self.apply_commits();
            self.release_round_nums();
            self.hand_out_round_nums();
            self.detect_and_emit_status_change();

            if !self.flush_event_queue() {
                tracing::warn!("Node was not properly shut down but simply dropped.");
                return;
            }
        }

        tracing::info!("Shutting down.");
        self.shut_down()
    }

    fn await_and_handle_next_request(&mut self) -> bool {
        let next_request = futures::executor::block_on(futures::future::poll_fn(|cx| {
            self.receiver.poll_next_unpin(cx)
        }));

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
                    }
                },
                Ok(None) => unreachable!(),
                Err(_) => return,
            }
        }
    }

    fn hand_out_round_nums(&mut self) {
        let round = self.state_round;

        let requests = &mut self.round_num_requests;
        let acquireds = &mut self.acquired_round_nums;
        let deferreds = &self.pending_commits;
        let sender = &self.release_sender;

        match self.state.as_ref() {
            Some(state) => {
                let mut ix = 0;

                'next_request: while ix < requests.len() {
                    let (range, _) = &requests[ix];

                    // Rounds up to and including `r` have already converged.
                    let range = std::cmp::max(round + One::one(), *range.start())..=*range.end();

                    // If the range is empty, we'll never be able to provide a reservation.
                    if range.is_empty() {
                        let (_, send) = requests.remove(ix).unwrap();
                        let _ = send.send(Response::AcquireRoundNum(Err(
                            AcquireRoundNumError::Converged,
                        )));
                        continue;
                    }

                    // Limit the range to the concurrency window.
                    let concurrency_bound = into_round_num(state.concurrency());
                    let range =
                        *range.start()..=std::cmp::min(round + concurrency_bound, *range.end());

                    for round_num in NumberIter::from_range(range) {
                        if !acquireds.contains(&round_num) && !deferreds.contains_key(&round_num) {
                            acquireds.insert(round_num);

                            let (_, send) = requests.remove(ix).unwrap();
                            let _ = send.send(Response::AcquireRoundNum(Ok(RoundNumReservation {
                                round_num,
                                sender: sender.clone(),
                            })));
                            continue 'next_request;
                        }
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

    fn flush_event_queue(&mut self) -> bool {
        while let Some(event) = self.queued_events.pop_front() {
            let mut timeout = 1;

            let mut send = self.event_emitter.send(event);

            loop {
                timeout *= 10;

                let timeout = std::time::Duration::from_millis(timeout);
                let delay = futures_timer::Delay::new(timeout);

                match futures::executor::block_on(futures::future::select(send, delay)) {
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

    fn shut_down(mut self) {
        let snapshot = self.prepare_snapshot().ok();
        let mut emitter = self.event_emitter;

        let _ = futures::executor::block_on(emitter.send(ShutdownEvent::Last { snapshot }));
    }

    fn handle_request_msg(&mut self, req: Request<I>, resp_sender: oneshot::Sender<Response<I>>) {
        let resp = match req {
            Request::PrepareSnapshot => Response::PrepareSnapshot(self.prepare_snapshot()),

            Request::AffirmSnapshot { snapshot } => {
                Response::AffirmSnapshot(self.affirm_snapshot(snapshot))
            }

            Request::InstallSnapshot { snapshot } => {
                Response::InstallSnapshot(self.install_snapshot(snapshot))
            }

            Request::ReadStale => Response::ReadStale(match &self.state {
                Some(s) => Ok(Arc::clone(s)),
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
                            let offset = crate::util::usize_delta(round_num, self.state_round);
                            let offset = std::num::NonZeroUsize::new(offset)
                                .expect("Zero was ruled out via equality check");

                            let bound = state.concurrency();

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
            Request::HighestObservedCoordNum => {
                Response::HighestObservedCoordNum(Ok(self.highest_observed_coord_num))
            }

            Request::PrepareEntry {
                round_num,
                coord_num,
            } => {
                self.observe_round_num(round_num);
                self.observe_coord_num(coord_num);

                let result = self.prepare_entry(round_num, coord_num);

                if result.is_ok() {
                    self.emit_directive(round_num, coord_num);
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
                    self.emit_directive(round_num, coord_num);
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
                        self.emit_directive(round_num, coord_num);
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
                    self.emit_directive(round_num, coord_num);
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
                            self.emit_directive(round_num, coord_num);
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

    fn become_passive(&mut self) {
        self.participation = Participation::passive();
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
                    .highest_observed_round_num
                    .unwrap_or_else(Bounded::max_value)
                    > self.state_round + into_round_num(state.concurrency())
                {
                    NodeStatus::Lagging
                } else if self.highest_observed_coord_num == self.leadership.1 {
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

    fn emit_directive(&mut self, round_num: RoundNumOf<I>, coord_num: CoordNumOf<I>) {
        if let Some(leader) = self.deduce_node(round_num, coord_num) {
            self.emit(Event::Directive {
                leader,
                round_num,
                coord_num,
                timestamp: Instant::now(),
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
                && round_num <= self.state_round + into_round_num(state.concurrency())
            {
                // TODO lots of duplication with NodeInner::determine_coord_num
                let round_offset = crate::util::usize_delta(round_num, self.state_round);
                let round_offset = std::num::NonZeroUsize::new(round_offset)
                    .expect("Zero was ruled out via equality check");

                let cluster = state.cluster_at(round_offset);
                let cluster_size = CoordNumOf::<I>::try_from(cluster.len()).unwrap_or_else(|_| {
                    panic!(
                        "Cannot convert cluster size `{}` into a coordination number.",
                        cluster.len()
                    )
                });
                let leader_ix = std::convert::TryInto::<usize>::try_into(coord_num % cluster_size)
                    .unwrap_or_else(|_| {
                        panic!("Out of usize range: {}", coord_num % cluster_size);
                    });

                let leader = cluster.into_iter().nth(leader_ix).unwrap();

                Some(leader)
            } else {
                None
            }
        })
    }

    fn observe_round_num(&mut self, round_num: RoundNumOf<I>) {
        self.highest_observed_round_num = self
            .highest_observed_round_num
            .map(|r| std::cmp::max(r, round_num))
            .or(Some(round_num));
    }

    fn observe_coord_num(&mut self, coord_num: CoordNumOf<I>) {
        self.highest_observed_coord_num = std::cmp::max(self.highest_observed_coord_num, coord_num);
    }

    fn prepare_snapshot(&mut self) -> Result<SnapshotFor<I>, PrepareSnapshotError> {
        let state = self
            .state
            .as_ref()
            .ok_or(PrepareSnapshotError::Disoriented)?;

        Ok(Snapshot::new(
            self.state_round,
            Arc::clone(state),
            self.highest_observed_coord_num,
            self.promises.clone(),
            self.accepted_entries.clone(),
        ))
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
            state_round,
            state,
            highest_observed_coord_num,
            promises,
            accepted_entries,
        } = snapshot.deconstruct();

        self.become_passive();

        self.state_round = state_round;
        self.state = Some(Arc::clone(&state));
        self.highest_observed_coord_num = highest_observed_coord_num;
        self.promises = promises;
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
                self.highest_observed_coord_num,
                self.try_get_applied_entry(round_num),
            ));
        } else if let btree_map::Entry::Occupied(e) = self.pending_commits.entry(round_num) {
            let coord_num = e.get().1;
            let log_entry = Arc::clone(&e.get().2);

            return Err(PrepareError::Converged(
                self.highest_observed_coord_num,
                Some((coord_num, log_entry)),
            ));
        }

        // We always compare against the strongest promise/highest coordination number.
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
                self.state.as_deref(),
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
        if let Some(offset) = crate::util::try_usize_delta(self.state_round, round_num) {
            if let Some((c, e)) = self.applied_entry_buffer.get(offset) {
                return Some((*c, Arc::clone(e)));
            }
        }

        if let Some(working_dir) = self.working_dir.as_mut() {
            return working_dir.try_get_entry_applied_in(round_num);
        }

        None
    }

    fn accept_entry(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    ) -> Result<YeaOf<I>, AcceptError<I>> {
        if round_num <= self.state_round {
            Err(AcceptError::Converged(
                self.highest_observed_coord_num,
                self.try_get_applied_entry(round_num),
            ))
        } else if let btree_map::Entry::Occupied(e) = self.pending_commits.entry(round_num) {
            let coord_num = e.get().1;
            let log_entry = Arc::clone(&e.get().2);

            Err(AcceptError::Converged(
                self.highest_observed_coord_num,
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
                    self.state.as_deref(),
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
                    // it's only obligated to return "the highest-numbered proposal (if any) that it
                    // has accepted". Therefore we need not store/can overwrite an entry with a
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
            .unwrap_or_else(Instant::now);

        debug!("Queuing entry {:?} for round {}.", entry, round_num);
        self.pending_commits
            .insert(round_num, (gap_time, coord_num, entry));

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

    fn apply_commits_to(&mut self, state: Arc<StateOf<I>>) -> (RoundNumOf<I>, Arc<StateOf<I>>) {
        if !self
            .pending_commits
            .contains_key(&(self.state_round + One::one()))
        {
            return (self.state_round, state);
        }

        let mut round = self.state_round;
        let mut state = Arc::try_unwrap(state).unwrap_or_else(|s| (&*s).clone());

        while let Some((_, coord_num, entry)) = self.pending_commits.remove(&(round + One::one())) {
            round = round + One::one();

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
                if observed_proposals.contains(&(round, coord_num)) {
                    let first_active_round = round + into_round_num(state.concurrency());
                    self.participation = Participation::PartiallyActive(first_active_round);

                    self.emit(Event::Activate(first_active_round));
                }
            }

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

            if self.applied_entry_buffer.capacity() > 0 {
                if self.applied_entry_buffer.len() == self.applied_entry_buffer.capacity() {
                    self.applied_entry_buffer.pop_back();
                }
                self.applied_entry_buffer
                    .push_front((coord_num, Arc::clone(&entry)));
            }

            if let Some(working_dir) = self.working_dir.as_mut() {
                working_dir.log_entry_application(round, coord_num, &*entry);
            }

            self.emit(Event::Apply {
                round,
                log_entry: entry,
                result: event,
            });
        }

        self.clean_up_after_applies();

        if !self.pending_commits.is_empty() {
            self.emit_gaps(round);
        }

        (round, Arc::new(state))
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
