use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::convert::TryFrom;
use std::future::Future;
use std::ops::RangeInclusive;
use std::rc::Rc;
use std::sync::Arc;

use futures::future::FutureExt;
use futures::lock::Mutex;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use num_traits::One;
use num_traits::Zero;
use tracing::debug;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::append::Importance;
use crate::append::Peeryness;
use crate::applicable::ApplicableTo;
use crate::communicator::Acceptance;
use crate::communicator::Communicator;
use crate::communicator::ErrorOf;
use crate::communicator::Vote;
use crate::error::ShutDownOr;
use crate::invocation::AbstainOf;
use crate::invocation::AcceptanceFor;
use crate::invocation::CommitFor;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::PromiseFor;
use crate::invocation::RoundNumOf;
use crate::invocation::StateOf;
use crate::invocation::VoteFor;
use crate::invocation::YeaOf;
use crate::retry::RetryPolicy;
use crate::Conflict;
use crate::LogEntry;
use crate::Promise;

use super::commits::Commit;
use super::commits::Commits;
use super::state_keeper::StateKeeperHandle;
use super::NodeInfo;

struct ElectionToken;

pub struct NodeInner<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = NodeOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        LogEntry = LogEntryOf<I>,
        Error = CommunicationErrorOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
{
    id: NodeIdOf<I>,
    communicator: RefCell<C>,

    state_keeper: StateKeeperHandle<I>,

    election_lock: Mutex<ElectionToken>,
    term_start: Cell<RoundNumOf<I>>,
    campaigned_on: Cell<CoordNumOf<I>>,

    commits: Commits,
}

impl<I, C> NodeInner<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = NodeOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        LogEntry = LogEntryOf<I>,
        Error = CommunicationErrorOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
{
    pub fn new(
        id: NodeIdOf<I>,
        communicator: C,
        state_keeper: StateKeeperHandle<I>,
        commits: Commits,
    ) -> Self {
        Self {
            id,
            communicator: RefCell::new(communicator),

            state_keeper,

            election_lock: Mutex::new(ElectionToken),
            term_start: Cell::new(Zero::zero()),
            campaigned_on: Cell::new(Zero::zero()),

            commits,
        }
    }

    pub fn id(&self) -> NodeIdOf<I> {
        self.id
    }

    pub async fn append<A, R>(
        self: Rc<Self>,
        applicable: A,
        args: AppendArgs<I, R>,
    ) -> Result<CommitFor<I, A>, ShutDownOr<R::Error>>
    where
        A: ApplicableTo<StateOf<I>>,
        R: RetryPolicy<Invocation = I>,
    {
        let log_entry = applicable.into_log_entry();
        let log_entry_id = log_entry.id();

        let passive = self
            .state_keeper
            .await_commit_of(log_entry_id)
            .await
            .map_err(|_| ShutDownOr::ShutDown)?
            .then(|r| match r {
                Ok((round_num, outcome)) => {
                    futures::future::ready(Ok(Commit::ready(round_num, outcome))).left_future()
                }
                Err(_) => futures::future::pending().right_future(),
            });

        let active = self.append_actively(log_entry, args);

        crate::util::Race::between(active, passive)
            .await
            .map(|commit| commit.projected())
    }

    async fn append_actively<R>(
        self: Rc<Self>,
        log_entry: Arc<LogEntryOf<I>>,
        mut args: AppendArgs<I, R>,
    ) -> Result<CommitFor<I>, ShutDownOr<R::Error>>
    where
        R: RetryPolicy<Invocation = I>,
    {
        let mut i: usize = 0;

        loop {
            let error = match Rc::clone(&self)
                .try_append(Arc::clone(&log_entry), args.round.clone(), args.importance)
                .await
            {
                Ok(r) => {
                    break Ok(r);
                }
                Err(e) => e,
            };

            args.retry_policy.eval(error).await?;

            i += 1;

            if i.count_ones() == 1 && i >= 8 {
                debug!("Retrying append of {:?} (#{}).", log_entry, i);
            }
        }
    }

    async fn try_append(
        self: Rc<Self>,
        log_entry: Arc<LogEntryOf<I>>,
        round: RangeInclusive<RoundNumOf<I>>,
        importance: Importance,
    ) -> Result<CommitFor<I>, AppendError<I>> {
        let reservation = self.state_keeper.reserve_round_num(round).await?;

        let result = self
            .try_append_internal(log_entry, reservation.round_num(), importance)
            .await;

        // The reservation must be held until the attempt is over.
        drop(reservation);

        result
    }

    async fn try_append_internal(
        self: Rc<Self>,
        log_entry: Arc<LogEntryOf<I>>,
        round_num: RoundNumOf<I>,
        importance: Importance,
    ) -> Result<CommitFor<I>, AppendError<I>> {
        let node_id = self.id;

        let log_entry_id = log_entry.id();

        let cluster = self.state_keeper.cluster_for(round_num).await?;
        let cluster_size = cluster.len();

        let own_node_ix = cluster
            .iter()
            .position(|n| n.id() == self.id)
            .ok_or(AppendError::Exiled)?;

        // The number of *additional* nodes that make a quorum.
        let quorum_prime = cluster.len() / 2;

        let other_nodes: Vec<NodeOf<I>> = cluster
            .into_iter()
            .filter(move |n| n.id() != node_id)
            .collect();

        // phase 0: determine coordination number
        let coord_num = self.determine_coord_num(cluster_size, own_node_ix).await?;

        // phase 1: become leader
        let converged_log_entry = self
            .ensure_leadership(round_num, coord_num, quorum_prime, &other_nodes, importance)
            .await?;

        let converged_log_entry = converged_log_entry.unwrap_or(log_entry);
        let converged_log_entry_id = converged_log_entry.id();

        // phase 2: propose entry
        let (accepted, rejected_or_failed, pending_acceptances) = self
            .propose_entry(
                round_num,
                coord_num,
                quorum_prime,
                &other_nodes,
                Arc::clone(&converged_log_entry),
            )
            .await?;

        // phase 3: commit entry
        let commit = self
            .commit_entry(
                round_num,
                coord_num,
                converged_log_entry,
                other_nodes,
                accepted,
                rejected_or_failed,
                pending_acceptances,
            )
            .await?;

        if converged_log_entry_id == log_entry_id {
            Ok(commit)
        } else {
            Err(AppendError::Railroaded)
        }
    }

    // This essentially implements footnote 1 on page 4 of "Paxos Made Live - An
    // Engineering Perspective" by Chandra, Griesemer and Redstone.
    async fn determine_coord_num(
        &self,
        cluster_size: usize,
        own_node_idx: usize,
    ) -> Result<CoordNumOf<I>, AppendError<I>> {
        assert!(own_node_idx < cluster_size);

        let cluster_size = CoordNumOf::<I>::try_from(cluster_size).unwrap_or_else(|_| {
            panic!(
                "Cannot convert cluster size `{}` into a coordination number.",
                cluster_size
            )
        });

        let own_node_ix = CoordNumOf::<I>::try_from(own_node_idx).unwrap_or_else(|_| {
            panic!(
                "Cannot convert `{}` into a coordination number.",
                own_node_idx
            )
        });

        let greatest_observed_coord_num = self.state_keeper.greatest_observed_coord_num().await?;
        let lowest_possible = std::cmp::max(greatest_observed_coord_num, One::one());

        let remainder = lowest_possible % cluster_size;

        let coord_num = if remainder <= own_node_ix {
            lowest_possible + own_node_ix - remainder
        } else {
            lowest_possible + cluster_size + own_node_ix - remainder
        };

        assert!(coord_num >= lowest_possible);

        Ok(coord_num)
    }

    async fn ensure_leadership(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<I>],
        importance: Importance,
    ) -> Result<Option<Arc<LogEntryOf<I>>>, AppendError<I>> {
        // This holds iff we already believe to be leader for `round_num`.
        if round_num >= self.term_start.get() && coord_num == self.campaigned_on.get() {
            return Ok(self.state_keeper.accepted_entry_of(round_num).await?);
        }

        // We're not leader for `round_num`, what now??
        match importance {
            Importance::GainLeadership => {
                // We limit ourselves to one election at a time.
                //
                // This is relevant in the common case of multiple concurrent appends by one
                // node. In such cases the mandate obtained by a successful election result of
                // the first append can be used skip the election of any subsequent appends.
                //
                // Limiting ourselves to one election therefore reduces network traffic. It will
                // even reduce latencies because the result of the first election will, on
                // average, be available before the results of additional elections would have
                // been.
                //
                // Also, subsequent elections would have failed, assuming they would have used
                // the same coordination number (see StateKeeper::prepare_entry). This way we
                // get to re-check for a mandate below.
                //
                // The downside is that latencies are increased in rare cases where multiple
                // elections are required. This happens when the first append targets a later
                // round or if a coordination number of another node is observed.
                let election_token = self.election_lock.lock().await;

                // Check whether we now have a valid mandate due to another election finishing
                // while we waited on the lock.
                if round_num >= self.term_start.get() && coord_num == self.campaigned_on.get() {
                    return Ok(self.state_keeper.accepted_entry_of(round_num).await?);
                }

                self.become_leader(
                    &*election_token,
                    round_num,
                    coord_num,
                    quorum_prime,
                    other_nodes,
                )
                .await
            }
            Importance::MaintainLeadership(peeryness) => {
                if peeryness == Peeryness::Peery && !other_nodes.is_empty() {
                    let coord_num = self.campaigned_on.get();

                    let mut pending_responses = self
                        .communicator
                        .borrow_mut()
                        .send_prepare(other_nodes, round_num, coord_num)
                        .into_iter()
                        .map(|(_node, fut)| fut.map(|x| x))
                        .into_iter()
                        .collect::<FuturesUnordered<_>>();

                    let mut converged = false;

                    while let Some(response) = pending_responses.next().await {
                        if let Ok(Vote::Conflicted(rejection)) = response {
                            self.state_keeper
                                .observe_coord_num(rejection.coord_num())
                                .await?;

                            if let Conflict::Converged { log_entry, .. } = &rejection {
                                converged = true;

                                if let Some((coord_num, entry)) = log_entry {
                                    self.state_keeper
                                        .commit_entry(round_num, *coord_num, Arc::clone(entry))
                                        .await?;

                                    return Err(AppendError::Converged { caught_up: true });
                                }
                            }
                        }
                    }

                    if converged {
                        return Err(AppendError::Converged { caught_up: false });
                    }
                }

                Err(AppendError::Lost)
            }
        }
    }

    async fn become_leader(
        &self,
        _election_token: &ElectionToken,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<I>],
    ) -> Result<Option<Arc<LogEntryOf<I>>>, AppendError<I>> {
        let own_promise = self
            .state_keeper
            .prepare_entry(round_num, coord_num)
            .await?;

        let pending_responses = self
            .communicator
            .borrow_mut()
            .send_prepare(other_nodes, round_num, coord_num)
            .into_iter()
            .map(|(_node, fut)| fut.map(|x| x));

        let promise_or_rejection = self
            .await_promise_quorum_or_first_conflict(
                round_num,
                quorum_prime,
                pending_responses,
                own_promise,
            )
            .await;

        match promise_or_rejection? {
            Vote::Given(promise) => {
                let converged_log_entry = if promise.is_empty() {
                    None
                } else {
                    let converged_log_entry = promise.log_entry_for(round_num);

                    // We must accept all the promised entries. Afterwards preparing further entries
                    // locally will return these accepted entries and make sure we converge towards
                    // them. This is crucial to fulfill Paxos' convergence requirement (P2 in PMS).
                    self.state_keeper
                        .accept_entries(
                            coord_num,
                            promise
                                .into_iter()
                                .map(|c| (c.round_num, c.log_entry))
                                .collect(),
                        )
                        .await?;

                    converged_log_entry
                };

                // We may skip phase 1 going forward…
                self.term_start.set(round_num);
                self.campaigned_on.set(coord_num);

                self.state_keeper
                    .assume_leadership(round_num, coord_num)
                    .await?;

                Ok(converged_log_entry)
            }

            Vote::Conflicted(rejection) => {
                self.state_keeper
                    .observe_coord_num(rejection.coord_num())
                    .await?;

                if let Conflict::Converged {
                    log_entry: Some((coord_num, entry)),
                    ..
                } = rejection
                {
                    let _ = self
                        .state_keeper
                        .commit_entry(round_num, coord_num, entry)
                        .await;
                }

                Err(AppendError::Lost)
            }

            Vote::Abstained(_infallible) => {
                unreachable!()
            }
        }
    }

    async fn await_promise_quorum_or_first_conflict(
        &self,
        round_num: RoundNumOf<I>,
        quorum: usize,
        pending_responses: impl IntoIterator<
            Item = impl Future<Output = Result<VoteFor<I>, ErrorOf<C>>>,
        >,
        own_promise: PromiseFor<I>,
    ) -> Result<Vote<RoundNumOf<I>, CoordNumOf<I>, LogEntryOf<I>, Infallible>, AppendError<I>> {
        if quorum == 0 {
            return Ok(Vote::Given(own_promise));
        }

        let mut pending_responses: FuturesUnordered<_> = pending_responses.into_iter().collect();

        assert!(quorum <= pending_responses.len());

        let mut pending_len = pending_responses.len();
        let mut promises = 0;
        let mut max_promise = own_promise;

        let mut abstentions = Vec::new();
        let mut communication_errors = Vec::new();

        while let Some(response) = pending_responses.next().await {
            pending_len -= 1;

            match response {
                Err(err) => {
                    communication_errors.push(err);

                    if promises + pending_len < quorum {
                        return Err(AppendError::NoQuorum {
                            abstentions,
                            communication_errors,
                            rejections: Vec::new(),
                        });
                    }
                }

                Ok(Vote::Abstained(abstention)) => {
                    abstentions.push(abstention);
                }

                Ok(Vote::Conflicted(rejection)) => {
                    self.state_keeper
                        .observe_coord_num(rejection.coord_num())
                        .await?;

                    if let Conflict::Converged {
                        log_entry: Some((coord_num, entry)),
                        ..
                    } = &rejection
                    {
                        self.state_keeper
                            .commit_entry(round_num, *coord_num, Arc::clone(entry))
                            .await?;
                    }

                    return Ok(Vote::Conflicted(rejection));
                }

                Ok(Vote::Given(promise)) => {
                    let current_max_promise = std::mem::replace(&mut max_promise, Promise::empty());
                    let new_max_promise = current_max_promise.merge_with(promise);

                    if promises + 1 >= quorum {
                        return Ok(Vote::Given(new_max_promise));
                    } else {
                        promises += 1;
                        max_promise = new_max_promise;
                    }
                }
            }
        }

        panic!("await_promise_quorum_or_first_rejection: insufficient pending_responses");
    }

    async fn propose_entry(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<I>],
        log_entry: Arc<LogEntryOf<I>>,
    ) -> Result<
        (
            HashSet<NodeIdOf<I>>,
            HashSet<NodeIdOf<I>>,
            FuturesUnordered<
                impl Future<Output = (NodeIdOf<I>, Result<AcceptanceFor<I>, ErrorOf<C>>)>,
            >,
        ),
        AppendError<I>,
    > {
        self.state_keeper
            .accept_entry(round_num, coord_num, Arc::clone(&log_entry))
            .await?;

        let accepts = self
            .communicator
            .borrow_mut()
            .send_proposal(other_nodes, round_num, coord_num, Arc::clone(&log_entry))
            .into_iter()
            .map(|(node, fut)| {
                let node_id: NodeIdOf<I> = node.id();
                fut.map(move |r| (node_id, r))
            });

        self.await_accepted_quorum(round_num, quorum_prime, accepts)
            .await
    }

    async fn await_accepted_quorum<'a, P>(
        &self,
        round_num: RoundNumOf<I>,
        quorum: usize,
        pending_responses: impl IntoIterator<Item = P>,
    ) -> Result<
        (
            HashSet<NodeIdOf<I>>,
            HashSet<NodeIdOf<I>>,
            FuturesUnordered<P>,
        ),
        AppendError<I>,
    >
    where
        P: Future<Output = (NodeIdOf<I>, Result<AcceptanceFor<I>, ErrorOf<C>>)>,
    {
        if quorum == 0 {
            return Ok((
                HashSet::new(),
                HashSet::new(),
                pending_responses.into_iter().collect(),
            ));
        }

        let mut pending_responses: FuturesUnordered<P> = pending_responses.into_iter().collect();

        assert!(quorum <= pending_responses.len());

        let mut pending_len = pending_responses.len();
        let mut accepted = HashSet::new();
        let mut rejected_or_failed = HashSet::new();

        let mut rejections = Vec::new();
        let mut communication_errors = Vec::new();

        let mut conflict = None;

        while let Some((node_id, response)) = pending_responses.next().await {
            pending_len -= 1;

            match response {
                Ok(Acceptance::Given(_)) => {
                    accepted.insert(node_id);

                    if accepted.len() >= quorum {
                        return Ok((accepted, rejected_or_failed, pending_responses));
                    }
                }
                Ok(Acceptance::Conflicted(Conflict::Converged {
                    log_entry: Some((coord_num, entry)),
                    ..
                })) => {
                    self.state_keeper
                        .commit_entry(round_num, coord_num, entry)
                        .await?;

                    return Err(AppendError::Converged { caught_up: true });
                }
                rejection_or_failure => {
                    match rejection_or_failure {
                        Err(err) => {
                            communication_errors.push(err);
                        }
                        Ok(Acceptance::Conflicted(
                            Conflict::Supplanted { coord_num }
                            | Conflict::Converged { coord_num, .. },
                        )) => {
                            conflict = conflict
                                .map(|c| std::cmp::max(c, coord_num))
                                .or(Some(coord_num));
                        }
                        Ok(Acceptance::Refused(rejection)) => rejections.push(rejection),
                        Ok(Acceptance::Given(_)) => {
                            // covered in outer match
                            unreachable!()
                        }
                    }

                    if accepted.len() + pending_len < quorum {
                        if let Some(coord_num) = conflict {
                            self.state_keeper.observe_coord_num(coord_num).await?;
                        }

                        return Err(AppendError::NoQuorum {
                            abstentions: Vec::new(),
                            communication_errors,
                            rejections,
                        });
                    }

                    rejected_or_failed.insert(node_id);
                }
            }
        }

        panic!("await_accepted_quorum: insufficient pending_responses");
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_entry(
        self: Rc<Self>,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry: Arc<LogEntryOf<I>>,
        mut other_nodes: Vec<NodeOf<I>>,
        accepted: HashSet<NodeIdOf<I>>,
        rejected_or_failed: HashSet<NodeIdOf<I>>,
        mut pending_acceptances: FuturesUnordered<
            impl 'static + Future<Output = (NodeIdOf<I>, Result<AcceptanceFor<I>, ErrorOf<C>>)>,
        >,
    ) -> Result<CommitFor<I>, AppendError<I>> {
        let accepted_nodes = other_nodes
            .iter()
            .cloned()
            .filter(|n| accepted.contains(&n.id()))
            .collect::<Vec<_>>();
        other_nodes.retain(|n| !accepted.contains(&n.id()));

        let rejected_or_failed_nodes = other_nodes
            .iter()
            .cloned()
            .filter(|n| rejected_or_failed.contains(&n.id()))
            .collect::<Vec<_>>();
        other_nodes.retain(|n| !rejected_or_failed.contains(&n.id()));

        let mut pending_nodes_by_id = other_nodes
            .into_iter()
            .map(|n| (n.id(), n))
            .collect::<HashMap<_, _>>();

        let state_keeper = self.state_keeper.clone();
        let commits = self.commits.clone();
        let log_entry_for_others = Arc::clone(&log_entry);

        let cle_id = log_entry_for_others.id();

        self.communicator
            .borrow_mut()
            .send_commit_by_id(&accepted_nodes, round_num, coord_num, cle_id)
            .into_iter()
            .for_each(|(_n, f)| commits.submit(f.map(|_| ())));

        self.communicator
            .borrow_mut()
            .send_commit(
                &rejected_or_failed_nodes,
                round_num,
                coord_num,
                Arc::clone(&log_entry_for_others),
            )
            .into_iter()
            .for_each(|(_n, f)| commits.submit(f.map(|_| ())));

        let pending = async move {
            while let Some((node_id, response)) = pending_acceptances.next().await {
                let node = pending_nodes_by_id.remove(&node_id).expect("pending node");

                match response {
                    Ok(Acceptance::Given(_)) => {
                        let commit = self
                            .communicator
                            .borrow_mut()
                            .send_commit_by_id(&[node], round_num, coord_num, cle_id)
                            .into_iter()
                            .next()
                            .expect("Expected exactly one element.")
                            .1
                            .map(|_| ());

                        self.commits.submit(commit);
                    }
                    _ => {
                        let commit = self
                            .communicator
                            .borrow_mut()
                            .send_commit(
                                &[node],
                                round_num,
                                coord_num,
                                Arc::clone(&log_entry_for_others),
                            )
                            .into_iter()
                            .next()
                            .expect("Expected exactly one element.")
                            .1
                            .map(|_| ());

                        self.commits.submit(commit);
                    }
                }
            }
        };

        commits.submit(pending);

        Ok(state_keeper
            .commit_entry(round_num, coord_num, log_entry)
            .await?)
    }
}
