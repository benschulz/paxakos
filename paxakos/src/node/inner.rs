use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::future::Future;
use std::ops::RangeInclusive;
use std::rc::Rc;
use std::sync::Arc;

use futures::future::FutureExt;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use num_traits::{One, Zero};
use tracing::debug;

use crate::append::{AppendArgs, AppendError, Importance, Peeryness};
use crate::applicable::{ApplicableTo, ProjectionOf};
use crate::communicator::{AcceptanceOrRejection, Communicator, CoordNumOf, ErrorOf};
use crate::communicator::{PromiseOrRejection, RoundNumOf};
use crate::error::PrepareError;
use crate::state::{LogEntryOf, NodeIdOf, NodeOf};
use crate::{LogEntry, Promise, Rejection, State};

use super::commits::{Commit, Commits};
use super::state_keeper::StateKeeperHandle;
use super::NodeInfo;

pub struct NodeInner<S, C>
where
    S: State,
    C: Communicator,
{
    id: NodeIdOf<S>,
    communicator: RefCell<C>,

    state_keeper: StateKeeperHandle<S, RoundNumOf<C>, CoordNumOf<C>>,

    term_start: Cell<RoundNumOf<C>>,
    campaigned_on: Cell<CoordNumOf<C>>,

    commits: Commits,
}

impl<S, C> NodeInner<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    pub fn new(
        id: NodeIdOf<S>,
        communicator: C,
        state_keeper: StateKeeperHandle<S, RoundNumOf<C>, CoordNumOf<C>>,
        commits: Commits,
    ) -> Self {
        Self {
            id,
            communicator: RefCell::new(communicator),

            state_keeper,

            term_start: Cell::new(Zero::zero()),
            campaigned_on: Cell::new(Zero::zero()),

            commits,
        }
    }

    pub async fn append<A: ApplicableTo<S>>(
        self: Rc<Self>,
        applicable: A,
        args: AppendArgs<RoundNumOf<C>>,
    ) -> Result<Commit<S, RoundNumOf<C>, ProjectionOf<A, S>>, AppendError> {
        let log_entry = applicable.into_log_entry();
        let log_entry_id = log_entry.id();

        let passive = self
            .state_keeper
            .await_commit_of(log_entry_id)
            .await?
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

    async fn append_actively(
        self: Rc<Self>,
        log_entry: Arc<LogEntryOf<S>>,
        mut args: AppendArgs<RoundNumOf<C>>,
    ) -> Result<Commit<S, RoundNumOf<C>>, AppendError> {
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

            if let Err(abortion) = args.retry_policy.eval(error).await {
                break Err(AppendError::Aborted(abortion));
            }

            i += 1;

            if i.count_ones() == 1 && i >= 8 {
                debug!("Retrying append of {:?} (#{}).", log_entry, i);
            }
        }
    }

    async fn try_append(
        self: Rc<Self>,
        log_entry: Arc<LogEntryOf<S>>,
        round: RangeInclusive<RoundNumOf<C>>,
        importance: Importance,
    ) -> Result<Commit<S, RoundNumOf<C>>, AppendError> {
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
        log_entry: Arc<LogEntryOf<S>>,
        round_num: RoundNumOf<C>,
        importance: Importance,
    ) -> Result<Commit<S, RoundNumOf<C>>, AppendError> {
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

        let other_nodes: Vec<NodeOf<S>> = cluster
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
    ) -> Result<CoordNumOf<C>, AppendError> {
        assert!(own_node_idx < cluster_size);

        let cluster_size = CoordNumOf::<C>::try_from(cluster_size).unwrap_or_else(|_| {
            panic!(
                "Cannot convert cluster size `{}` into a coordination number.",
                cluster_size
            )
        });

        let own_node_ix = CoordNumOf::<C>::try_from(own_node_idx).unwrap_or_else(|_| {
            panic!(
                "Cannot convert `{}` into a coordination number.",
                own_node_idx
            )
        });

        let highest_observed_coord_num = self.state_keeper.highest_observed_coord_num().await?;
        let lowest_possible = std::cmp::max(highest_observed_coord_num, One::one());

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
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<S>],
        importance: Importance,
    ) -> Result<Option<Arc<LogEntryOf<S>>>, AppendError> {
        // This holds iff we already believe to be leader for `round_num`.
        if round_num >= self.term_start.get() && coord_num == self.campaigned_on.get() {
            return Ok(self.state_keeper.accepted_entry_of(round_num).await?);
        }

        // We're not leader for `round_num`, what now??
        match importance {
            Importance::GainLeadership => {
                self.become_leader(round_num, coord_num, quorum_prime, other_nodes)
                    .await
            }
            Importance::MaintainLeadership(peeryness) => {
                if peeryness == Peeryness::Peery && !other_nodes.is_empty() {
                    let coord_num = self.campaigned_on.get();

                    let pending_responses = self
                        .communicator
                        .borrow_mut()
                        .send_prepare(other_nodes, round_num, coord_num)
                        .into_iter()
                        .map(|(_node, fut)| fut.map(|x| x));

                    // TODO This aborts on first rejection. It may be worthwile to keep going
                    //      until quorum
                    let _ = self
                        .await_promise_quorum_or_first_rejection(
                            round_num,
                            quorum_prime + 1,
                            pending_responses,
                            Promise::empty(),
                        )
                        .await;
                }

                Err(AppendError::Lost)
            }
        }
    }

    async fn become_leader(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<S>],
    ) -> Result<Option<Arc<LogEntryOf<S>>>, AppendError> {
        let own_promise = match self.state_keeper.prepare_entry(round_num, coord_num).await {
            Ok(promise) => promise,
            Err(err) => {
                if let PrepareError::Conflict(conflict) = err {
                    if conflict == coord_num {
                        // This is a somewhat ugly hack.
                        //
                        // What can happen is that we were elected leader but due to a communication
                        // error we don't realize. Because we don't know, we cannot skip election,
                        // but because we are leader and haven't observed a high enough coordination
                        // number, we keep reusing the one that should still be good. (And it would
                        // be good, if only we knew it.) But we cannot win an election with it.
                        //
                        // It's very difficult to work around this issue without either
                        //  - introducing lots of complexity,
                        //  - constantly bumping coordination numbers
                        //  - losing liveness or even (*shudder*)
                        //  - consistency (additional notes in `StateKeeper::prepare_entry`).
                        //
                        // So here we simply bump the coordination number after we've failed to
                        // vote for ourselves because of a conflict with the same coordination
                        // number.
                        self.state_keeper
                            .observe_coord_num(conflict + One::one())
                            .await?;
                    }
                }

                return Err(err.into());
            }
        };

        let pending_responses = self
            .communicator
            .borrow_mut()
            .send_prepare(other_nodes, round_num, coord_num)
            .into_iter()
            .map(|(_node, fut)| fut.map(|x| x));

        let promise_or_rejection = self
            .await_promise_quorum_or_first_rejection(
                round_num,
                quorum_prime,
                pending_responses,
                own_promise,
            )
            .await;

        match promise_or_rejection? {
            PromiseOrRejection::Promise(promise) => {
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
                            promise.into_iter().map(|(r, _, e)| (r, e)).collect(),
                        )
                        .await?;

                    converged_log_entry
                };

                // We may skip phase 1 going forward…
                self.term_start.set(round_num);
                self.campaigned_on.set(coord_num);

                Ok(converged_log_entry)
            }

            PromiseOrRejection::Rejection(rejection) => {
                self.state_keeper
                    .observe_coord_num(rejection.coord_num())
                    .await?;

                if let Rejection::Converged {
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
        }
    }

    async fn await_promise_quorum_or_first_rejection(
        &self,
        round_num: RoundNumOf<C>,
        quorum: usize,
        pending_responses: impl IntoIterator<
            Item = impl Future<
                Output = Result<
                    PromiseOrRejection<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<S>>,
                    ErrorOf<C>,
                >,
            >,
        >,
        own_promise: Promise<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<S>>,
    ) -> Result<PromiseOrRejection<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<S>>, AppendError> {
        if quorum == 0 {
            return Ok(PromiseOrRejection::Promise(own_promise));
        }

        let mut pending_responses: FuturesUnordered<_> = pending_responses.into_iter().collect();

        let mut pending_len = pending_responses.len();
        let mut promises = 0;
        let mut max_promise = own_promise;

        while let Some(response) = pending_responses.next().await {
            pending_len -= 1;

            match response {
                Err(_) => {
                    if promises + pending_len < quorum {
                        return Err(AppendError::NoQuorum);
                    }
                }

                Ok(PromiseOrRejection::Rejection(rejection)) => {
                    self.state_keeper
                        .observe_coord_num(rejection.coord_num())
                        .await?;

                    if let Rejection::Converged {
                        log_entry: Some((coord_num, entry)),
                        ..
                    } = &rejection
                    {
                        self.state_keeper
                            .commit_entry(round_num, *coord_num, Arc::clone(entry))
                            .await?;
                    }

                    return Ok(PromiseOrRejection::Rejection(rejection));
                }

                Ok(PromiseOrRejection::Promise(promise)) => {
                    let current_max_promise = std::mem::replace(&mut max_promise, Promise::empty());
                    let new_max_promise = current_max_promise.merge_with(promise);

                    if promises + 1 >= quorum {
                        return Ok(PromiseOrRejection::Promise(new_max_promise));
                    } else {
                        promises += 1;
                        max_promise = new_max_promise;
                    }
                }
            }
        }

        panic!("await_accepted_quorum: insufficient pending_responses");
    }

    async fn propose_entry(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        quorum_prime: usize,
        other_nodes: &[NodeOf<S>],
        log_entry: Arc<LogEntryOf<S>>,
    ) -> Result<
        (
            HashSet<NodeIdOf<S>>,
            HashSet<NodeIdOf<S>>,
            FuturesUnordered<
                impl Future<
                    Output = (
                        NodeIdOf<S>,
                        Result<AcceptanceOrRejection<CoordNumOf<C>, LogEntryOf<S>>, ErrorOf<C>>,
                    ),
                >,
            >,
        ),
        AppendError,
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
                let node_id: NodeIdOf<S> = node.id();
                fut.map(move |r| (node_id, r))
            });

        self.await_accepted_quorum(round_num, quorum_prime, accepts)
            .await
    }

    async fn await_accepted_quorum<'a, P>(
        &self,
        round_num: RoundNumOf<C>,
        quorum: usize,
        pending_responses: impl IntoIterator<Item = P>,
    ) -> Result<
        (
            HashSet<NodeIdOf<S>>,
            HashSet<NodeIdOf<S>>,
            FuturesUnordered<P>,
        ),
        AppendError,
    >
    where
        P: Future<
            Output = (
                NodeIdOf<S>,
                Result<AcceptanceOrRejection<CoordNumOf<C>, LogEntryOf<S>>, ErrorOf<C>>,
            ),
        >,
    {
        if quorum == 0 {
            return Ok((
                HashSet::new(),
                HashSet::new(),
                pending_responses.into_iter().collect(),
            ));
        }

        let mut pending_responses: FuturesUnordered<P> = pending_responses.into_iter().collect();

        let mut pending_len = pending_responses.len();
        let mut accepted = HashSet::new();
        let mut rejected_or_failed = HashSet::new();

        let mut conflict = None;

        while let Some((node_id, response)) = pending_responses.next().await {
            pending_len -= 1;

            match response {
                Ok(AcceptanceOrRejection::Acceptance) => {
                    accepted.insert(node_id);

                    if accepted.len() >= quorum {
                        return Ok((accepted, rejected_or_failed, pending_responses));
                    }
                }
                Ok(AcceptanceOrRejection::Rejection(Rejection::Converged {
                    log_entry: Some((coord_num, entry)),
                    ..
                })) => {
                    self.state_keeper
                        .commit_entry(round_num, coord_num, entry)
                        .await?;

                    return Err(AppendError::Converged);
                }
                rejection_or_failure => {
                    if let Ok(AcceptanceOrRejection::Rejection(Rejection::Conflict { coord_num })) =
                        rejection_or_failure
                    {
                        conflict = conflict
                            .map(|c| std::cmp::max(c, coord_num))
                            .or(Some(coord_num));
                    }

                    if accepted.len() + pending_len < quorum {
                        if let Some(coord_num) = conflict {
                            self.state_keeper.observe_coord_num(coord_num).await?;
                        }

                        return Err(AppendError::NoQuorum);
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
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        log_entry: Arc<LogEntryOf<S>>,
        mut other_nodes: Vec<NodeOf<S>>,
        accepted: HashSet<NodeIdOf<S>>,
        rejected_or_failed: HashSet<NodeIdOf<S>>,
        mut pending_acceptances: FuturesUnordered<
            impl 'static
                + Future<
                    Output = (
                        NodeIdOf<S>,
                        Result<AcceptanceOrRejection<CoordNumOf<C>, LogEntryOf<S>>, ErrorOf<C>>,
                    ),
                >,
        >,
    ) -> Result<Commit<S, RoundNumOf<C>>, AppendError> {
        let accepted = other_nodes
            .drain_filter(|n| accepted.contains(&n.id()))
            .collect::<Vec<_>>();

        let rejected_or_failed = other_nodes
            .drain_filter(|n| rejected_or_failed.contains(&n.id()))
            .collect::<Vec<_>>();

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
            .send_commit_by_id(&accepted, round_num, coord_num, cle_id)
            .into_iter()
            .for_each(|(_n, f)| commits.submit(f.map(|_| ())));

        self.communicator
            .borrow_mut()
            .send_commit(
                &rejected_or_failed,
                round_num,
                coord_num,
                Arc::clone(&log_entry_for_others),
            )
            .into_iter()
            .for_each(|(_n, f)| commits.submit(f.map(|_| ())));

        let commits_for_pending = commits.clone();
        let pending = async move {
            while let Some((node_id, response)) = pending_acceptances.next().await {
                let node = pending_nodes_by_id.remove(&node_id).expect("pending node");

                match response {
                    Ok(AcceptanceOrRejection::Acceptance) => {
                        let commit = self
                            .communicator
                            .borrow_mut()
                            .send_commit_by_id(&[node], round_num, coord_num, cle_id)
                            .into_iter()
                            .next()
                            .expect("Expected exactly one element.")
                            .1
                            .map(|_| ());

                        commits_for_pending.submit(commit);
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

                        commits_for_pending.submit(commit);
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
