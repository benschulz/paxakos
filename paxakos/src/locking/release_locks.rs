use std::collections::hash_map;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::deco::Decoration;
use crate::error::Disoriented;
use crate::node::builder::NodeBuilder;
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::EventOf;
use crate::node::InvocationOf;
use crate::node::LogEntryOf;
use crate::node::NayOf;
use crate::node::Node;
use crate::node::NodeIdOf;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::YeaOf;
use crate::voting::Voter;

type LockOf<N> = <EventOf<N> as AsLockEvent>::Lock;
type LockIdOf<N> = <LockOf<N> as Lock>::Id;

pub trait AsLockEvent {
    type Lock: Lock;

    fn as_lock_taken(&self) -> Option<&Self::Lock>;

    fn as_lock_released(&self) -> Option<<Self::Lock as Lock>::Id>;
}

pub trait Lock {
    type Id: Copy + Eq + std::hash::Hash + PartialEq;

    fn id(&self) -> Self::Id;

    /// The value returned need not be equal, even for two consecutive calls.
    fn timeout(&self) -> std::time::Instant;
}

pub trait HasLocks {
    type Lock: Lock;
    type Iter: Iterator<Item = Self::Lock>;

    fn locks(&self) -> Self::Iter;
}

pub trait AutoReleaseLocksBuilderExt {
    type Node: Node;
    type Voter: Voter;

    fn release_locks<C, P>(
        self,
        configure: C,
    ) -> NodeBuilder<AutoReleaseLocks<Self::Node, P>, Self::Voter>
    where
        EventOf<Self::Node>: AsLockEvent,
        C: FnOnce(
            AutoReleaseLocksBuilderBlank<Self::Node>,
        ) -> AutoReleaseLocksBuilder<Self::Node, P>,
        P: Fn(LockIdOf<Self::Node>) -> LogEntryOf<Self::Node> + 'static;
}

impl<N, V> AutoReleaseLocksBuilderExt for NodeBuilder<N, V>
where
    N: Node + 'static,
    EventOf<N>: AsLockEvent,
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

    fn release_locks<C, P>(self, configure: C) -> NodeBuilder<AutoReleaseLocks<N, P>, V>
    where
        EventOf<N>: AsLockEvent,
        C: FnOnce(AutoReleaseLocksBuilderBlank<N>) -> AutoReleaseLocksBuilder<N, P>,
        P: Fn(LockIdOf<N>) -> LogEntryOf<N> + 'static,
    {
        let configured = configure(AutoReleaseLocksBuilderBlank(std::marker::PhantomData));

        self.decorated_with(AutoReleaseLocksArgs {
            _node: std::marker::PhantomData,
            _jitter: std::time::Duration::from_millis(0),
            entry_producer: configured.entry_producer,
        })
    }
}

pub struct AutoReleaseLocksBuilderBlank<N>(std::marker::PhantomData<N>)
where
    N: Node,
    EventOf<N>: AsLockEvent;

impl<N> AutoReleaseLocksBuilderBlank<N>
where
    N: Node,
    EventOf<N>: AsLockEvent,
{
    pub fn with<P>(self, entry_producer: P) -> AutoReleaseLocksBuilder<N, P>
    where
        P: Fn(LockIdOf<N>) -> LogEntryOf<N>,
    {
        AutoReleaseLocksBuilder {
            entry_producer,
            _node: std::marker::PhantomData,
        }
    }
}

pub struct AutoReleaseLocksBuilder<N, P>
where
    N: Node,
    EventOf<N>: AsLockEvent,
    P: Fn(LockIdOf<N>) -> LogEntryOf<N>,
{
    entry_producer: P,

    _node: std::marker::PhantomData<N>,
}

pub struct AutoReleaseLocksArgs<N, P>
where
    N: Node,
    EventOf<N>: AsLockEvent,
    P: Fn(LockIdOf<N>) -> LogEntryOf<N>,
{
    _node: std::marker::PhantomData<N>,
    // TODO jitter
    _jitter: std::time::Duration,
    entry_producer: P,
}

pub struct AutoReleaseLocks<N, P>
where
    N: Node,
    EventOf<N>: AsLockEvent,
    P: Fn(LockIdOf<N>) -> LogEntryOf<N>,
{
    decorated: N,
    arguments: AutoReleaseLocksArgs<N, P>,

    queue: BinaryHeap<QueuedLock<LockIdOf<N>>>,
    timeouts: HashMap<LockIdOf<N>, usize>,
    next_timeout_id: usize,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, Option<LockIdOf<N>>>>,
}

impl<N, P> Decoration for AutoReleaseLocks<N, P>
where
    N: Node + 'static,
    EventOf<N>: AsLockEvent,
    P: Fn(LockIdOf<N>) -> LogEntryOf<N> + 'static,
{
    type Arguments = AutoReleaseLocksArgs<N, P>;

    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            arguments,
            queue: BinaryHeap::new(),
            timeouts: HashMap::new(),
            next_timeout_id: 0,
            timer: None,
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

impl<N, P> Node for AutoReleaseLocks<N, P>
where
    N: Node,
    EventOf<N>: AsLockEvent,
    P: Fn(LockIdOf<N>) -> LogEntryOf<N>,
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
        let e = self.decorated.poll_events(cx);

        // TODO queue locks on Init and Install

        if let Poll::Ready(crate::Event::Apply { result, .. }) = &e {
            if let Some(lock) = result.as_lock_taken() {
                let timeout_id = self.next_timeout_id;
                self.next_timeout_id += 1;

                self.timeouts.insert(lock.id(), timeout_id);
                self.queue.push(QueuedLock {
                    lock_id: lock.id(),
                    timeout_id,
                    timeout: lock.timeout(),
                })
            }

            if let Some(lock_id) = result.as_lock_released() {
                self.timeouts.remove(&lock_id);
            }
        }

        while let Poll::Ready(Some(r)) = self.appends.poll_next_unpin(cx) {
            if let Some(lock_id) = r {
                let timeout_id = self.next_timeout_id;
                self.next_timeout_id += 1;

                // TODO retry policy
                let new_timeout = std::time::Instant::now() + std::time::Duration::from_secs(5);
                self.timeouts.insert(lock_id, timeout_id);
                self.queue.push(QueuedLock {
                    lock_id,
                    timeout_id,
                    timeout: new_timeout,
                })
            }
        }

        loop {
            let now = std::time::Instant::now();

            while self.queue.peek().filter(|q| q.timeout <= now).is_some() {
                let queued = self.queue.pop().unwrap();

                if let hash_map::Entry::Occupied(e) = self.timeouts.entry(queued.lock_id) {
                    if *e.get() == queued.timeout_id {
                        let (id, _) = e.remove_entry();

                        let log_entry = (self.arguments.entry_producer)(id);
                        self.appends.push(
                            self.decorated
                                .append(log_entry, Default::default())
                                .map(move |r| r.map(|_| None).unwrap_or(Some(id)))
                                .boxed_local(),
                        );
                    }
                }
            }

            self.timer = self
                .queue
                .peek()
                .map(|q| futures_timer::Delay::new(q.timeout - std::time::Instant::now()));

            match self.timer.as_mut().map(|t| t.poll_unpin(cx)) {
                None | Some(Poll::Pending) => break,
                _ => {}
            }
        }

        e
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

    fn append<A: ApplicableTo<StateOf<Self>> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<Self::Invocation>,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

struct QueuedLock<I> {
    lock_id: I,
    timeout_id: usize,
    timeout: std::time::Instant,
}

impl<I> Eq for QueuedLock<I> {}

impl<I> PartialEq for QueuedLock<I> {
    fn eq(&self, other: &Self) -> bool {
        self.timeout.eq(&other.timeout)
    }
}

impl<I> Ord for QueuedLock<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timeout.cmp(&other.timeout).reverse()
    }
}

impl<I> PartialOrd for QueuedLock<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
