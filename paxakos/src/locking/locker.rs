use std::collections::BinaryHeap;
use std::sync::{atomic, Arc, Weak};
use std::task::Poll;

use futures::channel::{mpsc, oneshot};
use futures::future::{BoxFuture, FutureExt};
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use crate::{applicable::ProjectedOf, communicator::Communicator, node::StateOf};

pub type Result<T> = std::result::Result<T, LockError>;

const LOCK_STATE_HELD: u8 = 1;
const LOCK_STATE_LOST: u8 = 0;

pub trait IntoTake<T> {
    fn into_take(self, duration: std::time::Duration) -> T;
}

pub trait IntoExtend<E> {
    fn into_extend(self, duration: std::time::Duration) -> E;
}

pub trait IntoRelease<R> {
    fn into_release(self) -> R;
}

pub trait LockResult<V = ()> {
    type LockId: Copy + Eq + std::hash::Hash + Send + Sync + Unpin;

    fn into_result(self) -> Result<Locked<Self::LockId, V>>;
}

pub struct Locked<I, V = ()> {
    lock_id: I,
    value: V,
    timeout: std::time::Instant,
}

impl<I: Copy> Locked<I> {
    pub fn new(lock_id: I, timeout: std::time::Instant) -> Self {
        Self {
            lock_id,
            value: (),
            timeout,
        }
    }
}

impl<I: Copy, V> Locked<I, V> {
    pub fn with_value<T>(&self, value: T) -> Locked<I, T> {
        Locked {
            lock_id: self.lock_id,
            value,
            timeout: self.timeout,
        }
    }

    pub fn without_value(&self) -> Locked<I> {
        self.with_value(())
    }
}

#[must_use]
pub struct Lock<V = ()> {
    value: Option<V>,
    state: Arc<atomic::AtomicU8>,
    unlock_sender: Option<oneshot::Sender<()>>,
}

impl<V> Lock<V> {
    pub fn get(&self) -> Option<&V> {
        self.value.as_ref()
    }

    pub fn release(self) {}

    pub fn take(&mut self) -> Option<V> {
        self.value.take()
    }

    pub fn test(&self) -> bool {
        self.state.load(atomic::Ordering::Relaxed) == LOCK_STATE_HELD
    }
}

impl<V> Drop for Lock<V> {
    fn drop(&mut self) {
        let _ = self.unlock_sender.take().unwrap().send(());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    // FIXME
    #[error("generic")]
    Generic,
}

impl<C> From<crate::append::AppendError<C>> for LockError
where
    C: Communicator,
{
    fn from(_: crate::append::AppendError<C>) -> Self {
        LockError::Generic
    }
}

impl From<crate::error::ShutDown> for LockError {
    fn from(_: crate::error::ShutDown) -> Self {
        LockError::Generic
    }
}

impl From<futures::channel::mpsc::SendError> for LockError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        LockError::Generic
    }
}

impl From<futures::channel::oneshot::Canceled> for LockError {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        LockError::Generic
    }
}

#[derive(Clone)]
pub struct Locker<T, V = ()> {
    sender: mpsc::Sender<(T, oneshot::Sender<Result<Lock<V>>>)>,
}

impl<T, V> std::fmt::Debug for Locker<T, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paxakos::locking::Locker")
    }
}

pub struct LockKeeper<N: crate::Node, T, E, R, I: Copy, V> {
    node_handle: crate::node::HandleFor<N>,

    take_receiver: mpsc::Receiver<(T, oneshot::Sender<Result<Lock<V>>>)>,

    taken_or_extended_sender: mpsc::Sender<TakenOrExtended<I>>,
    taken_or_extended_receiver: mpsc::Receiver<TakenOrExtended<I>>,

    unlocks: futures::stream::FuturesUnordered<BoxFuture<'static, I>>,

    appends: futures::stream::FuturesUnordered<BoxFuture<'static, ()>>,

    queue: BinaryHeap<QueuedLock<I>>,

    timer: Option<futures_timer::Delay>,

    _p: std::marker::PhantomData<(E, R, V)>,
}

impl<T, V> Locker<T, V> {
    pub fn new<N, E, R, I>(
        node_handle: crate::node::HandleFor<N>,
    ) -> (Self, LockKeeper<N, T, E, R, I, V>)
    where
        N: crate::Node,
        T: crate::applicable::ApplicableTo<StateOf<N>>,
        E: crate::applicable::ApplicableTo<StateOf<N>>,
        R: crate::applicable::ApplicableTo<StateOf<N>>,
        ProjectedOf<T, StateOf<N>>: LockResult<V, LockId = I>,
        ProjectedOf<E, StateOf<N>>: LockResult<LockId = I>,
        I: Copy,
    {
        let (sender, take_receiver) = mpsc::channel(16);
        let (taken_or_extended_sender, taken_or_extended_receiver) = mpsc::channel(16);

        let locker = Self { sender };
        let keeper = LockKeeper {
            node_handle,

            take_receiver,

            taken_or_extended_sender,
            taken_or_extended_receiver,

            unlocks: futures::stream::FuturesUnordered::new(),

            appends: futures::stream::FuturesUnordered::new(),

            queue: BinaryHeap::new(),

            timer: None,

            _p: std::marker::PhantomData,
        };

        (locker, keeper)
    }

    // TODO args (timeout, extend, retry policy)
    pub async fn take_lock<A>(&mut self, take: A) -> Result<Lock<V>>
    where
        A: IntoTake<T>,
    {
        // TODO timeout
        let take = take.into_take(std::time::Duration::from_secs(3));

        let (send, recv) = oneshot::channel();
        self.sender.send((take, send)).await?;

        recv.await?
    }
}

impl<N, T, E, R, I, V> futures::stream::Stream for LockKeeper<N, T, E, R, I, V>
where
    N: crate::Node,
    T: crate::applicable::ApplicableTo<StateOf<N>> + 'static,
    ProjectedOf<T, StateOf<N>>: LockResult<V, LockId = I>,
    E: crate::applicable::ApplicableTo<StateOf<N>> + Unpin + 'static,
    ProjectedOf<E, StateOf<N>>: LockResult<LockId = I>,
    R: crate::applicable::ApplicableTo<StateOf<N>> + Unpin + 'static,
    I: Copy + std::hash::Hash + Eq + Send + Sync + Unpin + 'static,
    I: IntoExtend<E>,
    I: IntoRelease<R>,
    V: Send + Unpin + 'static,
{
    // TODO generate useful events
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        while let Poll::Ready(Some(toe)) = self.taken_or_extended_receiver.poll_next_unpin(cx) {
            match toe {
                TakenOrExtended::Taken(locked, state, unlock) => {
                    let now = std::time::Instant::now();
                    let lock_id = locked.lock_id;

                    self.queue.push(QueuedLock {
                        lock_id,
                        state,
                        // TODO introduce a policy
                        extend: now + (locked.timeout - now) * 2 / 3,
                    });
                    self.unlocks.push(
                        async move {
                            unlock.await.expect("unwrap unlock sender");

                            lock_id
                        }
                        .boxed(),
                    );
                }
                TakenOrExtended::Extended(locked, state) => {
                    let now = std::time::Instant::now();

                    self.queue.push(QueuedLock {
                        lock_id: locked.lock_id,
                        state,
                        // TODO introduce a policy
                        extend: now + (locked.timeout - now) * 2 / 3,
                    });
                }
            }
        }

        while let Poll::Ready(Some(lock_id)) = self.unlocks.poll_next_unpin(cx) {
            let release = lock_id.into_release();

            let append = self.node_handle.append(release, Default::default());

            self.appends.push(
                async move {
                    let _ = append.await;
                }
                .boxed(),
            );
        }

        // FIXME terminate
        while let Poll::Ready(Some((take, reply))) = self.take_receiver.poll_next_unpin(cx) {
            let mut taken_sender = self.taken_or_extended_sender.clone();

            let append = self.node_handle.append(take, Default::default());

            self.appends.push(
                async move {
                    let (send, recv) = oneshot::channel();

                    let state = Arc::new(atomic::AtomicU8::new(LOCK_STATE_HELD));
                    let downgraded_state = Arc::downgrade(&state);

                    let lock_result = async move {
                        let commit = append.await?;
                        let locked = commit.await?.into_result()?;

                        let taken = locked.without_value();
                        let value = locked.value;

                        taken_sender
                            .send(TakenOrExtended::Taken(taken, downgraded_state, recv))
                            .await?;

                        Ok(value)
                    }
                    .await;

                    let _ = reply.send(lock_result.map(|value| Lock {
                        value: Some(value),
                        state,
                        unlock_sender: Some(send),
                    }));
                }
                .boxed(),
            );
        }

        loop {
            let now = std::time::Instant::now();

            while self.queue.peek().filter(|q| q.extend <= now).is_some() {
                let queued = self.queue.pop().unwrap();

                if let Some(state) = queued.state.upgrade() {
                    let mut extended_sender = self.taken_or_extended_sender.clone();

                    // FIXME
                    let duration = std::time::Duration::from_secs(3);

                    let extend = queued.lock_id.into_extend(duration);
                    let append = self.node_handle.append(extend, Default::default());

                    self.appends.push(
                        async move {
                            let lock_result: Result<()> = async move {
                                let commit = append.await?;
                                let locked = commit.await?.into_result()?;

                                extended_sender
                                    .send(TakenOrExtended::Extended(
                                        locked.without_value(),
                                        queued.state,
                                    ))
                                    .await?;

                                Ok(())
                            }
                            .await;

                            if lock_result.is_err() {
                                state.store(LOCK_STATE_LOST, atomic::Ordering::Relaxed);
                            }
                        }
                        .boxed(),
                    )
                }
            }

            self.timer = self
                .queue
                .peek()
                .map(|q| futures_timer::Delay::new(q.extend - std::time::Instant::now()));

            match self.timer.as_mut().map(|t| t.poll_unpin(cx)) {
                None | Some(Poll::Pending) => break,
                _ => {}
            }
        }

        while let Poll::Ready(Some(_)) = self.appends.poll_next_unpin(cx) {
            // keep going
        }

        Poll::Pending
    }
}

enum TakenOrExtended<I: Copy> {
    Taken(Locked<I>, Weak<atomic::AtomicU8>, oneshot::Receiver<()>),
    Extended(Locked<I>, Weak<atomic::AtomicU8>),
}

struct QueuedLock<I> {
    lock_id: I,
    state: Weak<atomic::AtomicU8>,
    extend: std::time::Instant,
}

impl<I> Eq for QueuedLock<I> {}

impl<I> PartialEq for QueuedLock<I> {
    fn eq(&self, other: &Self) -> bool {
        self.extend.eq(&other.extend)
    }
}

impl<I> Ord for QueuedLock<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.extend.cmp(&other.extend).reverse()
    }
}

impl<I> PartialOrd for QueuedLock<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
