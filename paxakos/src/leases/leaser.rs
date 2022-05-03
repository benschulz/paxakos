use std::collections::BinaryHeap;
use std::sync::atomic;
use std::sync::Arc;
use std::sync::Weak;
use std::task::Poll;
use std::time::Duration;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use crate::applicable::ProjectedOf;
use crate::invocation::Invocation;
use crate::node::StateOf;

pub type Result<T> = std::result::Result<T, LeaseError>;

const LEASE_STATE_HELD: u8 = 1;
const LEASE_STATE_LOST: u8 = 0;

pub trait IntoTake<T> {
    fn into_take(self, duration: std::time::Duration) -> T;
}

pub trait IntoExtend<E> {
    fn into_extend(self, duration: std::time::Duration) -> E;
}

pub trait IntoRelease<R> {
    fn into_release(self) -> R;
}

pub trait LeaseResult<V = ()> {
    type LeaseId: Copy + Eq + std::hash::Hash + Send + Sync + Unpin;

    fn into_result(self) -> Result<Leased<Self::LeaseId, V>>;
}

pub struct Leased<I, V = ()> {
    lease_id: I,
    value: V,
    timeout: instant::Instant,
}

impl<I: Copy> Leased<I> {
    pub fn new(lease_id: I, timeout: instant::Instant) -> Self {
        Self {
            lease_id,
            value: (),
            timeout,
        }
    }
}

impl<I: Copy, V> Leased<I, V> {
    pub fn with_value<T>(&self, value: T) -> Leased<I, T> {
        Leased {
            lease_id: self.lease_id,
            value,
            timeout: self.timeout,
        }
    }

    pub fn without_value(&self) -> Leased<I> {
        self.with_value(())
    }
}

#[must_use]
pub struct Lease<V = ()> {
    value: Option<V>,
    state: Arc<atomic::AtomicU8>,
    release_sender: Option<oneshot::Sender<()>>,
}

impl<V> Lease<V> {
    pub fn get(&self) -> Option<&V> {
        self.value.as_ref()
    }

    pub fn release(self) {}

    pub fn take(&mut self) -> Option<V> {
        self.value.take()
    }

    pub fn test(&self) -> bool {
        self.state.load(atomic::Ordering::Relaxed) == LEASE_STATE_HELD
    }
}

impl<V> Drop for Lease<V> {
    fn drop(&mut self) {
        let _ = self.release_sender.take().unwrap().send(());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    // TODO
    #[error("generic")]
    Generic,
}

impl<I: Invocation> From<crate::append::AppendError<I>> for LeaseError {
    fn from(_: crate::append::AppendError<I>) -> Self {
        LeaseError::Generic
    }
}

impl From<crate::error::ShutDown> for LeaseError {
    fn from(_: crate::error::ShutDown) -> Self {
        LeaseError::Generic
    }
}

impl From<futures::channel::mpsc::SendError> for LeaseError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        LeaseError::Generic
    }
}

impl From<futures::channel::oneshot::Canceled> for LeaseError {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        LeaseError::Generic
    }
}

type LeaseRequest<T, V> = (T, LeaseArgs, oneshot::Sender<Result<Lease<V>>>);

#[derive(Clone)]
pub struct Leaser<T, V = ()> {
    sender: mpsc::Sender<LeaseRequest<T, V>>,
}

impl<T, V> std::fmt::Debug for Leaser<T, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paxakos::leases::leaser::Leaser")
    }
}

pub struct LeaseArgs {
    pub initial_duration: Duration,
    pub extension_threshold: Duration,
    pub extension_duration: Duration,
}

impl From<Duration> for LeaseArgs {
    fn from(d: Duration) -> Self {
        Self {
            initial_duration: d,
            extension_threshold: d / 4,
            extension_duration: d,
        }
    }
}

pub struct LeaseKeeper<N: crate::Node, T, E, R, I: Copy, V> {
    node_handle: crate::node::HandleFor<N>,

    take_receiver: mpsc::Receiver<LeaseRequest<T, V>>,

    taken_or_extended_sender: mpsc::Sender<TakenOrExtended<I>>,
    taken_or_extended_receiver: mpsc::Receiver<TakenOrExtended<I>>,

    releases: futures::stream::FuturesUnordered<BoxFuture<'static, I>>,

    appends: futures::stream::FuturesUnordered<BoxFuture<'static, ()>>,

    queue: BinaryHeap<QueuedLease<I>>,

    timer: Option<futures_timer::Delay>,

    _p: std::marker::PhantomData<(E, R, V)>,
}

impl<T, V> Leaser<T, V> {
    pub fn new<N, E, R, I>(
        node_handle: crate::node::HandleFor<N>,
    ) -> (Self, LeaseKeeper<N, T, E, R, I, V>)
    where
        N: crate::Node,
        T: crate::applicable::ApplicableTo<StateOf<N>>,
        E: crate::applicable::ApplicableTo<StateOf<N>>,
        R: crate::applicable::ApplicableTo<StateOf<N>>,
        ProjectedOf<T, StateOf<N>>: LeaseResult<V, LeaseId = I>,
        ProjectedOf<E, StateOf<N>>: LeaseResult<LeaseId = I>,
        I: Copy,
    {
        let (sender, take_receiver) = mpsc::channel(16);
        let (taken_or_extended_sender, taken_or_extended_receiver) = mpsc::channel(16);

        let leaser = Self { sender };
        let keeper = LeaseKeeper {
            node_handle,

            take_receiver,

            taken_or_extended_sender,
            taken_or_extended_receiver,

            releases: futures::stream::FuturesUnordered::new(),

            appends: futures::stream::FuturesUnordered::new(),

            queue: BinaryHeap::new(),

            timer: None,

            _p: std::marker::PhantomData,
        };

        (leaser, keeper)
    }

    pub async fn take_lease<A, P>(&mut self, take: A, args: P) -> Result<Lease<V>>
    where
        A: IntoTake<T>,
        P: Into<LeaseArgs>,
    {
        let args = args.into();
        let take = take.into_take(args.initial_duration);

        let (send, recv) = oneshot::channel();
        self.sender.send((take, args, send)).await?;

        recv.await?
    }
}

impl<N, T, E, R, I, V> futures::stream::Stream for LeaseKeeper<N, T, E, R, I, V>
where
    N: crate::Node,
    T: crate::applicable::ApplicableTo<StateOf<N>> + 'static,
    ProjectedOf<T, StateOf<N>>: LeaseResult<V, LeaseId = I>,
    E: crate::applicable::ApplicableTo<StateOf<N>> + Unpin + 'static,
    ProjectedOf<E, StateOf<N>>: LeaseResult<LeaseId = I>,
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
                TakenOrExtended::Taken(leased, args, state, release) => {
                    let lease_id = leased.lease_id;

                    self.queue.push(QueuedLease {
                        lease_id,
                        state,
                        extend: leased.timeout - args.extension_threshold,
                        args,
                    });
                    self.releases.push(
                        async move {
                            release.await.expect("unwrap release sender");

                            lease_id
                        }
                        .boxed(),
                    );
                }
                TakenOrExtended::Extended(leased, args, state) => {
                    self.queue.push(QueuedLease {
                        lease_id: leased.lease_id,
                        state,
                        extend: leased.timeout - args.extension_threshold,
                        args,
                    });
                }
            }
        }

        while let Poll::Ready(Some(lease_id)) = self.releases.poll_next_unpin(cx) {
            let release = lease_id.into_release();

            let append = self.node_handle.append(release, ());

            self.appends.push(
                async move {
                    let _ = append.await;
                }
                .boxed(),
            );
        }

        // TODO terminate
        while let Poll::Ready(Some((take, args, reply))) = self.take_receiver.poll_next_unpin(cx) {
            let mut taken_sender = self.taken_or_extended_sender.clone();

            let append = self.node_handle.append(take, ());

            self.appends.push(
                async move {
                    let (send, recv) = oneshot::channel();

                    let state = Arc::new(atomic::AtomicU8::new(LEASE_STATE_HELD));
                    let downgraded_state = Arc::downgrade(&state);

                    let lease_result = async move {
                        let commit = append.await?;
                        let leased = commit.await?.into_result()?;

                        let taken = leased.without_value();
                        let value = leased.value;

                        taken_sender
                            .send(TakenOrExtended::Taken(taken, args, downgraded_state, recv))
                            .await?;

                        Ok(value)
                    }
                    .await;

                    let _ = reply.send(lease_result.map(|value| Lease {
                        value: Some(value),
                        state,
                        release_sender: Some(send),
                    }));
                }
                .boxed(),
            );
        }

        loop {
            let now = instant::Instant::now();

            while self.queue.peek().filter(|q| q.extend <= now).is_some() {
                let queued = self.queue.pop().unwrap();

                if let Some(state) = queued.state.upgrade() {
                    let mut extended_sender = self.taken_or_extended_sender.clone();

                    let duration = queued.args.extension_duration;

                    let extend = queued.lease_id.into_extend(duration);
                    let append = self.node_handle.append(extend, ());

                    self.appends.push(
                        async move {
                            let lease_result: Result<()> = async move {
                                let commit = append.await?;
                                let leased = commit.await?.into_result()?;

                                extended_sender
                                    .send(TakenOrExtended::Extended(
                                        leased.without_value(),
                                        queued.args,
                                        queued.state,
                                    ))
                                    .await?;

                                Ok(())
                            }
                            .await;

                            if lease_result.is_err() {
                                state.store(LEASE_STATE_LOST, atomic::Ordering::Relaxed);
                            }
                        }
                        .boxed(),
                    )
                }
            }

            self.timer = self
                .queue
                .peek()
                .map(|q| futures_timer::Delay::new(q.extend - now));

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
    Taken(
        Leased<I>,
        LeaseArgs,
        Weak<atomic::AtomicU8>,
        oneshot::Receiver<()>,
    ),
    Extended(Leased<I>, LeaseArgs, Weak<atomic::AtomicU8>),
}

struct QueuedLease<I> {
    lease_id: I,
    state: Weak<atomic::AtomicU8>,
    extend: instant::Instant,
    args: LeaseArgs,
}

impl<I> Eq for QueuedLease<I> {}

impl<I> PartialEq for QueuedLease<I> {
    fn eq(&self, other: &Self) -> bool {
        self.extend.eq(&other.extend)
    }
}

impl<I> Ord for QueuedLease<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.extend.cmp(&other.extend).reverse()
    }
}

impl<I> PartialOrd for QueuedLease<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
