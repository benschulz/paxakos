use std::borrow::Borrow;
use std::cell::RefCell;
use std::future::Future;

use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

use crate::applicable::Identity;
use crate::applicable::Projection;
use crate::error::ShutDown;
use crate::state::OutcomeOf;
use crate::state::State;

pub struct Commits {
    waker: RefCell<std::task::Waker>,
    submitted: RefCell<Vec<BoxFuture<'static, ()>>>,
    active: RefCell<FuturesUnordered<BoxFuture<'static, ()>>>,
}

impl Commits {
    pub fn new() -> Self {
        Self {
            waker: RefCell::new(futures::task::noop_waker()),
            submitted: RefCell::new(Vec::new()),
            active: RefCell::new(FuturesUnordered::new()),
        }
    }

    pub fn submit<F: 'static + Future<Output = ()> + Send>(&self, commit: F) {
        self.submitted.borrow_mut().push(commit.boxed());

        self.waker.borrow().wake_by_ref();
    }

    pub fn poll_next(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<()>> {
        if !cx.waker().borrow().will_wake(&self.waker.borrow()) {
            *self.waker.borrow_mut() = cx.waker().clone();
        }

        loop {
            self.active
                .borrow_mut()
                .extend(self.submitted.borrow_mut().drain(..));

            let result = self.active.borrow_mut().poll_next_unpin(cx);

            if self.submitted.borrow().is_empty() {
                return result;
            }
        }
    }
}

impl std::fmt::Debug for Commits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Commits {{ submitted: {}, active: {} }}",
            self.submitted.borrow().len(),
            self.active.borrow().len(),
        )
    }
}

/// Converged on log entry waiting to be applied.
///
/// A log entry may not immediately be applied, even when it is already known to
/// have been inserted at a specific place in the distributed log. Usually
/// because there is a gap in the log. A `Commit` is a `Future` that represents
/// such a log entry. Once the entry is applied, the future will become ready.
#[derive(Debug)]
pub struct Commit<S: State, R, P = Identity> {
    round_num: R,
    receiver: oneshot::Receiver<(R, OutcomeOf<S>)>,
    _projection: std::marker::PhantomData<P>,
}

impl<S: State, R: Copy> Commit<S, R> {
    pub(crate) fn new(round_num: R, receiver: oneshot::Receiver<(R, OutcomeOf<S>)>) -> Self {
        Self {
            round_num,
            receiver,
            _projection: std::marker::PhantomData,
        }
    }

    pub(crate) fn ready(round_num: R, outcome: OutcomeOf<S>) -> Self {
        let (send, recv) = futures::channel::oneshot::channel();

        let _ = send.send((round_num, outcome));

        Self {
            round_num,
            receiver: recv,
            _projection: std::marker::PhantomData,
        }
    }

    /// Projects the outcome of applying the log entry.
    pub fn projected<P: Projection<OutcomeOf<S>>>(self) -> Commit<S, R, P> {
        Commit {
            round_num: self.round_num,
            receiver: self.receiver,
            _projection: std::marker::PhantomData,
        }
    }
}

impl<S: State, R: Copy, P> Commit<S, R, P> {
    /// The round this commit is slated for.
    ///
    /// This information is useful when, in a concurrent setting, another log
    /// entry is to be applied in a round later than the log entry underlying
    /// this commit.
    pub fn round_num(&self) -> R {
        self.round_num
    }
}

impl<S, R, P> Future for Commit<S, R, P>
where
    S: State,
    R: crate::RoundNum,
    P: Projection<OutcomeOf<S>>,
{
    type Output = Result<<P as Projection<OutcomeOf<S>>>::Projected, ShutDown>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.receiver.poll_unpin(cx).map(|r| {
            r.map(|(_, outcome)| P::project(outcome))
                .map_err(|oneshot::Canceled| ShutDown)
        })
    }
}
