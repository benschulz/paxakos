use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use futures::channel::oneshot;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};

use crate::error::CommitError;
use crate::state::{OutcomeOf, State};

#[derive(Clone)]
pub struct Commits {
    waker: std::task::Waker,
    inner: Rc<CommitsInner>,
}

struct CommitsInner {
    submitted: RefCell<Vec<LocalBoxFuture<'static, ()>>>,
    active: RefCell<FuturesUnordered<LocalBoxFuture<'static, ()>>>,
}

impl Commits {
    pub fn new() -> Self {
        Self {
            waker: futures::task::noop_waker(),
            inner: Rc::new(CommitsInner {
                submitted: RefCell::new(Vec::new()),
                active: RefCell::new(FuturesUnordered::new()),
            }),
        }
    }

    pub fn submit<F: 'static + Future<Output = ()>>(&self, commit: F) {
        self.inner.submitted.borrow_mut().push(commit.boxed_local());

        self.waker.wake_by_ref();
    }

    pub fn poll_next(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<()>> {
        if !cx.waker().will_wake(&self.waker) {
            self.waker = cx.waker().clone();
        }

        let mut result = self.inner.active.borrow_mut().poll_next_unpin(cx);

        while {
            let mut submitted = self.inner.submitted.borrow_mut();

            match submitted.is_empty() {
                true => false,
                false => {
                    self.inner.active.borrow_mut().extend(submitted.drain(..));

                    true
                }
            }
        } {
            result = self.inner.active.borrow_mut().poll_next_unpin(cx);
        }

        result
    }
}

impl std::fmt::Debug for Commits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Commits {{ submitted: {}, active: {} }}",
            self.inner.submitted.borrow().len(),
            self.inner.active.borrow().len(),
        )
    }
}

/// Converged on log entry waiting to be applied.
///
/// A log entry may not immediately be applied, even when it is already known to
/// have been inserted at a specific place in the distributed. Usually because
/// there is a gap in the log. A `Commit` is a `Future` that represents such a
/// log entry. Once the entry is applied, the future will become ready.
#[derive(Debug)]
pub struct Commit<S: State, R> {
    round_num: R,
    receiver: oneshot::Receiver<(R, OutcomeOf<S>)>,
}

impl<S: State, R: Copy> Commit<S, R> {
    pub(crate) fn new(round_num: R, receiver: oneshot::Receiver<(R, OutcomeOf<S>)>) -> Self {
        Self {
            round_num,
            receiver,
        }
    }

    pub(crate) fn ready(round_num: R, outcome: OutcomeOf<S>) -> Self {
        let (send, recv) = futures::channel::oneshot::channel();

        let _ = send.send((round_num, outcome));

        Self {
            round_num,
            receiver: recv,
        }
    }

    /// The round this commit is slated for.
    ///
    /// This information is useful when, in a concurrent setting, another log
    /// entry is to be applied in a round later than the log entry underlying
    /// this commit.
    pub fn round_num(&self) -> R {
        self.round_num
    }
}

// TODO ShutDown is the only reachable error variant
impl<S, R> Future for Commit<S, R>
where
    S: State,
    R: crate::RoundNum,
{
    type Output = Result<OutcomeOf<S>, CommitError<S>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.receiver.poll_unpin(cx).map(|r| {
            r.map(|(_, outcome)| outcome)
                .map_err(|_| CommitError::ShutDown)
        })
    }
}
