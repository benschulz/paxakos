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

#[derive(Debug)]
pub struct Commit<S: State> {
    receiver: oneshot::Receiver<Result<OutcomeOf<S>, CommitError<S>>>,
}

impl<S: State> Commit<S> {
    pub(crate) fn new(receiver: oneshot::Receiver<Result<OutcomeOf<S>, CommitError<S>>>) -> Self {
        Self { receiver }
    }

    pub(crate) fn ready(result: Result<OutcomeOf<S>, CommitError<S>>) -> Self {
        let (send, recv) = futures::channel::oneshot::channel();

        let _ = send.send(result);

        Self { receiver: recv }
    }
}

impl<S: State> Future for Commit<S> {
    type Output = Result<OutcomeOf<S>, CommitError<S>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.receiver
            .poll_unpin(cx)
            .map(|r| r.map_err(|_| CommitError::ShutDown).and_then(|v| v))
    }
}
