//! Defines the [`RetryPolicy`] trait and related types.

use async_trait::async_trait;

use crate::append::AppendError;
use crate::error::ShutDown;
use crate::error::ShutDownOr;
use crate::invocation::Invocation;

/// Policy that determines whether an append should be retried.
#[async_trait]
pub trait RetryPolicy: 'static {
    /// Parametrization of the paxakos algorithm.
    type Invocation: Invocation;

    /// Type of error produced by this policy.
    type Error;

    /// Union of `Self::Error` and `ShutDown`.
    ///
    /// See [`Node::append_static`][crate::Node::append_static].
    // TODO default to ShutDownOr<Self::Error> (https://github.com/rust-lang/rust/issues/29661)
    type StaticError;

    /// Determines wheter another attempt to append should be made.
    ///
    /// The given `error` is the reason the latest attempt failed. Returning
    /// `Ok(())` implies that another attempt should be made.
    async fn eval(&mut self, error: AppendError<Self::Invocation>) -> Result<(), Self::Error>;
}

#[async_trait]
impl<I: Invocation, E: 'static, F: 'static> RetryPolicy
    for Box<dyn RetryPolicy<Invocation = I, Error = E, StaticError = F> + Send>
{
    type Invocation = I;
    type Error = E;
    type StaticError = F;

    async fn eval(&mut self, error: AppendError<Self::Invocation>) -> Result<(), Self::Error> {
        let p = &mut **self;
        p.eval(error).await
    }
}

/// Implementation of [`RetryPolicy`] that never retries.
#[derive(Clone, Copy, Debug)]
pub struct DoNotRetry<I>(crate::util::PhantomSend<I>);

impl<I> DoNotRetry<I> {
    /// Constructs a new `DoNoRetry` policy.
    pub fn new() -> Self {
        Self(crate::util::PhantomSend::new())
    }
}

impl<I> Default for DoNotRetry<I> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[async_trait]
impl<I: Invocation> RetryPolicy for DoNotRetry<I> {
    type Invocation = I;
    type Error = AppendError<Self::Invocation>;
    type StaticError = AppendError<Self::Invocation>;

    async fn eval(&mut self, err: AppendError<Self::Invocation>) -> Result<(), Self::Error> {
        Err(err)
    }
}

/// Append was not retried.
#[derive(thiserror::Error)]
#[error("append was aborted")]
pub struct AbortedError<I: Invocation>(AppendError<I>);

impl<I: Invocation> std::fmt::Debug for AbortedError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AbortedError").field(&self.0).finish()
    }
}

impl<I: Invocation> From<ShutDown> for AbortedError<I> {
    fn from(shut_down: ShutDown) -> Self {
        Self(shut_down.into())
    }
}

impl<I: Invocation> From<ShutDownOr<AbortedError<I>>> for AbortedError<I> {
    fn from(err: ShutDownOr<AbortedError<I>>) -> Self {
        match err {
            ShutDownOr::Other(err) => err,
            ShutDownOr::ShutDown => Self::from(ShutDown),
        }
    }
}

#[cfg(feature = "backoff")]
pub use cfg_backoff::RetryWithBackoff;

#[cfg(feature = "backoff")]
mod cfg_backoff {
    use std::marker::PhantomData;

    use async_trait::async_trait;
    use backoff::backoff::Backoff;
    use backoff::exponential::ExponentialBackoff;

    use crate::append::AppendArgs;
    use crate::append::AppendError;
    use crate::invocation::Invocation;

    use super::AbortedError;
    use super::RetryPolicy;

    /// Retry policy basod on a [`Backoff`] implementation.
    pub struct RetryWithBackoff<B, I>(B, PhantomData<I>);

    impl<C, I> Default for RetryWithBackoff<ExponentialBackoff<C>, I>
    where
        C: backoff::Clock + Default + Send + 'static,
        I: Invocation + Send,
    {
        fn default() -> Self {
            Self(ExponentialBackoff::default(), Default::default())
        }
    }

    impl<B: Backoff, I> From<B> for RetryWithBackoff<B, I> {
        fn from(backoff: B) -> Self {
            Self(backoff, std::marker::PhantomData)
        }
    }

    impl<C, I> From<ExponentialBackoff<C>> for AppendArgs<I, RetryWithBackoff<ExponentialBackoff<C>, I>>
    where
        C: backoff::Clock + Send + 'static,
        I: Invocation + Send,
    {
        fn from(backoff: ExponentialBackoff<C>) -> Self {
            RetryWithBackoff::from(backoff).into()
        }
    }

    #[async_trait]
    impl<B: Backoff + Send + 'static, I: Invocation + Send> RetryPolicy for RetryWithBackoff<B, I> {
        type Invocation = I;
        type Error = AbortedError<I>;
        type StaticError = AbortedError<I>;

        async fn eval(&mut self, err: AppendError<Self::Invocation>) -> Result<(), Self::Error> {
            if let Some(d) = self.0.next_backoff() {
                futures_timer::Delay::new(d).await;
                Ok(())
            } else {
                Err(AbortedError(err))
            }
        }
    }
}
