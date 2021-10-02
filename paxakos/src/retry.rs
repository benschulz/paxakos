//! Defines the [`RetryPolicy`] trait and related types.

use async_trait::async_trait;

use crate::append::AppendError;
use crate::error::BoxError;
use crate::invocation::Invocation;

/// Policy that determines whether an append should be retried.
#[async_trait]
pub trait RetryPolicy {
    /// Parametrization of the paxakos algorithm.
    type Invocation: Invocation;

    /// Determines wheter another attempt to append should be made.
    ///
    /// The given `error` is the reason the latest attempt failed. Returning
    /// `Ok(())` implies that another attempt should be made.
    // TODO it should be possible to be cleverer with the error type
    //      as it is pretty much every failed append will return
    //      `AppendError::Aborted`
    async fn eval(&mut self, error: AppendError<Self::Invocation>) -> Result<(), BoxError>;
}

/// Implementation of [`RetryPolicy`] that never retries.
#[derive(Clone, Copy, Debug)]
pub struct DoNotRetry<I>(crate::util::PhantomSend<I>);

impl<I> DoNotRetry<I> {
    /// Constructs a new `DoNoRetry` policy.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(crate::util::PhantomSend::new())
    }
}

#[async_trait]
impl<I: Invocation> RetryPolicy for DoNotRetry<I> {
    type Invocation = I;

    async fn eval(&mut self, err: AppendError<Self::Invocation>) -> Result<(), BoxError> {
        Err(Box::new(AbortedError(err)))
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
    use crate::error::BoxError;
    use crate::invocation::Invocation;

    use super::AbortedError;
    use super::RetryPolicy;

    /// Retry policy basod on a [`Backoff`] implementation.
    pub struct RetryWithBackoff<B, I>(B, PhantomData<I>);

    #[async_trait]
    impl<B: Backoff + Send, I: Invocation + Send> RetryPolicy for RetryWithBackoff<B, I> {
        type Invocation = I;

        async fn eval(&mut self, err: AppendError<Self::Invocation>) -> Result<(), BoxError> {
            if let Some(d) = self.0.next_backoff() {
                futures_timer::Delay::new(d).await;
                Ok(())
            } else {
                Err(Box::new(AbortedError(err)))
            }
        }
    }

    impl<B: Backoff, I> From<B> for RetryWithBackoff<B, I> {
        fn from(backoff: B) -> Self {
            Self(backoff, std::marker::PhantomData)
        }
    }

    impl<C, I> From<ExponentialBackoff<C>> for AppendArgs<I>
    where
        C: backoff::Clock + Send + 'static,
        I: Invocation + Send,
    {
        fn from(backoff: ExponentialBackoff<C>) -> Self {
            RetryWithBackoff::from(backoff).into()
        }
    }

    impl<B: Backoff + Send + 'static, I: Invocation + Send> From<B>
        for Box<dyn RetryPolicy<Invocation = I> + Send>
    {
        fn from(backoff: B) -> Self {
            Box::new(RetryWithBackoff::from(backoff))
        }
    }
}
