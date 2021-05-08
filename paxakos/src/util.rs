use std::future::Future;

use pin_project::pin_project;

use crate::Number;

pub fn usize_delta<N: Number>(minuend: N, subtrahend: N) -> usize {
    try_usize_delta(minuend, subtrahend)
        .unwrap_or_else(|| panic!("Out of usize range: {}", minuend - subtrahend))
}

pub fn try_usize_delta<N: Number>(minuend: N, subtrahend: N) -> Option<usize> {
    assert!(minuend >= subtrahend);

    std::convert::TryInto::<usize>::try_into(minuend - subtrahend).ok()
}

pub fn try_usize_sub<N: Number>(minuend: N, subtrahend: usize) -> Option<N> {
    let subtrahend =
        N::try_from(subtrahend).unwrap_or_else(|_| panic!("Out of range: {}", subtrahend));

    if minuend >= subtrahend {
        Some(minuend - subtrahend)
    } else {
        None
    }
}

#[pin_project]
pub struct Race<A, B, T>
where
    A: Future<Output = T>,
    B: Future<Output = T>,
{
    #[pin]
    first: A,
    #[pin]
    second: B,
}

impl<A, B, T> Race<A, B, T>
where
    A: Future<Output = T>,
    B: Future<Output = T>,
{
    pub fn between(first: A, second: B) -> Self {
        Race { first, second }
    }
}

impl<A, B, T> Future for Race<A, B, T>
where
    A: Future<Output = T>,
    B: Future<Output = T>,
{
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();

        match this.first.poll(cx) {
            std::task::Poll::Pending => this.second.poll(cx),
            ready => ready,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PhantomSend<T>(std::marker::PhantomData<T>);

unsafe impl<T> Send for PhantomSend<T> {}

impl<T> PhantomSend<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}
