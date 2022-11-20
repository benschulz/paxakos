use std::future::Future;
use std::ops::RangeBounds;

use num_traits::One;
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

pub fn usize_remainder<N: Number>(dividend: N, divisor: N) -> usize {
    try_usize_remainder(dividend, divisor)
        .unwrap_or_else(|| panic!("Out of usize range: {}", dividend % divisor))
}

pub fn try_usize_remainder<N: Number>(dividend: N, divisor: N) -> Option<usize> {
    std::convert::TryInto::<usize>::try_into(dividend % divisor).ok()
}

pub fn from_usize<N: Number>(n: usize, quantity: &str) -> N {
    N::try_from(n).unwrap_or_else(|_| panic!("{quantity} out of range"))
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

#[derive(Copy, Debug)]
pub struct PhantomSend<T>(std::marker::PhantomData<T>);

unsafe impl<T> Send for PhantomSend<T> {}

impl<T> PhantomSend<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for PhantomSend<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> Clone for PhantomSend<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

pub struct NumberIter<N: Number> {
    next: Option<N>,
    last: N,
}

impl<N: Number> NumberIter<N> {
    pub fn from_range<R: RangeBounds<N>>(range: R) -> Self {
        let next = match range.start_bound() {
            std::ops::Bound::Included(n) => Some(*n),
            std::ops::Bound::Excluded(n) => {
                if *n == N::min_value() {
                    None
                } else {
                    Some(*n - One::one())
                }
            }
            std::ops::Bound::Unbounded => Some(N::min_value()),
        };

        let last = match range.end_bound() {
            std::ops::Bound::Included(n) => Some(*n),
            std::ops::Bound::Excluded(n) => {
                if *n == N::min_value() {
                    None
                } else {
                    Some(*n - One::one())
                }
            }
            std::ops::Bound::Unbounded => Some(N::max_value()),
        };

        match (next, last) {
            (Some(n), Some(last)) if n <= last => NumberIter { next, last },
            _ => NumberIter {
                next: None,
                last: N::min_value(),
            },
        }
    }
}

impl<N: Number> std::iter::Iterator for NumberIter<N> {
    type Item = N;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(n) = self.next {
            if n == self.last {
                self.next = None;
            } else {
                self.next = Some(n + One::one());
            }

            Some(n)
        } else {
            None
        }
    }
}
