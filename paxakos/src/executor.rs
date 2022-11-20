//! Defines the [`Executor`] trait and its [default implementation][StdThread].

use futures::FutureExt;

use crate::invocation::ContextOf;
use crate::invocation::StateOf;
use crate::Invocation;

/// Shorthand to extract `Error` type out of `E`.
pub type ErrorOf<E, I, V, B> = <E as Executor<I, V, B>>::Error;

/// The executor used to run the code which applies log entries to the `State`.
pub trait Executor<I: Invocation, V, B>: 'static {
    /// Type of error yielded when a task cannot be executed.
    type Error;

    /// Executes the given task.
    fn execute<F: std::future::Future<Output = ()> + 'static>(
        self,
        task: crate::node::Task<I, V, B, F>,
    ) -> Result<(), Self::Error>;
}

impl<I, V, B, E> Executor<I, V, B> for E
where
    I: Invocation + Send + 'static,
    StateOf<I>: Send,
    ContextOf<I>: Send,
    V: Send + 'static,
    B: Send + 'static,
    E: futures::task::Spawn + 'static,
{
    type Error = futures::task::SpawnError;

    fn execute<F: std::future::Future<Output = ()> + 'static>(
        self,
        task: crate::node::Task<I, V, B, F>,
    ) -> Result<(), Self::Error> {
        self.spawn_obj(futures::task::FutureObj::from(task.boxed()))
    }
}

/// Executor which spawns a new thread for each task.
pub struct StdThread;

impl Default for StdThread {
    fn default() -> Self {
        Self
    }
}

impl<I, V, B> Executor<I, V, B> for StdThread
where
    I: Invocation + Send + 'static,
    StateOf<I>: Send,
    ContextOf<I>: Send,
    V: Send + 'static,
    B: Send + 'static,
{
    type Error = std::io::Error;

    fn execute<F: std::future::Future<Output = ()> + 'static>(
        self,
        task: crate::node::Task<I, V, B, F>,
    ) -> Result<(), Self::Error> {
        let thread_buidler = std::thread::Builder::new();

        thread_buidler
            .spawn(|| {
                futures::executor::block_on(task);
            })
            .map(|_| ())
    }
}

/// Executor that delegates to `wasm_bindgen_futures::spawn_local`.
#[cfg(feature = "wasm-bindgen-futures")]
pub struct WasmExecutor;

#[cfg(feature = "wasm-bindgen-futures")]
impl<I, V, B> Executor<I, V, B> for WasmExecutor
where
    I: Invocation + 'static,
    V: 'static,
    B: 'static,
{
    type Error = ();

    fn execute<F: std::future::Future<Output = ()> + 'static>(
        self,
        task: crate::node::Task<I, V, B, F>,
    ) -> Result<(), Self::Error> {
        wasm_bindgen_futures::spawn_local(task);

        Ok(())
    }
}
