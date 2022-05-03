//! Defines the [`Executor`] trait and its [default implementation][StdThread].

use futures::FutureExt;

/// Shorthand to extract `Error` type out of `E`.
pub type ErrorOf<E> = <E as Executor>::Error;

/// The executor used to run the code which applies log entries to the `State`.
pub trait Executor: 'static {
    /// Type of error yielded when a task cannot be executed.
    type Error;

    /// Executes the given task.
    fn execute<F: std::future::Future<Output = ()> + Send + 'static>(
        self,
        task: F,
    ) -> Result<(), Self::Error>;
}

impl<S: futures::task::Spawn + 'static> Executor for S {
    type Error = futures::task::SpawnError;

    fn execute<F: std::future::Future<Output = ()> + Send + 'static>(
        self,
        task: F,
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

impl Executor for StdThread {
    type Error = std::io::Error;

    fn execute<F: std::future::Future<Output = ()> + Send + 'static>(
        self,
        task: F,
    ) -> Result<(), Self::Error> {
        let thread_buidler = std::thread::Builder::new();

        thread_buidler
            .spawn(|| {
                futures::executor::block_on(task);
            })
            .map(|_| ())
    }
}
