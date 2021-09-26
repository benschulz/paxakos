use std::sync::Arc;

use futures::future::LocalBoxFuture;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::error::AffirmSnapshotError;
use crate::error::Disoriented;
use crate::error::InstallSnapshotError;
use crate::error::PrepareSnapshotError;
use crate::Node;
use crate::NodeHandle;
use crate::NodeStatus;

use super::AppendResultFor;
use super::CommunicatorOf;
use super::InvocationOf;
use super::NodeIdOf;
use super::Participation;
use super::RoundNumOf;
use super::ShutdownOf;
use super::SnapshotFor;
use super::StateOf;

pub struct Shell<N> {
    pub(crate) wrapped: N,
}

impl<N> Shell<N> {
    pub(crate) fn around(wrapped: N) -> Self {
        Self { wrapped }
    }
}

impl<N> Node for Shell<N>
where
    N: Node,
{
    type Invocation = InvocationOf<N>;
    type Communicator = CommunicatorOf<N>;

    type Shutdown = ShutdownOf<N>;

    fn id(&self) -> NodeIdOf<Self> {
        self.wrapped.id()
    }

    fn status(&self) -> NodeStatus {
        self.wrapped.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.wrapped.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<super::EventFor<Self>> {
        self.wrapped.poll_events(cx)
    }

    fn handle(&self) -> NodeHandle<Self::Invocation> {
        self.wrapped.handle()
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, PrepareSnapshotError>> {
        self.wrapped.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), AffirmSnapshotError>> {
        self.wrapped.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), InstallSnapshotError>> {
        self.wrapped.install_snapshot(snapshot)
    }

    fn read_stale(&self) -> LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>> {
        self.wrapped.read_stale()
    }

    fn append<A: ApplicableTo<StateOf<Self>> + 'static, P: Into<AppendArgs<Self::Invocation>>>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.wrapped.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.wrapped.shut_down()
    }
}
