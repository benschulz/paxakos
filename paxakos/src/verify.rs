#![allow(missing_docs)]

use std::convert::Infallible;
use std::task::Poll;
use std::time::Duration;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::TryFutureExt;
use num_traits::One;
use num_traits::Zero;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::leadership::track::MaybeLeadershipAwareNode;
use crate::node::AppendResultFor;
use crate::node::DelegatingNodeImpl;
use crate::node::EjectionOf;
use crate::node::EventFor;
use crate::node::InvocationOf;
use crate::node::LogEntryOf;
use crate::node::NayOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::StaticAppendResultFor;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::RetryPolicy;
use crate::voting;
use crate::CoordNum;
use crate::Node;
use crate::RoundNum;
use voting::Voter;

pub trait State: crate::State
where
    Self::LogEntry: LogEntry,
{
    type Applicable: ApplicableTo<Self> + Send + 'static;

    fn prepare_verification(&self) -> Self::Applicable;

    fn is_consistent_with(&self, log_entry: &Self::LogEntry) -> Option<bool>;
}

pub trait LogEntry {
    fn is_verification(&self) -> bool;
}

/// Ensure leadership configuration.
pub trait Config
where
    <Self::RetryPolicy as RetryPolicy>::Future: Send,
{
    /// The node type that is decorated.
    type Node: Node;

    /// The applicable that is used to verify consistency.
    type Applicable: ApplicableTo<StateOf<Self::Node>> + 'static;

    /// Type of retry policy to be used.
    ///
    /// See [`retry_policy`][Config::retry_policy].
    type RetryPolicy: RetryPolicy<
            Invocation = InvocationOf<Self::Node>,
            Error = AppendError<InvocationOf<Self::Node>>,
            StaticError = AppendError<InvocationOf<Self::Node>>,
        > + Send;

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    fn leader_round_interval(&self) -> Option<RoundNumOf<Self::Node>> {
        None
    }

    fn round_interval(&self) -> Option<RoundNumOf<Self::Node>> {
        None
    }

    fn leader_time_interval(&self) -> Option<Duration> {
        None
    }

    fn time_interval(&self) -> Option<Duration> {
        None
    }

    /// Creates a retry policy.
    fn retry_policy(&self) -> Self::RetryPolicy;
}

pub trait VerifyBuilderExt<I = ()> {
    type Node: Node + 'static;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>
    where
        <C::RetryPolicy as RetryPolicy>::Future: Send;

    fn verify_consistency<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
        <C::RetryPolicy as RetryPolicy>::Future: Send;
}

impl<I, B> VerifyBuilderExt<I> for B
where
    I: 'static,
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
    B::Node: MaybeLeadershipAwareNode<I> + 'static,
    StateOf<B::Node>: State,
    LogEntryOf<B::Node>: LogEntry,
    NayOf<B::Node>: TryInto<Nay> + Clone,
    EjectionOf<B::Node>: From<Inconsistent>,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>
        = B::DecoratedBuilder<Verify<B::Node, C, I>>
            where <C::RetryPolicy as RetryPolicy>::Future: Send;

    fn verify_consistency<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
        <C::RetryPolicy as RetryPolicy>::Future: Send,
    {
        self.decorated_with(config)
    }
}

pub struct Verify<N, C, I = ()>
where
    N: MaybeLeadershipAwareNode<I>,
    C: Config,
    StateOf<N>: State,
    LogEntryOf<N>: LogEntry,
    <C::RetryPolicy as RetryPolicy>::Future: Send,
{
    decorated: N,
    config: C,

    unverified_rounds: RoundNumOf<N>,
    last_verifified_at: instant::Instant,

    timer: Option<(instant::Instant, futures_timer::Delay)>,

    verification: Option<LocalBoxFuture<'static, Result<(), AppendError<InvocationOf<N>>>>>,

    warned_no_leadership_tracking: bool,

    _p: std::marker::PhantomData<I>,
}

impl<N, C, I> Verify<N, C, I>
where
    N: NodeImpl + MaybeLeadershipAwareNode<I> + 'static,
    C: Config<Node = N>,
    StateOf<N>: State,
    LogEntryOf<N>: LogEntry,
    NayOf<N>: TryInto<Nay> + Clone,
    EjectionOf<N>: From<Inconsistent>,
    <C::RetryPolicy as RetryPolicy>::Future: Send,
{
    fn time_interval(&mut self) -> Option<instant::Duration> {
        match self.decorated.strict_leadership() {
            Some(leadership) => {
                if leadership.first().map(|l| l.leader) == Some(self.id()) {
                    self.config
                        .leader_time_interval()
                        .or_else(|| self.config.time_interval())
                } else {
                    self.config.time_interval()
                }
            }

            None => {
                if self.config.leader_time_interval().is_some()
                    && self.warned_no_leadership_tracking
                {
                    self.warned_no_leadership_tracking = true;

                    tracing::warn!(
                        "A leader interval is configured but leadership is not tracked."
                    );
                }

                self.config.time_interval()
            }
        }
    }

    fn round_interval(&mut self) -> Option<RoundNumOf<N>> {
        match self.decorated.strict_leadership() {
            Some(leadership) => {
                if leadership.first().map(|l| l.leader) == Some(self.id()) {
                    self.config
                        .leader_round_interval()
                        .or_else(|| self.config.round_interval())
                } else {
                    self.config.round_interval()
                }
            }

            None => {
                if self.config.leader_round_interval().is_some()
                    && self.warned_no_leadership_tracking
                {
                    self.warned_no_leadership_tracking = true;

                    tracing::warn!(
                        "A leader interval is configured but leadership is not tracked."
                    );
                }

                self.config.round_interval()
            }
        }
    }

    fn spawn_verify_consistency(&mut self) {
        let handle = self.handle();
        let retry_policy = self.config.retry_policy();

        self.verification = Some(
            async move {
                let verification = handle
                    .read_stale(|s| s.prepare_verification())
                    .await
                    .map_err(|_| AppendError::Disoriented)?;

                handle.append(verification, retry_policy).await?.await?;

                Ok(())
            }
            .boxed_local(),
        );
    }
}

impl<N, C, I> Decoration for Verify<N, C, I>
where
    N: NodeImpl + MaybeLeadershipAwareNode<I> + 'static,
    C: Config<Node = N> + 'static,
    StateOf<N>: State,
    LogEntryOf<N>: LogEntry,
    NayOf<N>: TryInto<Nay> + Clone,
    EjectionOf<N>: From<Inconsistent>,
    <C::RetryPolicy as RetryPolicy>::Future: Send,
{
    type Arguments = C;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        mut arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        arguments.init(&decorated);

        Ok(Self {
            decorated,
            config: arguments,

            unverified_rounds: Zero::zero(),
            last_verifified_at: instant::Instant::now(),

            timer: None,

            verification: None,

            warned_no_leadership_tracking: false,

            _p: std::marker::PhantomData,
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, C, I> Node for Verify<N, C, I>
where
    N: NodeImpl + MaybeLeadershipAwareNode<I> + 'static,
    C: Config<Node = N>,
    StateOf<N>: State,
    LogEntryOf<N>: LogEntry,
    NayOf<N>: TryInto<Nay> + Clone,
    EjectionOf<N>: From<Inconsistent>,
    <C::RetryPolicy as RetryPolicy>::Future: Send,
{
    type Invocation = InvocationOf<N>;
    type Shutdown = <N as Node>::Shutdown;

    fn id(&self) -> NodeIdOf<Self> {
        self.decorated.id()
    }

    fn status(&self) -> crate::NodeStatus {
        self.decorated.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.decorated.participation()
    }

    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> Poll<EventFor<Self>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            self.config.update(event);

            match event {
                crate::Event::Init { state: Some(_), .. }
                | crate::Event::Install { state: Some(_), .. } => {
                    self.unverified_rounds = Zero::zero();
                    self.last_verifified_at = instant::Instant::now();
                }

                crate::Event::Apply { log_entry, .. } => {
                    if log_entry.is_verification() {
                        self.unverified_rounds = Zero::zero();
                        self.last_verifified_at = instant::Instant::now();
                    } else {
                        self.unverified_rounds = self.unverified_rounds + One::one();
                    }
                }

                _ => {}
            }
        }

        if self.status() != NodeStatus::Disoriented && self.verification.is_none() {
            if let Some(interval) = self.time_interval() {
                let due_time = self.last_verifified_at + interval;

                if self.timer.is_none()
                    || self
                        .timer
                        .as_ref()
                        .filter(|(t, _)| *t != due_time)
                        .is_some()
                {
                    self.timer = Some((
                        due_time,
                        futures_timer::Delay::new(due_time - instant::Instant::now()),
                    ));
                }

                if let Some((_t, timer)) = &mut self.timer {
                    if timer.poll_unpin(cx).is_ready() {
                        self.spawn_verify_consistency();
                    }
                }
            }

            if let Some(interval) = self.round_interval() {
                if self.unverified_rounds >= interval {
                    self.spawn_verify_consistency();
                }
            }
        }

        if let Some(verification) = self.verification.as_mut() {
            if let Poll::Ready(result) = verification.poll_unpin(cx) {
                self.verification = None;

                match result {
                    Ok(_) => {
                        // this is a bit hacky: we're preventing ourselves from immediately
                        // starting another verification, but these
                        // values will be set again when the log
                        // entry is applied
                        self.unverified_rounds = Zero::zero();
                        self.last_verifified_at = instant::Instant::now();
                    }

                    Err(AppendError::NoQuorum {
                        abstentions,
                        communication_errors,
                        discards,
                        rejections,
                    }) => {
                        let inconsistency_count = rejections
                            .iter()
                            .filter(|r| (**r).clone().try_into().ok() == Some(Nay::Inconsistent))
                            .count();

                        // check whether we couldn't reach a quorum *mostly*
                        // due to inconsistency with other nodes
                        //
                        // TODO this condition is quite janky, can we do better?
                        if inconsistency_count
                            > (abstentions.len()
                                + communication_errors.len()
                                + discards.len()
                                + rejections.len())
                                / 2
                        {
                            // eject our state to reduce the chance that the
                            // inconsistency is propagated
                            self.verification = Some(
                                self.eject(Inconsistent.into())
                                    .map_ok(|_| ())
                                    .map_err(Into::into)
                                    .boxed_local(),
                            );
                        }
                    }

                    _ => {}
                }
            }
        }

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(&self) -> LocalBoxFuture<'static, SnapshotFor<Self>> {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> LocalBoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale(f)
    }

    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'_, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        self.decorated.append(applicable, args)
    }

    fn append_static<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'static, StaticAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.decorated.append_static(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N, C, I> DelegatingNodeImpl for Verify<N, C, I>
where
    N: NodeImpl + MaybeLeadershipAwareNode<I> + 'static,
    C: Config<Node = N>,
    StateOf<N>: State,
    LogEntryOf<N>: LogEntry,
    NayOf<N>: TryInto<Nay> + Clone,
    EjectionOf<N>: From<Inconsistent>,
    <C::RetryPolicy as RetryPolicy>::Future: Send,
{
    type Delegate = N;

    fn delegate(&self) -> &Self::Delegate {
        &self.decorated
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Consistent;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Inconsistent;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Nay {
    Inconsistent,
    ConsistencyUnknown,
}

#[derive(Default)]
pub struct VerifyVoter<S, R, C>(std::marker::PhantomData<(S, R, C)>);

impl<S, R, C> VerifyVoter<S, R, C> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<S, R, C> Voter for VerifyVoter<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    S::LogEntry: LogEntry,
{
    type State = S;

    type RoundNum = R;
    type CoordNum = C;

    type Yea = Consistent;
    type Nay = Nay;
    type Abstain = Infallible;

    fn contemplate_candidate(
        &mut self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        _candidate: Option<&crate::state::NodeOf<Self::State>>,
        _state: Option<&Self::State>,
    ) -> voting::Decision<(), std::convert::Infallible, Self::Abstain> {
        voting::Decision::Yea(())
    }

    fn contemplate_proposal(
        &mut self,
        _round_num: Self::RoundNum,
        _coord_num: Self::CoordNum,
        log_entry: &crate::state::LogEntryOf<Self::State>,
        _leader: Option<&crate::state::NodeOf<Self::State>>,
        state: Option<&Self::State>,
    ) -> voting::Decision<Self::Yea, Self::Nay, std::convert::Infallible> {
        if let Some(state) = state {
            match state.is_consistent_with(log_entry) {
                Some(true) => voting::Decision::Yea(Consistent),
                Some(false) => voting::Decision::Nay(Nay::Inconsistent),
                None => voting::Decision::Nay(Nay::ConsistencyUnknown),
            }
        } else {
            voting::Decision::Nay(Nay::ConsistencyUnknown)
        }
    }
}
