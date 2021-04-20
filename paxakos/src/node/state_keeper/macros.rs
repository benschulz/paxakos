#[doc(hidden)]
#[macro_export]
macro_rules! dispatch_state_keeper_req {
    ($self:ident, $name:ident) => {{
        let req = Request::$name;

        let (s, r) = oneshot::channel();

        let mut sender = $self.sender.clone();

        async move {
            sender.send((req, s)).await.map_err(|_| ShutDown)?;

            match r.await.map_err(|_| ShutDown)? {
                Response::$name(r) => Ok(r?),
                _ => unreachable!(),
            }
        }
    }};

    ($self:ident, $name:ident, $args:tt) => {{
        let req = Request::$name $args;

        let (s, r) = oneshot::channel();

        let mut sender = $self.sender.clone();

        async move {
            sender.send((req, s)).await.map_err(|_| ShutDown)?;

            match r.await.map_err(|_| ShutDown)? {
                Response::$name(r) => Ok(r?),
                _ => unreachable!(),
            }
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! emit {
    ($self:ident, $event:expr) => {{
        let event = $event;
        tracing::trace!("Emitting event {:?}.", event);
        let _ = futures::executor::block_on($self.event_emitter.send(event));
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! emit_gaps {
    ($self:ident, $state_round_num:expr) => {{
        let mut gaps = Vec::new();

        let mut last_round = $state_round_num;
        for (&k, v) in $self.pending_commits.iter() {
            if k > last_round + One::one() {
                gaps.push(crate::event::Gap {
                    since: v.0,
                    rounds: (last_round + One::one())..k,
                });
            }

            last_round = k;
        }

        crate::emit!($self, ShutdownEvent::Regular(Event::Gaps(gaps)));
    }};
}
