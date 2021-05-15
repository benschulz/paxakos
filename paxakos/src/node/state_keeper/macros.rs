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
