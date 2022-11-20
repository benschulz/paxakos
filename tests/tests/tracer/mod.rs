use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;

use futures::channel::mpsc;
use futures::stream::StreamExt;
use paxakos::invocation::CoordNumOf;
use paxakos::invocation::Invocation;
use paxakos::invocation::LogEntryIdOf;
use paxakos::invocation::NodeIdOf;
use paxakos::invocation::RoundNumOf;
use streamunordered::StreamUnordered;

use paxakos::CoordNum;
use paxakos::Identifier;
use paxakos::RoundNum;

type EventFor<I> = Event<RoundNumOf<I>, CoordNumOf<I>, LogEntryIdOf<I>>;

pub struct StabilityChecker<I: Invocation> {
    receivers: HashMap<NodeIdOf<I>, mpsc::UnboundedReceiver<EventFor<I>>>,
}

impl<I> StabilityChecker<I>
where
    I: Invocation,
    NodeIdOf<I>: Ord,
{
    pub fn new() -> Self {
        Self {
            receivers: HashMap::new(),
        }
    }

    pub fn tracer(&mut self, node: NodeIdOf<I>) -> Box<dyn paxakos::tracer::Tracer<I>> {
        let (send, recv) = mpsc::unbounded();

        assert!(self.receivers.insert(node, recv).is_none());

        Box::new(Tracer { sender: send })
    }

    pub fn spawn(self) {
        let receivers = self.receivers;

        std::thread::spawn(|| {
            let mut nodes = receivers.keys().copied().collect::<Vec<_>>();
            nodes.sort();

            let mut received_events = Vec::new();
            let mut received_events_indexed: BTreeMap<
                RoundNumOf<I>,
                BTreeMap<NodeIdOf<I>, Vec<_>>,
            > = BTreeMap::new();

            let event_stream = receivers
                .into_iter()
                .map(|(n, r)| r.map(move |e| (n, e)).boxed())
                .collect::<StreamUnordered<_>>();

            futures::executor::block_on(StreamExt::for_each(event_stream, |i| {
                let (n, e) = match i {
                    (streamunordered::StreamYield::Item(i), _) => i,
                    _ => return futures::future::ready(()),
                };

                received_events.push((n, e.clone()));

                match e {
                    Event::Promise(r, _, _) | Event::Accept(r, _, _) => {
                        received_events_indexed
                            .entry(r)
                            .or_default()
                            .entry(n)
                            .or_default()
                            .push(e);
                    }
                    Event::Commit(r, i) => {
                        let events = received_events_indexed
                            .entry(r)
                            .or_default()
                            .entry(n)
                            .or_default();

                        events.push(e);

                        for e in events.clone() {
                            if let Event::Commit(_, i0) = e {
                                if i0 != i {
                                    let timestamp = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs();
                                    let mut trace_file =
                                        std::fs::File::create(format!("trace_{timestamp}.txt"))
                                            .unwrap();

                                    tracing::error!("Node {:?} received inconsistent commit.", n);

                                    writeln!(
                                        trace_file,
                                        "Node {:?} received inconsistent commit.",
                                        n
                                    )
                                    .unwrap();
                                    writeln!(trace_file).unwrap();

                                    let mut log_entry_ids = HashSet::new();

                                    let round_events = received_events_indexed.get(&r).unwrap();

                                    for (n, es) in round_events {
                                        writeln!(trace_file, "  Node {n:?}:").unwrap();

                                        for e in es {
                                            if let Event::Commit(_, i) = e {
                                                log_entry_ids.insert(i);
                                            }

                                            writeln!(trace_file, "   - {e:?}:").unwrap();
                                        }
                                    }

                                    writeln!(trace_file).unwrap();
                                    writeln!(trace_file, "  Relevant Accepts:").unwrap();

                                    let mut coord_nums = HashSet::new();

                                    for n in &nodes {
                                        writeln!(trace_file, "    Node {n:?}:").unwrap();

                                        for e in &round_events[n] {
                                            if let Event::Accept(_, c, i) = e {
                                                if log_entry_ids.contains(i) {
                                                    coord_nums.insert(c);

                                                    writeln!(trace_file, "      - {e:?}").unwrap();
                                                }
                                            }
                                        }
                                    }

                                    writeln!(trace_file).unwrap();
                                    writeln!(trace_file, "  Relevant Promises:").unwrap();

                                    for n in &nodes {
                                        writeln!(trace_file, "    Node {n:?}:").unwrap();

                                        for (n0, e) in &received_events {
                                            if let Event::Promise(_, c, _) = e {
                                                if n0 == n && coord_nums.contains(c) {
                                                    writeln!(trace_file, "      - {e:?}").unwrap();
                                                }
                                            }
                                        }
                                    }

                                    writeln!(trace_file).unwrap();
                                    writeln!(trace_file).unwrap();

                                    for n in &nodes {
                                        writeln!(trace_file, "Node {n:?}:").unwrap();

                                        for (n0, e) in &received_events {
                                            if n0 == n {
                                                writeln!(trace_file, " - {e:?}:").unwrap();
                                            }
                                        }

                                        writeln!(trace_file).unwrap();
                                    }

                                    trace_file.flush().unwrap();
                                    trace_file.sync_all().unwrap();

                                    std::process::exit(1);
                                }
                            }
                        }
                    }
                }

                futures::future::ready(())
            }));
        });
    }
}

#[derive(Debug)]
struct Tracer<I: Invocation> {
    sender: mpsc::UnboundedSender<Event<RoundNumOf<I>, CoordNumOf<I>, LogEntryIdOf<I>>>,
}

impl<I: Invocation> paxakos::tracer::Tracer<I> for Tracer<I> {
    fn record_promise(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        promise: Vec<(RoundNumOf<I>, CoordNumOf<I>, LogEntryIdOf<I>)>,
    ) {
        let _ = self
            .sender
            .unbounded_send(Event::Promise(round_num, coord_num, promise));
    }

    fn record_accept(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry_id: LogEntryIdOf<I>,
    ) {
        let _ = self
            .sender
            .unbounded_send(Event::Accept(round_num, coord_num, log_entry_id));
    }

    fn record_commit(&mut self, round_num: RoundNumOf<I>, log_entry_id: LogEntryIdOf<I>) {
        let _ = self
            .sender
            .unbounded_send(Event::Commit(round_num, log_entry_id));
    }
}

#[derive(Clone, Debug)]
enum Event<R, C, I>
where
    R: RoundNum,
    C: CoordNum,
    I: Identifier,
{
    Promise(R, C, Vec<(R, C, I)>),
    Accept(R, C, I),
    Commit(R, I),
}
