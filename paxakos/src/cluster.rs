use std::collections::VecDeque;
use std::num::NonZeroUsize;

use serde::Deserialize;
use serde::Serialize;

pub trait ClusterLogEntry<N> {
    fn concurrency(&self) -> Option<NonZeroUsize>;

    fn added_nodes(&self) -> Vec<N>;

    fn removed_nodes(&self) -> Vec<N>;
}

/// Utility to ease the implementation of changes in cluster membership, i.e.
/// joining and parting of nodes, and concurrency.
///
/// This utility is most useful to implementors of the [State](crate::State)
/// trait. Whenever a log entry is [applied](crate::State::apply) it should also
/// be [applied](Cluster::apply) to this `Cluster`. If done consistently,
/// [`concurrency_at_offset_one`](Cluster::concurrency_at_offset_one) and
/// [`nodes_at`](Cluster::nodes_at) can be used to trivially implement the
/// methods [`concurrency`](crate::State::concurrency) and
/// [`cluster_at`](crate::State::cluster_at).
///
/// # Membership Changes
///
/// No limits are placed on membership changes. Practically speaking, however,
/// it is wise not to make significant changes quickly. Nodes should not become
/// voting members of a cluster until they can be certain they won't break an
/// earlier commitment.
///
/// # Concurrency Increases
///
/// The implementation is "optimal" in that it takes advantage of concurrency
/// increases the moment they become known.
///
/// | Round                 | r | r + 1 | r + 2 | … |
/// |-----------------------|---|-------|-------|---|
/// | Log Entry             |   | c ≔ 5 |       |   |
/// | Target Concurrency    | 2 |     5 |     5 | … |
/// | Effective Concurrency | 2 |     5 |     5 | … |
///
/// # Concurrency Decreases
///
/// When concurrency is decreased, it cannot be assumed that everyone knows of
/// this reduction until the the previous concurrency window is exhausted.
/// Consider that the concurrency level is `5` in round `r` and is then reduced
/// by a log entry in round `r + 1`. Any node that does not see that log entry
/// in time may think the concurrency level is unchanged, at least until it
/// exhausts the concurrency of `5` in round `r + 5`. Any further append to the
/// log is only allowed while taking the reduction into account. That means that
/// in effect the concurrency level slowly reduces from `5` to `2`, as if the
/// `5` throws a shadow along the axis of concurrency.
///
/// | Round                 | r | r + 1 | r + 2 | r + 3 | r + 4 | r + 5 | … |
/// |-----------------------|---|-------|-------|-------|-------|-------|---|
/// | Log Entry             |   | c ≔ 2 |       |       |       |       |   |
/// | Target Concurrency    | 5 |     2 |     2 |     2 |     2 |     2 | … |
/// | Effective Concurrency | 5 |     5 |     4 |     3 |     2 |     2 | … |
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Cluster<N> {
    /// Effective node set for the next round and up to `concurrency_at_offset
    /// -1` previous rounds.
    nodes_at_offset: Vec<N>,
    /// Concurrency level for the next round and up to `concurrency_at_offset
    /// -1` previous rounds.
    concurrency_at_offset: NonZeroUsize,
    /// Concurrency at the end of a potential concurrency reduction shadow.
    ///
    /// The target concurrency is always lower than or equal to all other levels
    /// of concurrency, those in `log_entries` and `concurrency_at_offset`.
    target_concurrency: NonZeroUsize,
    /// Buffer of up to `concurrency_at_offset - 1` "log entries" that affect
    /// either the effective node set or the concurrency level.
    ///
    /// This buffering is used to implement concurrency reduction shadows, i.e.
    /// the concurrency is monotonically decreasing (non-strictly) in these
    /// entries.
    log_entries: VecDeque<(NonZeroUsize, Vec<N>, Vec<N>)>,
}

impl<N, I> Cluster<N>
where
    N: crate::NodeInfo<Id = I> + Clone,
    I: crate::Identifier + Ord,
{
    /// Returns a new cluster with the given initial state.
    pub fn new(nodes: Vec<N>, concurrency: NonZeroUsize) -> Self {
        Cluster {
            nodes_at_offset: nodes,
            concurrency_at_offset: concurrency,
            target_concurrency: concurrency,
            log_entries: VecDeque::new(),
        }
    }

    pub fn apply<E: ClusterLogEntry<N>>(&mut self, log_entry: &E) {
        let new_concurrency = log_entry.concurrency();
        let mut added_nodes = log_entry.added_nodes();
        let mut removed_nodes = log_entry.removed_nodes();

        if !added_nodes.is_empty() && !removed_nodes.is_empty() {
            added_nodes.sort_by_key(|n| n.id());
            added_nodes.dedup_by_key(|n| n.id());

            removed_nodes.sort_by_key(|n| n.id());
            removed_nodes.dedup_by_key(|n| n.id());

            let any_node_added_and_removed = added_nodes.iter().any(|a| {
                removed_nodes
                    .binary_search_by_key(&a.id(), |n| n.id())
                    .is_ok()
            });

            if any_node_added_and_removed {
                panic!("Simultaneously adding and removing a node is undefined.");
            }
        }

        let prev = self
            .log_entries
            .back()
            .map(|(c, _, _)| *c)
            .unwrap_or(self.concurrency_at_offset);

        if self.target_concurrency < prev {
            // Compact node changes, which is necessary for when concurrency is
            // decreased and then increased again. Since we do it consistently,
            // we only have to compact the changes with one other entry.

            let mut compactable = self
                .log_entries
                .iter_mut()
                .rev()
                .enumerate()
                .take_while(|(i, (c, _, _))| usize::from(prev) + i == usize::from(*c))
                .last()
                .map(|(_, (_, a, r))| (a, r));

            if let Some((ref mut a, ref mut r)) = compactable {
                if !removed_nodes.is_empty() {
                    a.retain(|n| {
                        removed_nodes
                            .binary_search_by_key(&n.id(), |n| n.id())
                            .is_err()
                    });
                }

                let mut added = Vec::new();
                std::mem::swap(&mut added, &mut added_nodes);
                if !added.is_empty() {
                    a.append(&mut added);
                    a.sort_by_key(|n| n.id());
                    a.dedup_by_key(|n| n.id());
                }

                let mut removed = Vec::new();
                std::mem::swap(&mut removed, &mut removed_nodes);
                if !removed.is_empty() {
                    r.append(&mut removed);
                    r.sort_by_key(|n| n.id());
                    r.dedup_by_key(|n| n.id());
                }
            }
        }

        if let Some(new_concurrency) = new_concurrency {
            if new_concurrency > self.target_concurrency {
                self.log_entries
                    .iter_mut()
                    .rev()
                    .enumerate()
                    .take_while(|(i, (c, _, _))| usize::from(*c) > i + 1)
                    .for_each(|(_, (c, _, _))| *c = new_concurrency);
            }

            self.target_concurrency = new_concurrency;
        }

        let next = NonZeroUsize::new(std::cmp::max::<usize>(
            self.target_concurrency.into(),
            usize::from(prev) - 1,
        ))
        .unwrap();

        self.log_entries
            .push_back((next, added_nodes, removed_nodes));

        while self.log_entries.len() >= self.concurrency_at_offset.into() {
            let (c, added, removed) = self.log_entries.pop_front().unwrap();

            self.concurrency_at_offset = c;

            if !added.is_empty() {
                self.nodes_at_offset.extend(added);
                self.nodes_at_offset.sort_by_key(|n| n.id());
                self.nodes_at_offset.dedup_by_key(|n| n.id());
            }

            self.nodes_at_offset
                .retain(|n| removed.binary_search_by_key(&n.id(), |n| n.id()).is_err());
        }
    }

    pub fn concurrency_at_offset_one(&self) -> NonZeroUsize {
        self.log_entries
            .back()
            .map(|(c, _, _)| *c)
            .unwrap_or(self.concurrency_at_offset)
    }

    pub fn nodes_at_offset_one(&self) -> Vec<N> {
        self.nodes_at(NonZeroUsize::new(1).unwrap()).unwrap()
    }

    /// Returns effective set of nodes of the cluster for the given round number
    /// or `None` when the level of concurrency does not allow the node set to
    /// be determined yet.
    pub fn nodes_at(&self, round_offset: NonZeroUsize) -> Option<Vec<N>> {
        if round_offset > self.concurrency_at_offset_one() {
            return None;
        }

        // If `round_offset == 1` the result must be `nodes_at_offset`. If it is greater
        // than `1`, we have to take up to `round_offset - 1` log entries into account.
        // "Up to" because fewer may have accumulated as yet.
        let round_offset: usize = round_offset.into();
        let concurrency: usize = self.concurrency_at_offset.into();
        let take = round_offset - 1;
        // Initially or after concurrency increases `log_entries` may contain less than
        // `concurrency - 1` entries. These need to be accounted for.
        let take = take - std::cmp::min(take, concurrency - self.log_entries.len() - 1);
        let take = std::cmp::min(take, self.log_entries.len());

        let mut nodes = self.log_entries.iter().take(take).fold(
            self.nodes_at_offset.clone(),
            |mut aggregate, (_m, added, removed)| {
                aggregate.extend(added.iter().cloned());
                aggregate.retain(|n| removed.binary_search_by_key(&n.id(), |n| n.id()).is_err());
                aggregate
            },
        );

        nodes.sort_by_key(|n| n.id());
        nodes.dedup_by_key(|n| n.id());

        Some(nodes)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::Cluster;

    fn n(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    impl<N: Clone> super::ClusterLogEntry<N> for (Option<Vec<N>>, Option<Vec<N>>, Option<usize>) {
        fn concurrency(&self) -> Option<NonZeroUsize> {
            self.2.map(NonZeroUsize::new).map(Option::unwrap)
        }

        fn added_nodes(&self) -> Vec<N> {
            self.0.clone().unwrap_or_else(Vec::new)
        }

        fn removed_nodes(&self) -> Vec<N> {
            self.1.clone().unwrap_or_else(Vec::new)
        }
    }

    impl crate::NodeInfo for usize {
        type Id = usize;

        fn id(&self) -> Self::Id {
            *self
        }
    }

    #[test]
    fn test_initial_cluster() {
        let n1 = 1;
        let n2 = 2;

        let cluster = Cluster::new(vec![n1, n2], n(1));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(2)), None);
    }

    #[test]
    fn test_add_node_non_concurrently() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1], n(1));
        cluster.apply(&(Some(vec![n2]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2]));
    }

    #[test]
    fn test_add_node_concurrently() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1], n(2));
        cluster.apply(&(Some(vec![n2]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(3)), None);

        cluster.apply(&(None, None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(3)), None);
    }

    #[test]
    fn test_remove_node_non_concurrently() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1, n2], n(1));
        cluster.apply(&(None, Some(vec![n2]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
    }

    #[test]
    fn test_remove_node_concurrently() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1, n2], n(2));
        cluster.apply(&(None, Some(vec![n2]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), None);

        cluster.apply(&(None, None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), None);
    }

    #[test]
    fn test_concurrency() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1], n(5));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(6)), None);

        cluster.apply(&(Some(vec![n2]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(6)), None);
    }

    #[test]
    fn test_increase_concurrency() {
        let n1 = 1;
        let n2 = 2;

        let mut cluster = Cluster::new(vec![n1], n(3));

        cluster.apply(&(Some(vec![n2]), None, None));
        cluster.apply(&(None, None, Some(5)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(6)), None);

        cluster.apply(&(None, Some(vec![n1]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1, n2]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n2]));
        assert_eq!(cluster.nodes_at(n(6)), None);
    }

    #[test]
    fn test_decrease_concurrency_once() {
        let n1 = 1;
        let n2 = 2;
        let n3 = 3;

        let mut cluster = Cluster::new(vec![n1], n(5));

        cluster.apply(&(None, None, Some(1)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(5)), None);

        cluster.apply(&(Some(vec![n2]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), None);

        cluster.apply(&(Some(vec![n3]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), None);

        cluster.apply(&(None, Some(vec![n1]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), None);

        cluster.apply(&(None, None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n2, n3]));
        assert_eq!(cluster.nodes_at(n(2)), None);
    }

    #[test]
    fn test_decrease_concurrency_twice() {
        let n1 = 1;
        let n2 = 2;
        let n3 = 3;

        let mut cluster = Cluster::new(vec![n1], n(5));

        cluster.apply(&(None, None, Some(3)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(5)), None);

        cluster.apply(&(Some(vec![n2]), None, Some(1)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), None);

        cluster.apply(&(Some(vec![n3]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), None);

        cluster.apply(&(None, Some(vec![n1]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), None);

        cluster.apply(&(None, None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n2, n3]));
        assert_eq!(cluster.nodes_at(n(2)), None);
    }

    #[test]
    fn test_decrease_concurrency_then_increase_again() {
        let n1 = 1;
        let n2 = 2;
        let n3 = 3;

        let mut cluster = Cluster::new(vec![n1], n(5));

        cluster.apply(&(None, None, Some(1)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(5)), None);

        cluster.apply(&(Some(vec![n2]), None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(4)), None);

        cluster.apply(&(Some(vec![n3]), None, Some(5)));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(6)), None);

        cluster.apply(&(None, Some(vec![n1]), None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n2, n3]));
        assert_eq!(cluster.nodes_at(n(6)), None);

        cluster.apply(&(None, None, None));

        assert_eq!(cluster.nodes_at(n(1)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(2)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(3)), Some(vec![n1, n2, n3]));
        assert_eq!(cluster.nodes_at(n(4)), Some(vec![n2, n3]));
        assert_eq!(cluster.nodes_at(n(5)), Some(vec![n2, n3]));
        assert_eq!(cluster.nodes_at(n(6)), None);
    }
}
