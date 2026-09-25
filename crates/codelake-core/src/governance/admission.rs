//! Admission: how many queries run and wait, per node, per resource group and
//! per user.
//!
//! A query runs at once when the node, its resource group and its user all
//! have a free running slot; otherwise it waits in the queue when all three
//! have a free queued slot; otherwise it is refused. Queued queries are
//! admitted in arrival order among those that fit: a query held back by its
//! own user's or group's limit does not hold back the others (no head-of-line
//! blocking), and a newcomer never runs while an older query that fits waits,
//! because every change of the counters admits every waiting query that fits
//! before anything else happens.
//!
//! Slots are RAII: a [`Permit`] frees its running slot when dropped, a
//! [`Ticket`] dropped while waiting frees its queued slot. Every counter is
//! changed under one lock, and per-user entries are removed at zero, so the
//! state is bounded by the queries alive.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use regex::Regex;
use tokio::sync::oneshot;

use super::metrics;
use crate::config::GovernanceConfig;

/// Name of the implicit group of queries no resource group matches.
pub const GLOBAL_GROUP: &str = "global";

/// A pattern that must match the whole text.
pub fn whole_match(pattern: &str) -> Result<Regex, regex::Error> {
    Regex::new(&format!("^(?:{pattern})$"))
}

/// Which limit refused a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Node,
    Group(Arc<str>),
    User(String),
}

impl Scope {
    /// Metric label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Group(_) => "resource_group",
            Self::User(_) => "user",
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node => f.write_str("this node"),
            Self::Group(name) => write!(f, "resource group {name}"),
            Self::User(user) => write!(f, "user {user}"),
        }
    }
}

/// A query refused because every queue it could wait in is full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub scope: Scope,
    /// The queued-queries limit that was reached.
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct Slots {
    running: usize,
    queued: usize,
}

impl Slots {
    fn is_empty(self) -> bool {
        self.running == 0 && self.queued == 0
    }
}

#[derive(Debug)]
struct Group {
    name: Arc<str>,
    users: Vec<String>,
    user_pattern: Option<Regex>,
    source_pattern: Option<Regex>,
    max_running: usize,
    max_queued: usize,
    /// A pattern did not compile (config validation refuses that): the group
    /// matches nothing rather than everything.
    broken: bool,
}

impl Group {
    fn matches(&self, user: &str, source: Option<&str>) -> bool {
        if self.broken {
            return false;
        }
        let by_user = (self.users.is_empty() && self.user_pattern.is_none())
            || self.users.iter().any(|u| u == user)
            || self.user_pattern.as_ref().is_some_and(|p| p.is_match(user));
        let by_source = self
            .source_pattern
            .as_ref()
            .is_none_or(|p| source.is_some_and(|s| p.is_match(s)));
        by_user && by_source
    }
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    user: String,
    group: Option<usize>,
    wake: oneshot::Sender<()>,
}

#[derive(Debug, Default)]
struct Counters {
    node: Slots,
    groups: Vec<Slots>,
    users: HashMap<String, Slots>,
    waiters: VecDeque<Waiter>,
    next_waiter: u64,
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    running: usize,
    queued: usize,
    running_per_user: usize,
    queued_per_user: usize,
}

/// See the module documentation.
#[derive(Debug)]
pub struct Admission {
    limits: Limits,
    groups: Vec<Group>,
    counters: Mutex<Counters>,
}

impl Admission {
    /// Builds the controller; patterns were checked by config validation.
    pub fn new(config: &GovernanceConfig) -> Self {
        let groups: Vec<Group> = config
            .resource_groups
            .iter()
            .map(|g| {
                let user_pattern = g.user_pattern.as_deref().map(whole_match).transpose();
                let source_pattern = g.source_pattern.as_deref().map(whole_match).transpose();
                let broken = user_pattern.is_err() || source_pattern.is_err();
                Group {
                    name: Arc::from(g.name.trim()),
                    users: g.users.clone(),
                    user_pattern: user_pattern.ok().flatten(),
                    source_pattern: source_pattern.ok().flatten(),
                    max_running: g.max_running,
                    max_queued: g.max_queued,
                    broken,
                }
            })
            .collect();
        let counters = Counters {
            groups: vec![Slots::default(); groups.len()],
            ..Counters::default()
        };
        let admission = Self {
            limits: Limits {
                running: config.max_running_queries,
                queued: config.max_queued_queries,
                running_per_user: config.max_running_per_user,
                queued_per_user: config.max_queued_per_user,
            },
            groups,
            counters: Mutex::new(counters),
        };
        admission.publish(&admission.lock());
        admission
    }

    fn lock(&self) -> MutexGuard<'_, Counters> {
        // Counters are updated in single steps; a panic elsewhere leaves them whole.
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The resource group of a query: the first that matches, if any.
    pub fn classify(&self, user: &str, source: Option<&str>) -> Option<usize> {
        self.groups.iter().position(|g| g.matches(user, source))
    }

    /// Name of group `group` (`global` for `None`).
    pub fn group_name(&self, group: Option<usize>) -> Arc<str> {
        group
            .and_then(|g| self.groups.get(g))
            .map_or_else(|| Arc::from(GLOBAL_GROUP), |g| Arc::clone(&g.name))
    }

    fn can_run(&self, counters: &Counters, user: &str, group: Option<usize>) -> bool {
        let user_running = counters.users.get(user).map_or(0, |s| s.running);
        counters.node.running < self.limits.running
            && user_running < self.limits.running_per_user
            && group.is_none_or(|g| {
                counters.groups.get(g).map_or(0, |s| s.running)
                    < self.groups.get(g).map_or(0, |d| d.max_running)
            })
    }

    fn queue_refusal(&self, counters: &Counters, user: &str, group: Option<usize>) -> Option<Refusal> {
        if counters.node.queued >= self.limits.queued {
            return Some(Refusal {
                scope: Scope::Node,
                limit: self.limits.queued,
            });
        }
        if let Some(g) = group
            && let Some(definition) = self.groups.get(g)
            && counters.groups.get(g).map_or(0, |s| s.queued) >= definition.max_queued
        {
            return Some(Refusal {
                scope: Scope::Group(Arc::clone(&definition.name)),
                limit: definition.max_queued,
            });
        }
        if counters.users.get(user).map_or(0, |s| s.queued) >= self.limits.queued_per_user {
            return Some(Refusal {
                scope: Scope::User(user.to_owned()),
                limit: self.limits.queued_per_user,
            });
        }
        None
    }

    fn slots<'a>(counters: &'a mut Counters, user: &str, group: Option<usize>) -> [Option<&'a mut Slots>; 3] {
        let Counters {
            node, groups, users, ..
        } = counters;
        let user_slots = users.entry(user.to_owned()).or_default();
        let group_slots = group.and_then(|g| groups.get_mut(g));
        [Some(node), group_slots, Some(user_slots)]
    }

    fn change(counters: &mut Counters, user: &str, group: Option<usize>, change: impl Fn(&mut Slots)) {
        for slots in Self::slots(counters, user, group).into_iter().flatten() {
            change(slots);
        }
        if counters.users.get(user).is_some_and(|s| s.is_empty()) {
            counters.users.remove(user);
        }
    }

    /// Takes a running slot, or a queued one, or refuses.
    pub fn enter(self: &Arc<Self>, user: &str, group: Option<usize>) -> Result<Ticket, Refusal> {
        let mut counters = self.lock();
        let wait = if self.can_run(&counters, user, group) {
            Self::change(&mut counters, user, group, |s| s.running += 1);
            None
        } else {
            if let Some(refusal) = self.queue_refusal(&counters, user, group) {
                return Err(refusal);
            }
            Self::change(&mut counters, user, group, |s| s.queued += 1);
            let (wake, wait) = oneshot::channel();
            let id = counters.next_waiter;
            counters.next_waiter = counters.next_waiter.wrapping_add(1);
            counters.waiters.push_back(Waiter {
                id,
                user: user.to_owned(),
                group,
                wake,
            });
            Some((id, wait))
        };
        self.publish(&counters);
        drop(counters);
        let (waiter, wait) = wait.map_or((None, None), |(id, wait)| (Some(id), Some(wait)));
        Ok(Ticket {
            admission: Arc::clone(self),
            user: user.to_owned(),
            group,
            waiter,
            wait,
            settled: false,
        })
    }

    /// Moves every waiting query that now fits from the queue to running, in
    /// arrival order.
    fn promote(&self, counters: &mut Counters) {
        let mut index = 0;
        while index < counters.waiters.len() {
            let fits = counters
                .waiters
                .get(index)
                .is_some_and(|w| self.can_run(counters, &w.user, w.group));
            if !fits {
                index += 1;
                continue;
            }
            let Some(waiter) = counters.waiters.remove(index) else {
                break;
            };
            Self::change(counters, &waiter.user, waiter.group, |s| {
                s.queued -= 1;
                s.running += 1;
            });
            // The ticket may be gone already; its drop then sees it was
            // promoted and frees the running slot.
            let _ = waiter.wake.send(());
        }
    }

    fn release_running(&self, user: &str, group: Option<usize>) {
        let mut counters = self.lock();
        Self::change(&mut counters, user, group, |s| {
            s.running = s.running.saturating_sub(1);
        });
        self.promote(&mut counters);
        self.publish(&counters);
    }

    /// A ticket dropped before it became a permit.
    fn abandon(&self, waiter: Option<u64>, user: &str, group: Option<usize>) {
        let mut counters = self.lock();
        let position = waiter.and_then(|id| counters.waiters.iter().position(|w| w.id == id));
        match position {
            Some(position) => {
                counters.waiters.remove(position);
                Self::change(&mut counters, user, group, |s| {
                    s.queued = s.queued.saturating_sub(1);
                });
            }
            // Admitted (at once or promoted): the running slot is ours.
            None => {
                Self::change(&mut counters, user, group, |s| {
                    s.running = s.running.saturating_sub(1);
                });
            }
        }
        self.promote(&mut counters);
        self.publish(&counters);
    }

    fn publish(&self, counters: &Counters) {
        // Gauges are f64; counts beyond u32 (never reached) saturate.
        let gauge = |n: usize| u32::try_from(n).map_or(f64::from(u32::MAX), f64::from);
        metrics::slots(None, gauge(counters.node.running), gauge(counters.node.queued));
        for (group, slots) in self.groups.iter().zip(&counters.groups) {
            metrics::slots(Some(&group.name), gauge(slots.running), gauge(slots.queued));
        }
    }

    /// `(running, queued)` on this node.
    pub fn load(&self) -> (usize, usize) {
        let counters = self.lock();
        (counters.node.running, counters.node.queued)
    }

    /// `(running, queued)` of `user`.
    pub fn user_load(&self, user: &str) -> (usize, usize) {
        self.lock()
            .users
            .get(user)
            .map_or((0, 0), |s| (s.running, s.queued))
    }
}

/// A place in the queue, or a running slot already granted. Dropping it
/// before [`Ticket::ready`] finishes gives the place back.
#[derive(Debug)]
pub struct Ticket {
    admission: Arc<Admission>,
    user: String,
    group: Option<usize>,
    waiter: Option<u64>,
    wait: Option<oneshot::Receiver<()>>,
    settled: bool,
}

impl Ticket {
    /// Whether the query waits in the queue.
    pub fn is_queued(&self) -> bool {
        self.wait.is_some()
    }

    /// Waits for a running slot. Cancel-safe: dropping the future drops the
    /// ticket, which leaves the queue.
    pub async fn ready(mut self) -> Permit {
        if let Some(wait) = self.wait.as_mut() {
            // The sender is dropped without a send only by this ticket's own
            // drop, which cannot run while it is being awaited.
            let _ = wait.await;
        }
        self.settled = true;
        Permit {
            admission: Arc::clone(&self.admission),
            user: std::mem::take(&mut self.user),
            group: self.group,
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.settled {
            self.admission.abandon(self.waiter, &self.user, self.group);
        }
    }
}

/// A running slot; dropping it frees the slot and admits whoever fits next.
#[derive(Debug)]
pub struct Permit {
    admission: Arc<Admission>,
    user: String,
    group: Option<usize>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.release_running(&self.user, self.group);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::ResourceGroupConfig;

    fn admission(running: usize, queued: usize, per_user: (usize, usize)) -> Arc<Admission> {
        Arc::new(Admission::new(&GovernanceConfig {
            max_running_queries: running,
            max_queued_queries: queued,
            max_running_per_user: per_user.0,
            max_queued_per_user: per_user.1,
            ..GovernanceConfig::default()
        }))
    }

    fn admitted_now(ticket: &Ticket) -> bool {
        !ticket.is_queued()
    }

    async fn ready_soon(ticket: Ticket) -> Option<Permit> {
        tokio::time::timeout(Duration::from_millis(50), ticket.ready())
            .await
            .ok()
    }

    #[tokio::test]
    async fn node_limit_queues_then_refuses() {
        let a = admission(2, 1, (10, 10));
        let first = a.enter("u", None).unwrap();
        let second = a.enter("v", None).unwrap();
        assert!(admitted_now(&first) && admitted_now(&second));
        let third = a.enter("w", None).unwrap();
        assert!(third.is_queued());
        let refused = a.enter("x", None).unwrap_err();
        assert_eq!(
            refused,
            Refusal {
                scope: Scope::Node,
                limit: 1
            }
        );
        assert_eq!(a.load(), (2, 1));

        let first = first.ready().await;
        drop(first);
        let third = ready_soon(third).await.expect("promoted when a slot freed");
        assert_eq!(a.load(), (2, 0));
        drop((second.ready().await, third));
        assert_eq!(a.load(), (0, 0));
        assert!(a.lock().users.is_empty(), "per-user entries go away at zero");
    }

    #[tokio::test]
    async fn per_user_limit_does_not_block_other_users() {
        let a = admission(10, 10, (1, 5));
        let busy = a.enter("alice", None).unwrap().ready().await;
        let waiting = a.enter("alice", None).unwrap();
        assert!(waiting.is_queued());
        // Bob is not behind Alice's queued query.
        let bob = a.enter("bob", None).unwrap();
        assert!(admitted_now(&bob));
        assert_eq!(a.user_load("alice"), (1, 1));
        drop(busy);
        assert!(ready_soon(waiting).await.is_some());
        drop(bob);
        assert_eq!(a.load(), (0, 0));
    }

    #[tokio::test]
    async fn per_user_queue_limit_refuses_with_the_user() {
        let a = admission(10, 10, (1, 1));
        let _running = a.enter("alice", None).unwrap();
        let _queued = a.enter("alice", None).unwrap();
        let refused = a.enter("alice", None).unwrap_err();
        assert_eq!(refused.scope, Scope::User("alice".into()));
        assert_eq!(refused.limit, 1);
        assert_eq!(refused.scope.to_string(), "user alice");
    }

    #[tokio::test]
    async fn resource_groups_classify_and_limit() {
        let config = GovernanceConfig {
            resource_groups: vec![
                ResourceGroupConfig {
                    name: "etl".into(),
                    users: vec!["dbt".into()],
                    user_pattern: Some("airflow_.*".into()),
                    source_pattern: None,
                    max_running: 1,
                    max_queued: 1,
                },
                ResourceGroupConfig {
                    name: "bi".into(),
                    users: Vec::new(),
                    user_pattern: None,
                    source_pattern: Some("metabase|superset".into()),
                    max_running: 5,
                    max_queued: 0,
                },
            ],
            ..GovernanceConfig::default()
        };
        let a = Arc::new(Admission::new(&config));
        assert_eq!(a.classify("dbt", None), Some(0));
        assert_eq!(a.classify("airflow_x", Some("metabase")), Some(0));
        assert_eq!(a.classify("ana", Some("metabase")), Some(1));
        assert_eq!(a.classify("ana", Some("metabase2")), None, "whole-text match");
        assert_eq!(a.classify("airflow", None), None);
        assert_eq!(&*a.group_name(Some(0)), "etl");
        assert_eq!(&*a.group_name(None), GLOBAL_GROUP);

        let etl = Some(0);
        let first = a.enter("dbt", etl).unwrap();
        assert!(admitted_now(&first));
        let second = a.enter("airflow_1", etl).unwrap();
        assert!(second.is_queued(), "the group allows one running");
        let refused = a.enter("dbt", etl).unwrap_err();
        assert_eq!(refused.scope, Scope::Group(Arc::from("etl")));
        // Outside the group, the node still has room.
        assert!(admitted_now(&a.enter("ana", None).unwrap()));
    }

    #[tokio::test]
    async fn dropping_a_queued_ticket_frees_its_place() {
        let a = admission(1, 1, (10, 10));
        let running = a.enter("u", None).unwrap();
        let queued = a.enter("u", None).unwrap();
        assert!(a.enter("u", None).is_err());
        drop(queued);
        assert_eq!(a.load(), (1, 0));
        let queued = a.enter("u", None).unwrap();
        assert!(queued.is_queued());
        // Canceling the wait (dropping the future) leaves the queue too.
        assert!(ready_soon(queued).await.is_none());
        assert_eq!(a.load(), (1, 0));
        drop(running);
        assert_eq!(a.load(), (0, 0));
    }

    #[tokio::test]
    async fn a_promoted_ticket_dropped_before_use_frees_the_slot() {
        let a = admission(1, 1, (10, 10));
        let running = a.enter("u", None).unwrap();
        let queued = a.enter("v", None).unwrap();
        drop(running);
        // Promoted, never awaited.
        assert_eq!(a.load(), (1, 0));
        drop(queued);
        assert_eq!(a.load(), (0, 0));
    }

    #[tokio::test]
    async fn queue_is_first_come_first_served_among_queries_that_fit() {
        let a = admission(1, 10, (10, 10));
        let running = a.enter("u", None).unwrap();
        let first = a.enter("a", None).unwrap();
        let second = a.enter("b", None).unwrap();
        drop(running);
        let first = ready_soon(first).await.expect("oldest first");
        assert_eq!(a.lock().waiters.len(), 1);
        drop(first);
        assert!(ready_soon(second).await.is_some());
    }

    #[tokio::test]
    async fn zero_queue_refuses_at_once() {
        let a = admission(1, 0, (10, 10));
        let _running = a.enter("u", None).unwrap();
        assert_eq!(a.enter("v", None).unwrap_err().scope, Scope::Node);
    }
}
