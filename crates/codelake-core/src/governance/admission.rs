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
//! A query whose client stopped reading its results does no work: its
//! execution waits for room in the buffer between it and the client. Such a
//! query may give its running slot back ([`Permit::suspend`]) and keep a
//! place among at most `max_suspended_queries` suspended ones; it takes a
//! running slot again ([`Suspension::resume`]) before it produces anything
//! more, ahead of the queries that never ran (it was admitted before them)
//! but, like them, only when the node, its group and its user have room.
//!
//! Slots are RAII: a [`Permit`] frees its running slot when dropped, a
//! [`Ticket`] dropped while waiting frees its queued slot, a [`Suspension`]
//! dropped frees its suspended place. Every counter is changed under one
//! lock, and per-user entries are removed at zero, so the state is bounded by
//! the queries alive.

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
    /// A suspended query taking its slot back: it waits ahead of the queries
    /// that never ran.
    resumed: bool,
    wake: oneshot::Sender<()>,
}

#[derive(Debug, Default)]
struct Counters {
    node: Slots,
    groups: Vec<Slots>,
    users: HashMap<String, Slots>,
    waiters: VecDeque<Waiter>,
    next_waiter: u64,
    /// Queries that gave their running slot back while their client does not
    /// read (node-wide).
    suspended: usize,
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    running: usize,
    queued: usize,
    running_per_user: usize,
    queued_per_user: usize,
    suspended: usize,
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
                suspended: config.max_suspended_queries,
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
                resumed: false,
                wake,
            });
            Some((id, wait))
        };
        self.publish(&counters);
        drop(counters);
        Ok(self.ticket(user, group, wait))
    }

    fn ticket(
        self: &Arc<Self>,
        user: &str,
        group: Option<usize>,
        wait: Option<(u64, oneshot::Receiver<()>)>,
    ) -> Ticket {
        let (waiter, wait) = wait.map_or((None, None), |(id, wait)| (Some(id), Some(wait)));
        Ticket {
            admission: Arc::clone(self),
            user: user.to_owned(),
            group,
            waiter,
            wait,
            settled: false,
        }
    }

    /// Gives a running slot back and keeps a suspended place, unless the node
    /// has `max_suspended_queries` suspended already.
    fn suspend(&self, user: &str, group: Option<usize>) -> bool {
        let mut counters = self.lock();
        if counters.suspended >= self.limits.suspended {
            return false;
        }
        counters.suspended += 1;
        Self::change(&mut counters, user, group, |s| {
            s.running = s.running.saturating_sub(1);
        });
        self.promote(&mut counters);
        self.publish(&counters);
        true
    }

    /// A suspended query asks for a running slot again: it takes one at once
    /// when it fits, or waits ahead of every query that never ran, behind the
    /// suspended ones that asked first. It is never refused: it was admitted
    /// once already.
    fn resume(self: &Arc<Self>, user: &str, group: Option<usize>) -> Ticket {
        let mut counters = self.lock();
        counters.suspended = counters.suspended.saturating_sub(1);
        let wait = if self.can_run(&counters, user, group) {
            Self::change(&mut counters, user, group, |s| s.running += 1);
            None
        } else {
            Self::change(&mut counters, user, group, |s| s.queued += 1);
            let (wake, wait) = oneshot::channel();
            let id = counters.next_waiter;
            counters.next_waiter = counters.next_waiter.wrapping_add(1);
            let position = counters
                .waiters
                .iter()
                .position(|w| !w.resumed)
                .unwrap_or(counters.waiters.len());
            counters.waiters.insert(
                position,
                Waiter {
                    id,
                    user: user.to_owned(),
                    group,
                    resumed: true,
                    wake,
                },
            );
            Some((id, wait))
        };
        self.publish(&counters);
        drop(counters);
        self.ticket(user, group, wait)
    }

    /// A suspension dropped without resuming (the query ended).
    fn release_suspended(&self) {
        let mut counters = self.lock();
        counters.suspended = counters.suspended.saturating_sub(1);
        self.publish(&counters);
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
        metrics::suspended(gauge(counters.suspended));
        for (group, slots) in self.groups.iter().zip(&counters.groups) {
            metrics::slots(Some(&group.name), gauge(slots.running), gauge(slots.queued));
        }
    }

    /// `(running, queued)` on this node.
    pub fn load(&self) -> (usize, usize) {
        let counters = self.lock();
        (counters.node.running, counters.node.queued)
    }

    /// Queries on this node that gave their running slot back while their
    /// client does not read.
    pub fn suspended(&self) -> usize {
        self.lock().suspended
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
            held: true,
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
    /// Still holds the running slot (not given back by [`Permit::suspend`]).
    held: bool,
}

impl Permit {
    /// Gives the running slot back while the query waits for its client, and
    /// admits whoever fits next. Refused — the permit comes back, still
    /// running — when the node has `max_suspended_queries` suspended queries
    /// already (`0` keeps every slot).
    pub fn suspend(mut self) -> Result<Suspension, Self> {
        if !self.admission.suspend(&self.user, self.group) {
            return Err(self);
        }
        self.held = false;
        Ok(Suspension {
            admission: Arc::clone(&self.admission),
            user: std::mem::take(&mut self.user),
            group: self.group,
            settled: false,
        })
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.held {
            self.admission.release_running(&self.user, self.group);
        }
    }
}

/// A query that gave its running slot back while its client does not read
/// ([`Permit::suspend`]). Dropping it frees its suspended place.
#[derive(Debug)]
pub struct Suspension {
    admission: Arc<Admission>,
    user: String,
    group: Option<usize>,
    settled: bool,
}

impl Suspension {
    /// Asks for a running slot again (see [`Admission`]): the ticket is
    /// admitted at once or waits ahead of the queries that never ran. It is
    /// never refused, whatever the queue limits.
    pub fn resume(mut self) -> Ticket {
        self.settled = true;
        self.admission.resume(&self.user, self.group)
    }
}

impl Drop for Suspension {
    fn drop(&mut self) {
        if !self.settled {
            self.admission.release_suspended();
        }
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

    fn with_suspended(running: usize, per_user: usize, suspended: usize) -> Arc<Admission> {
        Arc::new(Admission::new(&GovernanceConfig {
            max_running_queries: running,
            max_running_per_user: per_user,
            max_suspended_queries: suspended,
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

    /// D4: a user's queries whose client went away give their slots back;
    /// the same user's next query runs at once.
    #[tokio::test]
    async fn suspending_frees_the_running_slot_for_the_same_user() {
        let a = with_suspended(32, 2, 8);
        let first = a.enter("app", None).unwrap().ready().await;
        let second = a.enter("app", None).unwrap().ready().await;
        let waiting = a.enter("app", None).unwrap();
        assert!(waiting.is_queued());
        assert_eq!(a.user_load("app"), (2, 1));
        let first = first.suspend().unwrap();
        // The queued query was admitted by the suspension itself.
        assert_eq!(a.user_load("app"), (2, 0));
        assert_eq!(a.suspended(), 1);
        let third = ready_soon(waiting)
            .await
            .expect("admitted when a slot was given back");
        let second = second.suspend().unwrap();
        assert_eq!((a.load(), a.suspended()), ((1, 0), 2));
        drop((first, second));
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
        drop(third);
        assert_eq!(a.load(), (0, 0));
        assert!(a.lock().users.is_empty());
    }

    /// At most `max_suspended_queries` suspended on the node; beyond, a query
    /// keeps its slot, and `0` keeps every slot.
    #[tokio::test]
    async fn the_suspended_cap_keeps_the_slot() {
        let a = with_suspended(4, 4, 1);
        let first = a.enter("u", None).unwrap().ready().await;
        let second = a.enter("u", None).unwrap().ready().await;
        let first = first.suspend().unwrap();
        let second = second.suspend().expect_err("the node has one suspended already");
        assert_eq!((a.load(), a.suspended()), ((1, 0), 1));
        drop(first);
        let second = second.suspend().expect("room again");
        assert_eq!((a.load(), a.suspended()), ((0, 0), 1));
        drop(second);
        assert_eq!(a.suspended(), 0);

        let off = with_suspended(4, 4, 0);
        let permit = off.enter("u", None).unwrap().ready().await;
        let permit = permit.suspend().expect_err("suspension off");
        assert_eq!(off.load(), (1, 0));
        drop(permit);
        assert_eq!(off.load(), (0, 0));
    }

    /// A resumed query runs at once when it fits.
    #[tokio::test]
    async fn resuming_with_room_runs_at_once() {
        let a = with_suspended(4, 4, 4);
        let suspension = a.enter("u", None).unwrap().ready().await.suspend().unwrap();
        let ticket = suspension.resume();
        assert!(!ticket.is_queued());
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
        drop(ticket.ready().await);
        assert_eq!(a.load(), (0, 0));
    }

    /// A resumed query waits for room, ahead of the queries that never ran,
    /// behind the resumed ones that asked first; it is never refused, even
    /// when the queue is full; and it does not hold back another user's query
    /// that fits (no head-of-line blocking).
    #[tokio::test]
    async fn resumed_queries_wait_ahead_of_new_ones() {
        let a = Arc::new(Admission::new(&GovernanceConfig {
            max_running_queries: 2,
            max_queued_queries: 1,
            max_running_per_user: 1,
            max_queued_per_user: 1,
            max_suspended_queries: 4,
            ..GovernanceConfig::default()
        }));
        let ana = a.enter("ana", None).unwrap().ready().await.suspend().unwrap();
        let ana_late = a.enter("ana", None).unwrap().ready().await.suspend().unwrap();
        let ana_now = a.enter("ana", None).unwrap().ready().await;
        let newcomer = a.enter("ana", None).unwrap();
        assert!(newcomer.is_queued());
        assert!(a.enter("ana", None).is_err(), "the queue is full");
        // Resumed despite the full queue; queued ahead of the newcomer.
        let ana = ana.resume();
        let ana_late = ana_late.resume();
        assert!(ana.is_queued() && ana_late.is_queued());
        assert_eq!(a.user_load("ana"), (1, 3));
        // Bob fits: not behind ana's waiting queries.
        let bob = a.enter("bob", None).unwrap();
        assert!(!bob.is_queued());
        drop(bob);
        drop(ana_now);
        let ana = ready_soon(ana).await.expect("the first resumed runs first");
        assert_eq!(a.user_load("ana"), (1, 2));
        drop(ana);
        let ana_late = ready_soon(ana_late).await.expect("then the second resumed");
        drop(ana_late);
        assert!(ready_soon(newcomer).await.is_some(), "then the newcomer");
        assert_eq!((a.load(), a.suspended()), ((0, 0), 0));
    }

    /// A resumed ticket dropped while waiting (the query ended) gives its
    /// queued place back.
    #[tokio::test]
    async fn a_resumed_ticket_dropped_while_waiting_leaves_the_queue() {
        let a = with_suspended(1, 1, 1);
        let suspension = a.enter("u", None).unwrap().ready().await.suspend().unwrap();
        let busy = a.enter("u", None).unwrap().ready().await;
        let ticket = suspension.resume();
        assert_eq!((a.load(), a.suspended()), ((1, 1), 0));
        assert!(ready_soon(ticket).await.is_none());
        assert_eq!(a.load(), (1, 0));
        drop(busy);
        assert_eq!(a.load(), (0, 0));
        assert!(a.lock().waiters.is_empty());
    }

    /// Gauges by name, as a metrics exporter would see them.
    #[derive(Default)]
    struct Gauges(Mutex<HashMap<String, Arc<GaugeCell>>>);

    #[derive(Default)]
    struct GaugeCell(std::sync::atomic::AtomicU64);

    impl ::metrics::GaugeFn for GaugeCell {
        fn increment(&self, _: f64) {}
        fn decrement(&self, _: f64) {}
        fn set(&self, value: f64) {
            self.0.store(value.to_bits(), std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Gauges {
        fn get(&self, name: &str) -> Option<f64> {
            let cells = self.0.lock().unwrap();
            cells
                .get(name)
                .map(|cell| f64::from_bits(cell.0.load(std::sync::atomic::Ordering::SeqCst)))
        }
    }

    impl ::metrics::Recorder for Gauges {
        fn describe_counter(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn register_counter(&self, _: &::metrics::Key, _: &::metrics::Metadata<'_>) -> ::metrics::Counter {
            ::metrics::Counter::noop()
        }
        fn register_gauge(&self, key: &::metrics::Key, _: &::metrics::Metadata<'_>) -> ::metrics::Gauge {
            let mut cells = self.0.lock().unwrap();
            let cell = cells.entry(key.name().to_owned()).or_default();
            ::metrics::Gauge::from_arc(Arc::clone(cell))
        }
        fn register_histogram(
            &self,
            _: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Histogram {
            ::metrics::Histogram::noop()
        }
    }

    /// `codelake_queries_suspended` follows the suspended queries, and the
    /// running gauge the slots they give back and take again.
    #[test]
    fn suspended_queries_are_published() {
        use futures::FutureExt;

        use crate::governance::metrics::{RUNNING, SUSPENDED};

        let gauges = Gauges::default();
        ::metrics::with_local_recorder(&gauges, || {
            let a = with_suspended(4, 4, 4);
            assert_eq!(gauges.get(SUSPENDED), Some(0.0));
            let permit = a.enter("u", None).unwrap().ready().now_or_never().unwrap();
            assert_eq!(gauges.get(RUNNING), Some(1.0));
            let suspension = permit.suspend().unwrap();
            assert_eq!(
                (gauges.get(RUNNING), gauges.get(SUSPENDED)),
                (Some(0.0), Some(1.0))
            );
            let ticket = suspension.resume();
            assert_eq!(
                (gauges.get(RUNNING), gauges.get(SUSPENDED)),
                (Some(1.0), Some(0.0))
            );
            let suspension = ticket.ready().now_or_never().unwrap().suspend().unwrap();
            assert_eq!(gauges.get(SUSPENDED), Some(1.0));
            drop(suspension);
            assert_eq!(
                (gauges.get(RUNNING), gauges.get(SUSPENDED)),
                (Some(0.0), Some(0.0))
            );
        });
    }
}
