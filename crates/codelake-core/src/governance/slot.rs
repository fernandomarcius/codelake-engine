//! The running slot of a query that sends its results to a client through a
//! bounded buffer (ADR 0016, amended for D4).
//!
//! When the buffer is full the query's execution waits for the client and
//! does no work. A client that read the results it wanted and went away (a
//! killed process never sends its cancel) would leave the query holding its
//! running slot until it is declared abandoned, and with it a place of its
//! user's `max_running_per_user`. So a [`Slot`] gives the running slot back
//! when the wait for the client lasts longer than `suspend_after`
//! ([`Slot::wait_for_client`]), and takes one again before the query
//! produces anything more ([`Slot::resume`]): a suspended query never runs
//! beyond the limits, it only stops counting while it does nothing. Its
//! memory stays reserved in the pool as before; its deadline, its
//! cancellation and the abandonment of its client still stop it.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use super::admission::{Permit, Suspension};

#[derive(Debug)]
enum Held {
    Running(Permit),
    Suspended(Suspension),
}

/// See the module documentation.
#[derive(Debug)]
pub struct Slot {
    /// `None` only when a [`Slot::resume`] was dropped while it waited (the
    /// query is being stopped): nothing is held any more.
    held: Option<Held>,
}

impl Slot {
    pub fn new(permit: Permit) -> Self {
        Self {
            held: Some(Held::Running(permit)),
        }
    }

    /// Whether the query holds a running slot now.
    pub fn is_running(&self) -> bool {
        matches!(self.held, Some(Held::Running(_)))
    }

    /// Gives the running slot back; `false` when the slot is kept (the node
    /// has `max_suspended_queries` suspended already) or was not running.
    pub fn suspend(&mut self) -> bool {
        match self.held.take() {
            Some(Held::Running(permit)) => match permit.suspend() {
                Ok(suspension) => {
                    self.held = Some(Held::Suspended(suspension));
                    true
                }
                Err(permit) => {
                    self.held = Some(Held::Running(permit));
                    false
                }
            },
            other => {
                self.held = other;
                false
            }
        }
    }

    /// Holds a running slot again, waiting for one when the node, the group
    /// or the user has none free (ahead of the queries that never ran).
    /// Cancel-safe: dropped while it waits, the place in the queue is given
    /// back and the slot holds nothing.
    pub async fn resume(&mut self) {
        match self.held.take() {
            Some(Held::Suspended(suspension)) => {
                let permit = suspension.resume().ready().await;
                self.held = Some(Held::Running(permit));
            }
            other => self.held = other,
        }
    }

    /// Waits for `room` (the client making room for the next results),
    /// holding the running slot for `suspend_after` at most; past that the
    /// slot is given back and the wait goes on.
    pub async fn wait_for_client<T>(&mut self, suspend_after: Duration, room: impl Future<Output = T>) -> T {
        let mut room = pin!(room);
        if let Ok(ready) = tokio::time::timeout(suspend_after, &mut room).await {
            return ready;
        }
        self.suspend();
        room.await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::GovernanceConfig;
    use crate::governance::Admission;

    fn admission(running: usize, suspended: usize) -> Arc<Admission> {
        Arc::new(Admission::new(&GovernanceConfig {
            max_running_queries: running,
            max_running_per_user: running,
            max_suspended_queries: suspended,
            ..GovernanceConfig::default()
        }))
    }

    async fn slot(a: &Arc<Admission>) -> Slot {
        Slot::new(a.enter("u", None).unwrap().ready().await)
    }

    #[tokio::test]
    async fn a_client_that_reads_in_time_keeps_the_slot() {
        let a = admission(1, 4);
        let mut slot = slot(&a).await;
        let read = slot
            .wait_for_client(Duration::from_secs(5), async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                7
            })
            .await;
        assert_eq!(read, 7);
        assert!(slot.is_running());
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
        drop(slot);
        assert_eq!(a.load(), (0, 0));
    }

    #[tokio::test]
    async fn a_client_that_does_not_read_gets_the_slot_suspended() {
        let a = admission(1, 4);
        let mut slot = slot(&a).await;
        let (tx, rx) = tokio::sync::oneshot::channel::<u8>();
        let observer = Arc::clone(&a);
        let client = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            // While the query waits, its slot is free for another query.
            let other = observer.enter("u", None).unwrap();
            assert!(!other.is_queued(), "the slot was given back");
            assert_eq!(observer.suspended(), 1);
            drop(other);
            tx.send(3).unwrap();
        });
        let read = slot
            .wait_for_client(Duration::from_millis(20), async { rx.await.unwrap() })
            .await;
        client.await.unwrap();
        assert_eq!(read, 3);
        assert!(!slot.is_running());
        assert_eq!((a.load(), a.suspended()), ((0, 0), 1));
        slot.resume().await;
        assert!(slot.is_running());
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
        // Resuming a running slot changes nothing.
        slot.resume().await;
        assert_eq!(a.load(), (1, 0));
        drop(slot);
        assert_eq!((a.load(), a.suspended()), ((0, 0), 0));
    }

    #[tokio::test]
    async fn the_cap_keeps_the_slot_while_it_waits() {
        let a = admission(2, 0);
        let mut slot = slot(&a).await;
        slot.wait_for_client(
            Duration::from_millis(10),
            tokio::time::sleep(Duration::from_millis(50)),
        )
        .await;
        assert!(slot.is_running());
        assert!(!slot.suspend());
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
    }

    /// Resuming waits while the node is full; dropped while it waits, it
    /// leaves nothing behind.
    #[tokio::test]
    async fn resuming_waits_for_room_and_is_cancel_safe() {
        let a = admission(1, 4);
        let mut slot = slot(&a).await;
        assert!(slot.suspend());
        assert!(!slot.suspend(), "suspended already");
        let busy = a.enter("u", None).unwrap().ready().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slot.resume())
                .await
                .is_err(),
            "the node is full"
        );
        assert!(!slot.is_running());
        assert_eq!((a.load(), a.suspended()), ((1, 0), 0));
        drop(slot);
        drop(busy);
        assert_eq!((a.load(), a.suspended()), ((0, 0), 0));

        let mut slot = Slot::new(a.enter("u", None).unwrap().ready().await);
        assert!(slot.suspend());
        let busy = a.enter("u", None).unwrap().ready().await;
        let resumed = {
            let a = Arc::clone(&a);
            tokio::spawn(async move {
                slot.resume().await;
                assert!(slot.is_running());
                assert_eq!(a.load(), (1, 0));
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while a.load() != (1, 1) {
            assert!(
                std::time::Instant::now() < deadline,
                "never queued: {:?}",
                a.load()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(busy);
        resumed.await.unwrap();
        assert_eq!((a.load(), a.suspended()), ((0, 0), 0));
    }
}
