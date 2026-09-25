//! [`CeilingPool`]: the engine's memory pool, a hard ceiling shared fairly by
//! the operators that can spill.
//!
//! It keeps `DataFusion`'s `FairSpillPool` policy — an operator that can spill
//! (sort, hash aggregation) gets at most its share of what the operators that
//! cannot spill left, so every one of them can reach its share — and changes
//! two things that broke the ceiling in `DataFusion` 53:
//!
//! 1. **The total is checked.** `FairSpillPool` checks a spilling reservation
//!    only against its share, one reservation at a time, and an operator may
//!    hold several: the external sort splits its own (one per sorted batch),
//!    an aggregation merging its spill files reserves them on a second one.
//!    Each still gets its share — `DataFusion`'s operators rely on it — but
//!    here every refusable growth also keeps the total under the limit.
//! 2. **Exchange buffers do not dilute the shares.** Every `RepartitionExec`
//!    registers one spilling consumer per output partition, so a plan over 18
//!    partitions registered dozens of consumers that each held a batch or two;
//!    a sort's share of a 64 MiB pool fell to ~3 MB, less than the reservation
//!    of one 8192-row batch, and the sort failed without spilling. Those
//!    buffers spill any batch they are refused; here they are not counted
//!    among the operators sharing the pool and have a slice of their own,
//!    [`BUFFERS_DIVISOR`]⁻¹ of it, set aside from the shares: a buffer never
//!    takes the memory a sort or an aggregation freed to spill (both give
//!    their reservation back, then reserve again to sort what they spill).
//!
//! Unchecked growth (`MemoryReservation::grow`) is recorded but not refused, as
//! in every `DataFusion` pool. `DataFusion` 54 uses it only for memory an
//! operator already holds and must keep: the nested loop join's visited
//! bitmaps and the one batch it accepts to make progress when refused (in
//! its spill fallback, which the engine keeps off: `crate::join_guard`), and
//! the sort-merge join's key arrays of a spilled batch and the spilled batch
//! it reads back to emit.

use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use datafusion::common::{human_readable_size, resources_datafusion_err};
use datafusion::error::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};

/// Exchange buffers together hold at most `limit / BUFFERS_DIVISOR`.
pub const BUFFERS_DIVISOR: usize = 8;

/// Consumer names of exchange channels: `DataFusion`'s (`RepartitionExec[3]`,
/// `SortPreservingRepartitionExec[0]`) and the engine's merge
/// ([`crate::output::AccountedMergeExec`]). Checked by
/// `exchange_buffers_are_recognized`, so a rename upstream fails a test.
const EXCHANGE_BUFFERS: [&str; 3] = [
    "RepartitionExec[",
    "SortPreservingRepartitionExec[",
    "AccountedMerge[",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Unspillable,
    /// A spilling exchange channel.
    Buffer,
    /// A spilling operator with a fair share.
    Spiller,
}

/// Whether `consumer` is an exchange channel, which spills what it is refused
/// and has its own slice of the pool.
pub(crate) fn is_exchange_buffer(consumer: &MemoryConsumer) -> bool {
    class(consumer) == Class::Buffer
}

fn class(consumer: &MemoryConsumer) -> Class {
    if !consumer.can_spill() {
        Class::Unspillable
    } else if EXCHANGE_BUFFERS
        .iter()
        .any(|prefix| consumer.name().starts_with(prefix))
    {
        Class::Buffer
    } else {
        Class::Spiller
    }
}

#[derive(Debug, Default)]
struct State {
    unspillable: usize,
    buffers: usize,
    spilling: usize,
    /// Spilling operators registered, whether they hold memory or not.
    registered: usize,
    /// Most bytes reserved at once since the pool started or the last
    /// [`CeilingPool::reset_peak`].
    peak: usize,
}

impl State {
    fn total(&self) -> usize {
        self.unspillable + self.buffers + self.spilling
    }

    fn add(&mut self, consumer: &MemoryConsumer, bytes: usize) {
        match class(consumer) {
            Class::Unspillable => self.unspillable += bytes,
            Class::Buffer => self.buffers += bytes,
            Class::Spiller => self.spilling += bytes,
        }
        self.peak = self.peak.max(self.total());
    }

    fn remove(&mut self, consumer: &MemoryConsumer, bytes: usize) {
        match class(consumer) {
            Class::Unspillable => self.unspillable = self.unspillable.saturating_sub(bytes),
            Class::Buffer => self.buffers = self.buffers.saturating_sub(bytes),
            Class::Spiller => self.spilling = self.spilling.saturating_sub(bytes),
        }
    }
}

/// See the module documentation.
#[derive(Debug)]
pub struct CeilingPool {
    limit: usize,
    state: Mutex<State>,
}

impl CeilingPool {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(State::default()),
        }
    }

    fn buffers_cap(&self) -> usize {
        self.limit / BUFFERS_DIVISOR
    }

    /// Most bytes reserved at once since the pool started or the last
    /// [`CeilingPool::reset_peak`].
    pub fn peak(&self) -> usize {
        self.state().peak
    }

    /// Restarts [`CeilingPool::peak`] from what is reserved now.
    pub fn reset_peak(&self) {
        let mut state = self.state();
        state.peak = state.total();
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Plain counters: a panic elsewhere leaves them consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn refused(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> datafusion::error::DataFusionError {
    resources_datafusion_err!(
        "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total pool",
        human_readable_size(additional),
        reservation.consumer().name(),
        human_readable_size(reservation.size()),
        human_readable_size(available)
    )
}

impl fmt::Display for CeilingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reserved = self.state().total();
        write!(
            f,
            "{}(used: {}, pool_size: {})",
            self.name(),
            human_readable_size(reserved),
            human_readable_size(self.limit)
        )
    }
}

impl MemoryPool for CeilingPool {
    fn name(&self) -> &'static str {
        "ceiling"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if class(consumer) == Class::Spiller {
            self.state().registered += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if class(consumer) == Class::Spiller {
            let mut state = self.state();
            state.registered = state.registered.saturating_sub(1);
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.state().add(reservation.consumer(), additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.state().remove(reservation.consumer(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let consumer = reservation.consumer();
        let mut state = self.state();
        let free = self.limit.saturating_sub(state.total());
        if additional > free {
            return Err(refused(reservation, additional, free));
        }
        match class(consumer) {
            Class::Unspillable => {}
            Class::Buffer => {
                let room = self.buffers_cap().saturating_sub(state.buffers);
                if additional > room {
                    return Err(refused(reservation, additional, room));
                }
            }
            Class::Spiller => {
                let share = self
                    .limit
                    .saturating_sub(state.unspillable + self.buffers_cap())
                    .checked_div(state.registered)
                    .unwrap_or(0);
                let room = share.saturating_sub(reservation.size());
                if additional > room {
                    return Err(refused(reservation, additional, room));
                }
            }
        }
        state.add(consumer, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state().total()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    const MIB: usize = 1 << 20;

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CeilingPool::new(limit))
    }

    fn spiller(pool: &Arc<dyn MemoryPool>, name: &str) -> MemoryReservation {
        MemoryConsumer::new(name).with_can_spill(true).register(pool)
    }

    #[test]
    fn unspillable_consumers_share_the_limit_first_come() {
        let pool = pool(10 * MIB);
        let a = MemoryConsumer::new("a").register(&pool);
        let b = MemoryConsumer::new("b").register(&pool);
        a.try_grow(6 * MIB).unwrap();
        assert!(b.try_grow(5 * MIB).is_err());
        b.try_grow(4 * MIB).unwrap();
        assert_eq!(pool.reserved(), 10 * MIB);
        drop(a);
        assert_eq!(pool.reserved(), 4 * MIB);
    }

    #[test]
    fn a_spiller_gets_its_share_of_what_unspillable_consumers_left() {
        let pool = pool(16 * MIB);
        let fixed = MemoryConsumer::new("fixed").register(&pool);
        fixed.try_grow(4 * MIB).unwrap();
        let a = spiller(&pool, "a");
        let _b = spiller(&pool, "b");
        // (16 - 4 - 2 for buffers) / 2 = 5 MiB each.
        a.try_grow(5 * MIB).unwrap();
        let error = a.try_grow(1).unwrap_err().to_string();
        assert!(error.contains("Resources exhausted"), "{error}");
        assert!(error.contains("for a with 5.0 MB"), "{error}");
    }

    /// Each reservation gets the share, as in `FairSpillPool`, but together
    /// they never pass the limit.
    #[test]
    fn every_reservation_gets_the_share_within_the_limit() {
        // (10 - 1.25 for buffers) / 2 = 4.375 MiB each.
        let pool = pool(10 * MIB);
        let first = spiller(&pool, "aggregation");
        let _other = spiller(&pool, "other");
        first.try_grow(4 * MIB).unwrap();
        assert!(first.try_grow(MIB).is_err());
        let merge = first.new_empty();
        merge.try_grow(4 * MIB).unwrap();
        // 8 MiB held, 2 MiB left: the share no longer fits.
        let third = first.new_empty();
        assert!(third.try_grow(3 * MIB).is_err());
        third.try_grow(2 * MIB).unwrap();
        assert_eq!(pool.reserved(), 10 * MIB);
    }

    #[test]
    fn the_total_never_passes_the_limit() {
        // (16 - 2 for buffers) / 2 = 7 MiB each.
        let pool = pool(16 * MIB);
        let a = spiller(&pool, "a");
        let b = spiller(&pool, "b");
        a.try_grow(7 * MIB).unwrap();
        // A consumer that cannot spill takes what is left...
        let fixed = MemoryConsumer::new("fixed").register(&pool);
        fixed.try_grow(9 * MIB).unwrap();
        // ...and `b`, within its share of 7 MiB when it registered, is refused
        // until `a` gives memory back.
        assert!(b.try_grow(MIB).is_err());
        a.shrink(7 * MIB);
        // Now (16 - 9 - 2) / 2 = 2.5 MiB.
        assert!(b.try_grow(3 * MIB).is_err());
        b.try_grow(2 * MIB).unwrap();
        assert_eq!(pool.reserved(), 11 * MIB);
    }

    #[test]
    fn exchange_buffers_are_recognized() {
        let consumer = |name: &str| MemoryConsumer::new(name).with_can_spill(true);
        assert_eq!(class(&consumer("RepartitionExec[3]")), Class::Buffer);
        assert_eq!(
            class(&consumer("SortPreservingRepartitionExec[0]")),
            Class::Buffer
        );
        let merge = format!("{}[0]", crate::output::MERGE_CONSUMER);
        assert_eq!(class(&consumer(&merge)), Class::Buffer);
        assert_eq!(class(&consumer("ExternalSorter[0]")), Class::Spiller);
        assert_eq!(
            class(&MemoryConsumer::new("RepartitionExec[Merge 0]")),
            Class::Unspillable
        );
    }

    /// Idle exchange buffers do not shrink a sort's share; together they hold
    /// at most an eighth of the pool, set aside from the shares.
    #[test]
    fn buffers_do_not_dilute_shares_and_have_their_own_slice() {
        let pool = pool(64 * MIB);
        let buffers: Vec<_> = (0..36)
            .map(|i| spiller(&pool, &format!("RepartitionExec[{i}]")))
            .collect();
        let sort = spiller(&pool, "ExternalSorter[0]");
        assert!(sort.try_grow(57 * MIB).is_err(), "the sort's share is 64 - 8");
        sort.try_grow(56 * MIB).unwrap();
        buffers[0].try_grow(5 * MIB).unwrap();
        assert!(
            buffers[1].try_grow(4 * MIB).is_err(),
            "buffers are capped at 8 MiB"
        );
        buffers[1].try_grow(3 * MIB).unwrap();
        assert_eq!(pool.reserved(), 64 * MIB);
        // What a buffer gives back is not taken by the sort.
        buffers[1].shrink(3 * MIB);
        assert!(sort.try_grow(MIB).is_err());
    }

    #[test]
    fn a_spiller_that_went_away_no_longer_counts() {
        // (16 - 2 for buffers) / 1 = 14 MiB once `a` is gone.
        let pool = pool(16 * MIB);
        let a = spiller(&pool, "a");
        let b = spiller(&pool, "b");
        a.try_grow(MIB).unwrap();
        assert!(
            b.try_grow(8 * MIB).is_err(),
            "share is 7 MiB while `a` is registered"
        );
        a.free();
        assert_eq!(pool.reserved(), 0);
        drop(a);
        b.try_grow(14 * MIB).unwrap();
    }
}
