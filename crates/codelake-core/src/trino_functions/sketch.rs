//! Bounded-memory sketches for `approx_distinct` and `approx_percentile`.
//!
//! Trino answers both in constant memory (`HyperLogLog`, `qdigest`). Keeping
//! every value instead makes a large input fail with a memory error where
//! Trino answers, so both sketches stay exact while the input is small (the
//! answer is then the exact one, as Trino's sparse forms give) and switch to a
//! fixed-size summary past a threshold:
//!
//! - [`Hll`]: the exact set of value hashes up to [`SPARSE_LIMIT`], then a
//!   `HyperLogLog` with 2^14 registers (standard error 0.8%, tighter than
//!   Trino's default 2.3%);
//! - [`QuantileSketch`]: `(value, weight)` pairs, compacted past `2 ×`
//!   [`QUANTILE_ITEMS`]: first without loss (equal values and runs of
//!   consecutive integers merge, so dense integer data stays exact), then, if
//!   still too many, to [`QUANTILE_ITEMS`] items of about equal weight: a
//!   rank is off by at most one item's weight (total weight /
//!   [`QUANTILE_ITEMS`], 0.025% of the input).

use std::collections::HashSet;

use num_traits::ToPrimitive;

/// Distinct hashes kept exactly before switching to registers.
pub(crate) const SPARSE_LIMIT: usize = 4096;
const REGISTER_BITS: u32 = 14;
const REGISTERS: usize = 1 << REGISTER_BITS;

/// `approx_distinct` state: exact while small, `HyperLogLog` registers after.
#[derive(Debug, Default)]
pub(crate) struct Hll {
    sparse: HashSet<u64>,
    registers: Option<Vec<u8>>,
}

/// Spreads the bits of a value hash (`MurmurHash3`'s 64-bit finalizer).
fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

fn update_register(registers: &mut [u8], hash: u64) {
    let h = mix(hash);
    let index = usize::try_from(h >> (64 - REGISTER_BITS)).unwrap_or(0);
    let rest = h << REGISTER_BITS;
    // Leading zeros of the remaining 50 bits, plus one.
    let rank = u8::try_from(rest.leading_zeros().min(64 - REGISTER_BITS) + 1).unwrap_or(u8::MAX);
    if let Some(slot) = registers.get_mut(index)
        && *slot < rank
    {
        *slot = rank;
    }
}

impl Hll {
    pub(crate) fn insert(&mut self, hash: u64) {
        if let Some(registers) = &mut self.registers {
            update_register(registers, hash);
            return;
        }
        self.sparse.insert(hash);
        if self.sparse.len() > SPARSE_LIMIT {
            self.densify();
        }
    }

    fn densify(&mut self) {
        let mut registers = vec![0_u8; REGISTERS];
        for hash in self.sparse.drain() {
            update_register(&mut registers, hash);
        }
        self.sparse = HashSet::new();
        self.registers = Some(registers);
    }

    /// Adds another state's registers.
    pub(crate) fn merge_registers(&mut self, other: &[u8]) {
        if self.registers.is_none() {
            self.densify();
        }
        if let Some(registers) = &mut self.registers {
            for (mine, theirs) in registers.iter_mut().zip(other) {
                *mine = (*mine).max(*theirs);
            }
        }
    }

    /// Exact hashes (empty once dense).
    pub(crate) fn hashes(&self) -> impl Iterator<Item = u64> + '_ {
        self.sparse.iter().copied()
    }

    pub(crate) fn registers(&self) -> Option<&[u8]> {
        self.registers.as_deref()
    }

    pub(crate) fn estimate(&self) -> i64 {
        let Some(registers) = &self.registers else {
            return i64::try_from(self.sparse.len()).unwrap_or(i64::MAX);
        };
        let m = f64::from(u32::try_from(REGISTERS).unwrap_or(u32::MAX));
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let (sum, zeros) = registers.iter().fold((0.0_f64, 0_u32), |(sum, zeros), r| {
            (sum + (-f64::from(*r)).exp2(), zeros + u32::from(*r == 0))
        });
        let raw = alpha * m * m / sum;
        let estimate = if raw <= 2.5 * m && zeros > 0 {
            // Linear counting in the small range.
            m * (m / f64::from(zeros)).ln()
        } else {
            raw
        };
        estimate.round().to_i64().unwrap_or(i64::MAX)
    }

    pub(crate) fn size(&self) -> usize {
        size_of::<Self>()
            + self.sparse.capacity() * (size_of::<u64>() + 8)
            + self.registers.as_ref().map_or(0, Vec::capacity)
    }
}

/// Items a [`QuantileSketch`] keeps after a lossy compaction.
pub(crate) const QUANTILE_ITEMS: usize = 4096;

/// Every value of `lo..=hi` (one value for floats), each with `weight`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Run<T> {
    pub(crate) lo: T,
    pub(crate) hi: T,
    pub(crate) weight: i64,
}

/// A value kind a [`QuantileSketch`] can hold.
pub(crate) trait SketchValue: Copy + std::fmt::Debug {
    /// Values of a run.
    fn values(run: &Run<Self>) -> i128;
    /// The `k`-th value (from 0) of a run.
    fn nth(run: &Run<Self>, k: i128) -> Self;
    /// Lossless normal form: sorted, disjoint runs, equal neighbours merged.
    fn normalize(items: &[Run<Self>]) -> Vec<Run<Self>>;
}

impl SketchValue for i64 {
    fn values(run: &Run<Self>) -> i128 {
        i128::from(run.hi) - i128::from(run.lo) + 1
    }

    fn nth(run: &Run<Self>, k: i128) -> Self {
        i64::try_from(i128::from(run.lo) + k).unwrap_or(run.hi)
    }

    /// A sweep over the run boundaries: overlapping runs add their weights.
    fn normalize(items: &[Run<Self>]) -> Vec<Run<Self>> {
        let mut events: Vec<(i128, i128)> = Vec::with_capacity(2 * items.len());
        for run in items {
            events.push((i128::from(run.lo), i128::from(run.weight)));
            events.push((i128::from(run.hi) + 1, -i128::from(run.weight)));
        }
        events.sort_unstable_by_key(|(at, _)| *at);
        let mut out: Vec<Run<i64>> = Vec::new();
        let mut weight: i128 = 0;
        let mut i = 0;
        while i < events.len() {
            let at = events[i].0;
            while i < events.len() && events[i].0 == at {
                weight += events[i].1;
                i += 1;
            }
            let (Some(next), Ok(lo)) = (events.get(i).map(|e| e.0), i64::try_from(at)) else {
                continue;
            };
            if weight <= 0 {
                continue;
            }
            let (Ok(hi), Ok(w)) = (i64::try_from(next - 1), i64::try_from(weight)) else {
                continue;
            };
            match out.last_mut() {
                Some(last) if i128::from(last.hi) + 1 == at && last.weight == w => last.hi = hi,
                _ => out.push(Run { lo, hi, weight: w }),
            }
        }
        out
    }
}

impl SketchValue for f64 {
    fn values(_run: &Run<Self>) -> i128 {
        1
    }

    fn nth(run: &Run<Self>, _k: i128) -> Self {
        run.lo
    }

    fn normalize(items: &[Run<Self>]) -> Vec<Run<Self>> {
        let mut sorted = items.to_vec();
        sorted.sort_by(|a, b| a.lo.total_cmp(&b.lo));
        let mut out: Vec<Run<f64>> = Vec::with_capacity(sorted.len());
        for run in sorted {
            match out.last_mut() {
                Some(last) if last.lo.total_cmp(&run.lo).is_eq() => {
                    last.weight = last.weight.saturating_add(run.weight);
                }
                _ => out.push(run),
            }
        }
        out
    }
}

fn run_weight<T: SketchValue>(run: &Run<T>) -> i128 {
    T::values(run) * i128::from(run.weight)
}

/// The value at cumulative weight `target` (0-based, `0 ≤ target < weight`)
/// of sorted, disjoint runs.
fn value_at<T: SketchValue>(runs: &[Run<T>], target: i128) -> Option<T> {
    let mut before: i128 = 0;
    for run in runs {
        let total = run_weight(run);
        if before + total > target {
            return Some(T::nth(run, (target - before) / i128::from(run.weight.max(1))));
        }
        before += total;
    }
    runs.last().map(|r| r.hi)
}

/// Weighted values in bounded memory (see the module docs). Runs of
/// consecutive integers with one weight are kept as one item, so dense
/// integer data stays exact far past [`QUANTILE_ITEMS`] values.
#[derive(Debug)]
pub(crate) struct QuantileSketch<T> {
    items: Vec<Run<T>>,
}

impl<T: SketchValue> Default for QuantileSketch<T> {
    fn default() -> Self {
        Self { items: Vec::new() }
    }
}

impl<T: SketchValue> QuantileSketch<T> {
    pub(crate) fn add(&mut self, run: Run<T>) {
        if self.items.len() == self.items.capacity() && self.items.len() >= QUANTILE_ITEMS {
            // One allocation of the final size instead of doubling past it.
            self.items.reserve_exact(2 * QUANTILE_ITEMS - self.items.len());
        }
        self.items.push(run);
        if self.items.len() >= 2 * QUANTILE_ITEMS {
            self.compact();
        }
    }

    pub(crate) fn items(&self) -> &[Run<T>] {
        &self.items
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// First without loss (overlaps summed, neighbours merged); if that is
    /// still too many items, sorted groups of about `total /
    /// QUANTILE_ITEMS` weight become one value (where the group's middle
    /// weight falls), and runs heavier than that stay whole.
    fn compact(&mut self) {
        let normal = T::normalize(&self.items);
        let out = if normal.len() <= QUANTILE_ITEMS {
            normal
        } else {
            let total: i128 = normal.iter().map(run_weight).sum();
            let buckets = i128::try_from(QUANTILE_ITEMS).unwrap_or(i128::MAX);
            let target = (total + buckets - 1) / buckets;
            let mut out: Vec<Run<T>> = Vec::with_capacity(QUANTILE_ITEMS + 1);
            let mut group: Vec<Run<T>> = Vec::new();
            let mut group_weight: i128 = 0;
            let flush = |group: &mut Vec<Run<T>>, group_weight: &mut i128, out: &mut Vec<Run<T>>| {
                if let Some(value) = value_at(group, *group_weight / 2) {
                    out.push(Run {
                        lo: value,
                        hi: value,
                        weight: i64::try_from(*group_weight).unwrap_or(i64::MAX),
                    });
                }
                group.clear();
                *group_weight = 0;
            };
            for run in normal {
                let weight = run_weight(&run);
                if weight >= target && group.is_empty() {
                    out.push(run);
                    continue;
                }
                group.push(run);
                group_weight += weight;
                if group_weight >= target {
                    flush(&mut group, &mut group_weight, &mut out);
                }
            }
            if !group.is_empty() {
                flush(&mut group, &mut group_weight, &mut out);
            }
            out
        };
        // Same buffer: its capacity (the reported size) stays put. The
        // aggregation operator only ever grows its reservation, so a buffer
        // that shrank and grew again would be counted twice.
        self.items.clear();
        self.items.extend_from_slice(&out);
    }

    /// The first value whose cumulative weight passes `p × total` (Trino's
    /// rule), for each `p`.
    pub(crate) fn quantiles(&self, percentiles: &[f64]) -> Vec<Option<T>> {
        let runs = T::normalize(&self.items);
        let total: i128 = runs.iter().map(run_weight).sum();
        let total_f = super::cast::i64_to_f64(i64::try_from(total).unwrap_or(i64::MAX));
        percentiles
            .iter()
            .map(|p| {
                // Smallest cumulative weight strictly above `p × total`.
                let threshold = (p * total_f).floor();
                let index = threshold
                    .to_i64()
                    .map_or(total, i128::from)
                    .clamp(0, (total - 1).max(0));
                value_at(&runs, index)
            })
            .collect()
    }

    pub(crate) fn size(&self) -> usize {
        size_of::<Self>() + self.items.capacity() * size_of::<Run<T>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_is_exact_while_sparse_and_close_after() {
        let mut small = Hll::default();
        for v in 0..1000_u64 {
            small.insert(v.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            small.insert(v.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        assert_eq!(small.estimate(), 1000);
        let mut big = Hll::default();
        for v in 0..1_000_000_u64 {
            big.insert(v);
        }
        let estimate = super::super::cast::i64_to_f64(big.estimate());
        assert!((estimate - 1e6).abs() < 0.03 * 1e6, "{estimate}");
        assert!(big.size() < 64 * 1024);
    }

    #[test]
    fn merged_registers_count_the_union() {
        let mut a = Hll::default();
        let mut b = Hll::default();
        for v in 0..100_000_u64 {
            a.insert(v);
            b.insert(v + 50_000);
        }
        a.merge_registers(b.registers().unwrap_or_default());
        let estimate = super::super::cast::i64_to_f64(a.estimate());
        assert!((estimate - 150_000.0).abs() < 0.03 * 150_000.0, "{estimate}");
    }

    fn single(v: i64) -> Run<i64> {
        Run {
            lo: v,
            hi: v,
            weight: 1,
        }
    }

    #[test]
    fn quantiles_stay_close_in_bounded_memory() {
        let mut sketch = QuantileSketch::<i64>::default();
        for v in 1..=1_000_000_i64 {
            // Scrambled arrival order, one value in 7 missing: no long runs.
            let v = (v * 7919) % 1_000_000 + 1;
            if v % 7 != 0 {
                sketch.add(single(v));
            }
        }
        assert!(sketch.items().len() < 2 * QUANTILE_ITEMS);
        let got = sketch.quantiles(&[0.5, 0.9]);
        let median = super::super::cast::i64_to_f64(got[0].unwrap_or_default());
        let p90 = super::super::cast::i64_to_f64(got[1].unwrap_or_default());
        assert!((median - 500_000.0).abs() < 5_000.0, "{median}");
        assert!((p90 - 900_000.0).abs() < 5_000.0, "{p90}");
    }

    #[test]
    fn dense_integers_and_small_inputs_are_exact() {
        let mut sketch = QuantileSketch::<i64>::default();
        for v in [4, 1, 3, 2] {
            sketch.add(single(v));
        }
        assert_eq!(
            sketch.quantiles(&[0.0, 0.25, 0.5, 1.0]),
            vec![Some(1), Some(2), Some(3), Some(4)]
        );
        let mut dense = QuantileSketch::<i64>::default();
        for v in (1..=100_000_i64).rev() {
            dense.add(single(v));
        }
        assert!(dense.items().len() < 2 * QUANTILE_ITEMS);
        assert_eq!(
            dense.quantiles(&[0.01, 0.5, 0.95, 0.99]),
            vec![Some(1_001), Some(50_001), Some(95_001), Some(99_001)]
        );
    }

    #[test]
    fn overlapping_runs_add_up() {
        let runs = i64::normalize(&[
            Run {
                lo: 1,
                hi: 10,
                weight: 1,
            },
            Run {
                lo: 5,
                hi: 6,
                weight: 2,
            },
            Run {
                lo: 11,
                hi: 12,
                weight: 1,
            },
        ]);
        assert_eq!(
            runs,
            vec![
                Run {
                    lo: 1,
                    hi: 4,
                    weight: 1
                },
                Run {
                    lo: 5,
                    hi: 6,
                    weight: 3
                },
                Run {
                    lo: 7,
                    hi: 12,
                    weight: 1
                },
            ]
        );
        let floats = f64::normalize(&[
            Run {
                lo: 2.0,
                hi: 2.0,
                weight: 1,
            },
            Run {
                lo: 1.0,
                hi: 1.0,
                weight: 1,
            },
            Run {
                lo: 2.0,
                hi: 2.0,
                weight: 3,
            },
        ]);
        assert_eq!(floats.len(), 2);
        assert_eq!(floats[1].weight, 4);
    }
}
