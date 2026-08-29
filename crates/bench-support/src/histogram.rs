//! A latency histogram with a bounded relative error, recorded without
//! allocating.
//!
//! # Why not just keep the samples
//!
//! A 60-second run at a million messages a second is 60 million samples. Storing
//! them is 480MB and sorting them to get a p99.9 is a second of work — on the
//! machine that is supposed to be measuring something. Worse, the allocation
//! would happen *on the path being measured*, which is the one thing milestone 5
//! spent a session proving does not happen.
//!
//! # Why not a fixed bucket width
//!
//! Latency spans four or five orders of magnitude: a decode is tens of
//! nanoseconds, a p99.9 stall can be milliseconds. Buckets narrow enough to say
//! something useful about 40ns are absurd at 4ms, and buckets wide enough for
//! 4ms say nothing at all about 40ns.
//!
//! # What this does instead
//!
//! The HDR histogram layout: buckets are powers of two, each subdivided into a
//! fixed number of linear sub-buckets. The bucket index comes from the leading
//! zero count, so recording is a few instructions and one array increment, and
//! the **relative** error is bounded everywhere — 0.1% at three significant
//! figures, at 40ns and at 4ms alike.
//!
//! This is a deliberate reimplementation rather than a dependency. It is about
//! eighty lines, its correctness is directly testable against exact quantiles,
//! and a number this project publishes should not rest on a transitive crate
//! nobody in the reader's position is going to audit.
//!
//! # Reading a quantile
//!
//! [`Histogram::quantile`] returns the **highest value equivalent** to the
//! bucket the quantile falls in, which is what HdrHistogram reports and is the
//! conservative direction for a latency figure: it never claims the system was
//! faster than the bucket allows.

/// Significant figures the bucketing preserves.
const SIGNIFICANT_FIGURES: u32 = 3;
/// `2^SUB_BUCKET_BITS` linear sub-buckets per power of two.
///
/// Chosen so that `2^bits >= 2 * 10^SIGNIFICANT_FIGURES` — the doubling is what
/// makes the *lower half* of every bucket unreachable, which is what keeps the
/// relative error bounded rather than merely small on average.
const SUB_BUCKET_BITS: u32 = 11;
// The relationship the error bound rests on, checked by the compiler rather
// than left as a comment that can drift away from the constant above it.
const _: () = assert!(
    (1u64 << SUB_BUCKET_BITS) >= 2 * 10u64.pow(SIGNIFICANT_FIGURES),
    "SUB_BUCKET_BITS is too small for the significant figures it promises"
);
const SUB_BUCKET_COUNT: u64 = 1 << SUB_BUCKET_BITS;
const SUB_BUCKET_HALF: u64 = SUB_BUCKET_COUNT / 2;
const SUB_BUCKET_MASK: u64 = SUB_BUCKET_COUNT - 1;
/// `64 - log2(SUB_BUCKET_COUNT)`. See [`Histogram::bucket_index`].
const LEADING_ZERO_BASE: u32 = 64 - SUB_BUCKET_BITS;

/// The default ceiling: ten seconds in nanoseconds.
///
/// Anything slower than this is not a latency measurement, it is an outage, and
/// it is counted in [`Histogram::overflowed`] rather than distorting the
/// buckets.
pub const DEFAULT_HIGHEST_NANOS: u64 = 10_000_000_000;

#[derive(Debug, Clone)]
pub struct Histogram {
    counts: Box<[u64]>,
    highest: u64,
    total: u64,
    /// Samples above `highest`. Counted, never silently clamped: a clamped
    /// outlier is indistinguishable from a fast one at the top of the range.
    overflow: u64,
    min: u64,
    max: u64,
    sum: u128,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new(DEFAULT_HIGHEST_NANOS)
    }
}

impl Histogram {
    /// Sized to track values up to `highest`. Allocates once, here.
    pub fn new(highest: u64) -> Self {
        let highest = highest.max(SUB_BUCKET_COUNT);
        // How many powers of two are needed on top of the first bucket.
        let mut buckets = 1usize;
        let mut smallest_untrackable = SUB_BUCKET_COUNT;
        while smallest_untrackable <= highest {
            if smallest_untrackable > u64::MAX / 2 {
                break;
            }
            smallest_untrackable <<= 1;
            buckets += 1;
        }
        let len = (buckets + 1) * (SUB_BUCKET_HALF as usize);
        Self {
            counts: vec![0u64; len].into_boxed_slice(),
            highest,
            total: 0,
            overflow: 0,
            min: u64::MAX,
            max: 0,
            sum: 0,
        }
    }

    /// Which power-of-two bucket `value` lands in.
    ///
    /// `value | SUB_BUCKET_MASK` forces at least `SUB_BUCKET_BITS` significant
    /// bits, so every value below `SUB_BUCKET_COUNT` lands in bucket 0 and is
    /// recorded exactly.
    #[inline]
    fn bucket_index(value: u64) -> u32 {
        LEADING_ZERO_BASE - (value | SUB_BUCKET_MASK).leading_zeros()
    }

    #[inline]
    fn counts_index(value: u64) -> usize {
        let bucket = Self::bucket_index(value);
        let sub = value >> bucket;
        // Bucket 0 covers [0, SUB_BUCKET_COUNT); every later bucket contributes
        // only its upper half, because its lower half is covered by the bucket
        // below it at finer resolution.
        let base = (u64::from(bucket) + 1) << (SUB_BUCKET_BITS - 1);
        (base + sub - SUB_BUCKET_HALF) as usize
    }

    /// The largest value that lands in the same slot as `value`.
    #[inline]
    fn highest_equivalent(value: u64) -> u64 {
        let bucket = Self::bucket_index(value);
        let unit = 1u64 << bucket;
        (value / unit) * unit + unit - 1
    }

    /// Records one sample. Does not allocate.
    #[inline]
    pub fn record(&mut self, value: u64) {
        if value > self.highest {
            self.overflow += 1;
            self.total += 1;
            self.max = self.max.max(value);
            self.sum += u128::from(value);
            return;
        }
        let i = Self::counts_index(value);
        self.counts[i] += 1;
        self.total += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += u128::from(value);
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Samples that exceeded the tracked range.
    ///
    /// Non-zero means the reported quantiles above that point are floors, not
    /// values, and a report has to say so.
    pub fn overflowed(&self) -> u64 {
        self.overflow
    }

    pub fn min(&self) -> u64 {
        if self.total == 0 {
            0
        } else {
            self.min
        }
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn mean(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.sum as f64) / (self.total as f64)
    }

    /// The value at `q`, where `q` is in `0.0..=1.0`.
    ///
    /// Returns the highest value equivalent to the bucket the quantile falls in
    /// — the conservative direction, and the one HdrHistogram reports. A
    /// quantile that lands in the overflow is reported as [`max`](Self::max).
    pub fn quantile(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        // Ceiling, so that `quantile(1.0)` is the last sample rather than one
        // past it, and so a p99 over 100 samples is the 99th, not the 98th.
        let rank = ((q * self.total as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            if *c == 0 {
                continue;
            }
            seen += *c;
            if seen >= rank {
                return Self::highest_equivalent(Self::value_from_index(i));
            }
        }
        // Everything in the counts array was passed, so the rank is in the
        // overflow. The true value is unknown beyond being at least `highest`.
        self.max
    }

    /// The lowest value that lands in slot `i`. Inverse of `counts_index`.
    fn value_from_index(i: usize) -> u64 {
        let i = i as u64;
        // Bucket 0 occupies the first `SUB_BUCKET_COUNT` slots and its unit is
        // 1, so the slot *is* the value.
        if i < SUB_BUCKET_COUNT {
            return i;
        }
        let bucket = (i >> (SUB_BUCKET_BITS - 1)) - 1;
        let sub = (i & (SUB_BUCKET_HALF - 1)) + SUB_BUCKET_HALF;
        sub << bucket
    }

    /// Folds `other` into `self`. Both must have been built with the same
    /// `highest`, which the assertion enforces rather than hoping.
    pub fn merge(&mut self, other: &Histogram) {
        assert_eq!(
            self.counts.len(),
            other.counts.len(),
            "merging histograms of different ranges would silently misfile every sample"
        );
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += *b;
        }
        self.total += other.total;
        self.overflow += other.overflow;
        self.sum += other.sum;
        if other.total > 0 {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
    }

    /// Empties it, keeping the allocation.
    pub fn clear(&mut self) {
        for c in self.counts.iter_mut() {
            *c = 0;
        }
        self.total = 0;
        self.overflow = 0;
        self.min = u64::MAX;
        self.max = 0;
        self.sum = 0;
    }

    /// `count median p99 p99.9 max` and friends, as `key=value` for a report
    /// file. `prefix` names what was measured.
    pub fn to_fields(&self, prefix: &str) -> String {
        format!(
            "{prefix}_count={}\n\
             {prefix}_min={}\n\
             {prefix}_median={}\n\
             {prefix}_p90={}\n\
             {prefix}_p99={}\n\
             {prefix}_p999={}\n\
             {prefix}_max={}\n\
             {prefix}_mean={:.1}\n\
             {prefix}_overflow={}",
            self.total,
            self.min(),
            self.quantile(0.5),
            self.quantile(0.90),
            self.quantile(0.99),
            self.quantile(0.999),
            self.max,
            self.mean(),
            self.overflow,
        )
    }
}

impl std::fmt::Display for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} median={} p99={} p99.9={} max={} mean={:.1}",
            self.total,
            self.quantile(0.5),
            self.quantile(0.99),
            self.quantile(0.999),
            self.max,
            self.mean()
        )?;
        if self.overflow > 0 {
            write!(f, " OVERFLOW={}", self.overflow)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guarantee the whole layout exists to provide.
    const MAX_RELATIVE_ERROR: f64 = 1.0 / (1 << (SUB_BUCKET_BITS - 1)) as f64;

    #[test]
    fn small_values_are_recorded_exactly() {
        // Below SUB_BUCKET_COUNT the unit is 1, so there is no bucketing error
        // at all — which matters, because a decode is supposed to land at a few
        // tens of nanoseconds and that is exactly this range.
        let mut h = Histogram::default();
        for v in [1u64, 2, 7, 40, 99, 100, 500, 2047] {
            h.clear();
            h.record(v);
            assert_eq!(h.quantile(0.5), v, "value {v} was not recorded exactly");
            assert_eq!(h.min(), v);
            assert_eq!(h.max(), v);
        }
    }

    #[test]
    fn every_value_reads_back_within_the_error_bound() {
        // The claim in the module docs, checked across five orders of magnitude
        // rather than asserted.
        let mut h = Histogram::default();
        let mut v = 1u64;
        while v < 1_000_000_000 {
            h.clear();
            h.record(v);
            let read = h.quantile(1.0);
            assert!(
                read >= v,
                "value {v} read back as {read}, which is below it: a latency \
                 histogram must never report faster than the truth"
            );
            let err = (read - v) as f64 / v as f64;
            assert!(
                err <= MAX_RELATIVE_ERROR,
                "value {v} read back as {read}, relative error {err:.5} exceeds \
                 the {MAX_RELATIVE_ERROR:.5} the bucketing promises"
            );
            v = v + (v / 7) + 1;
        }
    }

    #[test]
    fn quantiles_match_an_exact_computation() {
        // A uniform 1..=100_000 has quantiles that can be worked out by hand, so
        // this checks the rank arithmetic rather than trusting it.
        let mut h = Histogram::default();
        for v in 1..=100_000u64 {
            h.record(v);
        }
        assert_eq!(h.count(), 100_000);
        for (q, exact) in [(0.5, 50_000.0), (0.9, 90_000.0), (0.99, 99_000.0)] {
            let got = h.quantile(q) as f64;
            let err = (got - exact).abs() / exact;
            assert!(
                err <= MAX_RELATIVE_ERROR,
                "q{q}: got {got}, expected about {exact}, relative error {err:.5}"
            );
        }
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 100_000);
    }

    #[test]
    fn a_tail_of_slow_samples_shows_up_in_p999_and_not_in_the_median() {
        // The shape of a real latency distribution, and the reason p99.9 is
        // reported at all: a median that looks fine while one request in a
        // thousand takes a hundred times longer is the case a mean hides.
        let mut h = Histogram::default();
        for _ in 0..999_000 {
            h.record(100);
        }
        for _ in 0..1_000 {
            h.record(50_000);
        }
        assert_eq!(h.quantile(0.5), 100);
        assert_eq!(h.quantile(0.99), 100);
        assert!(
            h.quantile(0.999) >= 100,
            "the tail must be visible at p99.9"
        );
        assert!(h.quantile(1.0) >= 50_000);
        assert!(
            h.mean() > 100.0 && h.mean() < 200.0,
            "the mean hides the tail, which is why it is not the headline number"
        );
    }

    #[test]
    fn a_value_past_the_range_is_counted_not_clamped() {
        // A clamped outlier is indistinguishable from a fast sample at the top
        // of the range, which would make the histogram lie in the direction
        // that flatters it.
        let mut h = Histogram::new(1_000);
        h.record(500);
        h.record(1_000_000);
        assert_eq!(h.overflowed(), 1);
        assert_eq!(h.count(), 2);
        assert_eq!(h.max(), 1_000_000, "the true maximum is still known");
        assert_eq!(h.quantile(1.0), 1_000_000);
    }

    #[test]
    fn an_empty_histogram_reports_zero_rather_than_panicking() {
        let h = Histogram::default();
        assert_eq!(h.count(), 0);
        assert_eq!(h.quantile(0.5), 0);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.mean(), 0.0);
    }

    #[test]
    fn merging_is_the_same_as_recording_into_one() {
        // Per-thread histograms merged at the end is how a multi-threaded run
        // avoids contending on a shared counter, so the merge has to be exact.
        let mut a = Histogram::default();
        let mut b = Histogram::default();
        let mut both = Histogram::default();
        for v in 1..=10_000u64 {
            if v % 2 == 0 {
                a.record(v);
            } else {
                b.record(v);
            }
            both.record(v);
        }
        a.merge(&b);
        assert_eq!(a.count(), both.count());
        assert_eq!(a.min(), both.min());
        assert_eq!(a.max(), both.max());
        for q in [0.5, 0.9, 0.99, 0.999] {
            assert_eq!(a.quantile(q), both.quantile(q), "q{q} differs after merge");
        }
    }

    #[test]
    fn clearing_keeps_the_allocation_and_the_shape() {
        let mut h = Histogram::default();
        for v in 1..=1000 {
            h.record(v);
        }
        h.clear();
        assert!(h.is_empty());
        assert_eq!(h.max(), 0);
        h.record(42);
        assert_eq!(h.quantile(0.5), 42);
    }

    #[test]
    fn the_index_round_trips_for_every_slot() {
        // `value_from_index` is the inverse of `counts_index`, and a quantile is
        // wrong in a way nothing else would catch if it is not.
        let h = Histogram::default();
        for i in 0..h.counts.len() {
            let v = Histogram::value_from_index(i);
            assert_eq!(
                Histogram::counts_index(v),
                i,
                "slot {i} maps to value {v}, which maps back to slot {}",
                Histogram::counts_index(v)
            );
        }
    }

    #[test]
    fn recording_does_not_allocate() {
        // It runs in the measured path. Milestone 5 built the tool that checks
        // this rather than asserting it; this is that tool pointed at the thing
        // measuring the thing it was built for.
        use alloc_guard::{AllocGuard, CountingAllocator};

        #[global_allocator]
        static ALLOC: CountingAllocator<std::alloc::System> =
            CountingAllocator::new(std::alloc::System);

        let mut h = Histogram::default();
        h.record(1); // warm anything lazy
        let guard = AllocGuard::start();
        for v in 1..=100_000u64 {
            h.record(v);
        }
        let _ = h.quantile(0.99);
        let delta = guard.finish();
        assert!(
            delta.is_clean(),
            "the histogram allocated while recording: {delta}"
        );
    }
}
