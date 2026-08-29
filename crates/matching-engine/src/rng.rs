//! A small deterministic PRNG.
//!
//! Hand-rolled rather than pulled from `rand`, for one reason that matters:
//! reproducibility is a requirement here, not a convenience. `scripts/smoke.sh`
//! asserts that two processes agree on a book digest, and a run that cannot be
//! repeated byte for byte turns any disagreement into a coin flip rather than a
//! bug report. SplitMix64 is a dozen lines, has no dependencies to version-drift,
//! and produces the same stream on every platform and every toolchain.
//!
//! It is not cryptographically secure and nothing here wants it to be.

/// SplitMix64. Referenced by the algorithm's usual constants.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`. Returns 0 when `n` is 0 rather than dividing by it.
    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        // Modulo bias is negligible at the ranges used here (all far below
        // 2^32) and the alternative costs a rejection loop on the hot path.
        self.next_u64() % n
    }

    /// Uniform in `lo..=hi`.
    #[inline]
    pub fn range_inclusive(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `p`, clamped to `0.0..=1.0`.
    #[inline]
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        // 53 bits of mantissa is plenty and avoids any float conversion
        // surprises at the extremes.
        let x = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        x < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_produces_the_same_stream() {
        // The property the reproducibility of the whole smoke test rests on.
        let mut a = Rng::new(12345);
        let mut b = Rng::new(12345);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge_immediately() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn below_stays_in_range_and_tolerates_zero() {
        let mut r = Rng::new(7);
        assert_eq!(r.below(0), 0);
        for _ in 0..10_000 {
            assert!(r.below(10) < 10);
        }
    }

    #[test]
    fn range_inclusive_covers_both_ends() {
        let mut r = Rng::new(99);
        let mut saw_lo = false;
        let mut saw_hi = false;
        for _ in 0..10_000 {
            let v = r.range_inclusive(5, 9);
            assert!((5..=9).contains(&v));
            saw_lo |= v == 5;
            saw_hi |= v == 9;
        }
        assert!(saw_lo && saw_hi, "the endpoints must be reachable");
        assert_eq!(r.range_inclusive(4, 4), 4);
        assert_eq!(
            r.range_inclusive(9, 3),
            9,
            "an inverted range is not a panic"
        );
    }

    #[test]
    fn chance_is_roughly_calibrated_and_exact_at_the_extremes() {
        let mut r = Rng::new(2024);
        assert!(!r.chance(0.0));
        assert!(r.chance(1.0));

        let mut hits = 0;
        const N: usize = 100_000;
        for _ in 0..N {
            if r.chance(0.25) {
                hits += 1;
            }
        }
        let observed = hits as f64 / N as f64;
        assert!(
            (observed - 0.25).abs() < 0.01,
            "expected about 0.25, observed {observed}"
        );
    }
}
