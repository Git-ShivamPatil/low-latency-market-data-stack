//! Reading the cycle counter, and refusing to trust it when it should not be
//! trusted.
//!
//! # Why not `Instant::now`
//!
//! A vDSO `clock_gettime` is roughly 20–25ns. The decode this project has to
//! measure is supposed to be around 100ns, so the clock would be a quarter of
//! the measurement — and the overhead is not constant, so it cannot simply be
//! subtracted. At that resolution the timer has to be the cycle counter.
//!
//! # Why `rdtsc` is not free either
//!
//! `rdtsc` is only a clock if the TSC is **invariant**: constant across
//! frequency changes (`constant_tsc`) and not stopped in deep C-states
//! (`nonstop_tsc`). Without both, a "measurement" is a count of something that
//! changes rate underneath it, and the number produced is not wrong by a
//! knowable amount — it is meaningless.
//!
//! Both flags are missing under WSL2, which is the development host for this
//! project. That is not a detail to discover at report-writing time, so
//! [`TscQuality`] reads them and [`Tsc::calibrate`] carries the verdict with it.
//! Nothing here silently falls back to a worse clock and keeps printing
//! nanoseconds.
//!
//! # Ordering
//!
//! `rdtsc` is not a serialising instruction: the CPU may execute it before or
//! after the work being timed. The conventional fix is `lfence` before the
//! opening read and `rdtscp` (which waits for prior loads) followed by `lfence`
//! at the close. [`start`] and [`stop`] do exactly that. It costs a few cycles
//! and it is the difference between timing the work and timing something near
//! it.
//!
//! [`start`]: Tsc::start
//! [`stop`]: Tsc::stop

// Reading a cycle counter is an intrinsic and cannot be done in safe Rust. The
// unsafety is confined to this file and to two instructions that read a
// register; no pointer is created, dereferenced or interpreted.
#![allow(unsafe_code)]

use std::fmt;
use std::time::{Duration, Instant};

/// What the host says about its own cycle counter.
///
/// The two x86 flags are meaningless on aarch64, where the equivalent guarantee
/// is architectural rather than advertised — see [`TscQuality::detect`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TscQuality {
    /// The TSC ticks at a constant rate regardless of the core's frequency.
    pub constant_tsc: bool,
    /// The TSC keeps ticking in deep C-states.
    pub nonstop_tsc: bool,
    /// `/proc/cpuinfo` was readable at all. False on non-Linux, where the flags
    /// are simply unknown rather than absent.
    pub flags_readable: bool,
    /// aarch64 only: the generic timer's fixed frequency from `cntfrq_el0`.
    ///
    /// Recorded because it is usually far lower than an x86 TSC — 25MHz and
    /// 100MHz are both common on server ARM — and that sets a floor on what can
    /// be resolved. A report from an ARM host has to state it.
    pub counter_hz: Option<u64>,
}

impl TscQuality {
    /// Reads the flags from `/proc/cpuinfo`, or the timer frequency on aarch64.
    ///
    /// # Why aarch64 is different
    ///
    /// `constant_tsc` and `nonstop_tsc` are x86 CPUID bits and do not exist on
    /// ARM. The equivalent property is not advertised there because it is
    /// architectural: the generic timer (`cntvct_el0`) runs at the fixed
    /// frequency in `cntfrq_el0`, independent of the core clock and of idle
    /// states, by definition of the architecture. Requiring the x86 flag names
    /// on an ARM host would refuse every ARM host for lacking a bit that ARM
    /// does not have.
    ///
    /// This matters because the only free host that meets this project's core
    /// requirement is an ARM one.
    #[cfg(target_arch = "aarch64")]
    pub fn detect() -> Self {
        let hz = counter_frequency();
        Self {
            // Architectural, not advertised. See above.
            constant_tsc: hz.is_some(),
            nonstop_tsc: hz.is_some(),
            flags_readable: true,
            counter_hz: hz,
        }
    }

    /// Reads the flags from `/proc/cpuinfo`.
    #[cfg(not(target_arch = "aarch64"))]
    pub fn detect() -> Self {
        let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") else {
            return Self {
                constant_tsc: false,
                nonstop_tsc: false,
                flags_readable: false,
                counter_hz: None,
            };
        };
        let flags = text
            .lines()
            .find(|l| l.starts_with("flags"))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v)
            .unwrap_or("");
        Self {
            constant_tsc: flags.split_whitespace().any(|f| f == "constant_tsc"),
            nonstop_tsc: flags.split_whitespace().any(|f| f == "nonstop_tsc"),
            flags_readable: true,
            counter_hz: None,
        }
    }

    /// Nanoseconds per tick — the finest interval this counter can distinguish.
    ///
    /// On x86 the TSC runs at roughly the core's nominal frequency, so this is
    /// well under a nanosecond and nothing needs saying. On ARM the generic
    /// timer is commonly 25MHz, which is 40ns a tick — coarser than the decode
    /// being measured. That does not make it useless, because the measurement
    /// is per datagram and divided by the batch factor, but it does have to
    /// appear in the report.
    pub fn granularity_nanos(&self) -> Option<f64> {
        self.counter_hz
            .filter(|hz| *hz > 0)
            .map(|hz| 1e9 / hz as f64)
    }

    /// Whether a cycle count from this host can be converted to a duration.
    pub fn is_invariant(&self) -> bool {
        self.constant_tsc && self.nonstop_tsc
    }

    /// The sentence a report has to carry if this is not invariant.
    pub fn why_not(&self) -> Option<String> {
        if self.is_invariant() {
            return None;
        }
        if cfg!(target_arch = "aarch64") {
            return Some(
                "cntfrq_el0 read as zero or could not be read, so the generic timer's \
                 frequency is unknown and a tick count cannot be converted to a duration."
                    .to_string(),
            );
        }
        if !self.flags_readable {
            return Some(
                "/proc/cpuinfo could not be read, so constant_tsc and nonstop_tsc are \
                 unknown. A cycle count from this host cannot be converted to a duration."
                    .to_string(),
            );
        }
        let mut missing = Vec::new();
        if !self.constant_tsc {
            missing.push("constant_tsc");
        }
        if !self.nonstop_tsc {
            missing.push("nonstop_tsc");
        }
        Some(format!(
            "the host does not advertise {}. Without them the cycle counter changes rate \
             underneath the measurement, so the result is not imprecise — it is meaningless. \
             This is the normal state under WSL2, where the flags are masked.",
            missing.join(" and ")
        ))
    }
}

/// A cycle counter with a measured tick rate.
#[derive(Debug, Clone)]
pub struct Tsc {
    ticks_per_nano: f64,
    quality: TscQuality,
    /// Spread across the calibration samples, as a fraction of the best. A
    /// large value means the host was too noisy for the calibration itself to
    /// be trusted, which is worth knowing before anything else is measured.
    calibration_spread: f64,
    /// Cost of one `start`/`stop` pair, in ticks. Subtracted from short
    /// measurements; reported so the reader can judge whether it mattered.
    overhead_ticks: u64,
}

/// Opens a timed region.
///
/// `lfence` first, so that instructions issued before this point cannot drift
/// past the read.
#[inline]
pub fn start() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            core::arch::x86_64::_mm_lfence();
            core::arch::x86_64::_rdtsc()
        }
    }
    // ARM's generic timer needs an instruction barrier for the same reason x86
    // needs `lfence`: `mrs` is not ordered against surrounding work, so without
    // the `isb` the read can float out of the region being timed.
    #[cfg(target_arch = "aarch64")]
    {
        let t: u64;
        unsafe {
            core::arch::asm!(
                "isb",
                "mrs {t}, cntvct_el0",
                t = out(reg) t,
                options(nomem, nostack)
            );
        }
        t
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        0
    }
}

/// Closes a timed region.
///
/// `rdtscp` waits for prior loads to retire, and the trailing `lfence` stops
/// later instructions from being hoisted above it.
#[inline]
pub fn stop() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            let mut aux = 0u32;
            let t = core::arch::x86_64::__rdtscp(&mut aux);
            core::arch::x86_64::_mm_lfence();
            t
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let t: u64;
        unsafe {
            core::arch::asm!(
                "isb",
                "mrs {t}, cntvct_el0",
                t = out(reg) t,
                options(nomem, nostack)
            );
        }
        t
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        0
    }
}

/// The generic timer's fixed frequency, in hertz.
#[cfg(target_arch = "aarch64")]
fn counter_frequency() -> Option<u64> {
    let hz: u64;
    unsafe {
        core::arch::asm!("mrs {hz}, cntfrq_el0", hz = out(reg) hz, options(nomem, nostack));
    }
    // Zero means firmware never programmed it, which is a real and reported
    // condition on some boards. A frequency outside a plausible range means
    // something is wrong enough not to build a measurement on.
    (1_000_000..=1_000_000_000).contains(&hz).then_some(hz)
}

/// Whether this build can read a cycle counter at all.
pub const fn available() -> bool {
    cfg!(target_arch = "x86_64") || cfg!(target_arch = "aarch64")
}

impl Tsc {
    /// Measures the tick rate against the monotonic clock.
    ///
    /// Takes several samples and keeps the **fastest**, because every source of
    /// error here — a scheduler preemption, an interrupt, a migration — makes
    /// the wall-clock interval longer relative to the ticks counted, never
    /// shorter. The minimum is therefore the least contaminated sample, not
    /// merely the luckiest.
    pub fn calibrate() -> Self {
        Self::calibrate_with(Duration::from_millis(50), 5)
    }

    pub fn calibrate_with(per_sample: Duration, samples: usize) -> Self {
        let quality = TscQuality::detect();
        if !available() {
            return Self {
                ticks_per_nano: 1.0,
                quality,
                calibration_spread: f64::INFINITY,
                overhead_ticks: 0,
            };
        }

        let samples = samples.max(1);
        let mut best = f64::INFINITY;
        let mut worst: f64 = 0.0;
        for _ in 0..samples {
            let t0 = start();
            let w0 = Instant::now();
            while w0.elapsed() < per_sample {
                std::hint::spin_loop();
            }
            let elapsed = w0.elapsed();
            let t1 = stop();
            let nanos = elapsed.as_nanos() as f64;
            if nanos <= 0.0 || t1 <= t0 {
                continue;
            }
            let rate = (t1 - t0) as f64 / nanos;
            best = best.min(rate);
            worst = worst.max(rate);
        }

        if !best.is_finite() || best <= 0.0 {
            return Self {
                ticks_per_nano: 1.0,
                quality,
                calibration_spread: f64::INFINITY,
                overhead_ticks: 0,
            };
        }

        let spread = if best > 0.0 {
            (worst - best) / best
        } else {
            f64::INFINITY
        };
        let mut tsc = Self {
            ticks_per_nano: best,
            quality,
            calibration_spread: spread,
            overhead_ticks: 0,
        };
        tsc.overhead_ticks = tsc.measure_overhead();
        tsc
    }

    /// The cost of an empty `start`/`stop` pair, as the minimum over many
    /// attempts — the same reasoning as the calibration.
    fn measure_overhead(&self) -> u64 {
        let mut best = u64::MAX;
        for _ in 0..10_000 {
            let a = start();
            let b = stop();
            best = best.min(b.saturating_sub(a));
        }
        if best == u64::MAX {
            0
        } else {
            best
        }
    }

    pub fn ticks_per_nano(&self) -> f64 {
        self.ticks_per_nano
    }

    /// Nominal frequency in MHz, for the report header.
    pub fn megahertz(&self) -> f64 {
        self.ticks_per_nano * 1000.0
    }

    pub fn quality(&self) -> &TscQuality {
        &self.quality
    }

    pub fn calibration_spread(&self) -> f64 {
        self.calibration_spread
    }

    pub fn overhead_ticks(&self) -> u64 {
        self.overhead_ticks
    }

    /// Converts a tick count to nanoseconds, removing the timer's own cost.
    ///
    /// Saturating: a measurement shorter than the timer overhead reads as zero
    /// rather than wrapping to something enormous. That happens for genuinely
    /// tiny regions and is a signal the region is too small to time this way,
    /// not a number to publish.
    #[inline]
    pub fn ticks_to_nanos(&self, ticks: u64) -> u64 {
        let net = ticks.saturating_sub(self.overhead_ticks);
        (net as f64 / self.ticks_per_nano) as u64
    }

    /// Whether a duration derived from this counter means anything.
    pub fn is_trustworthy(&self) -> bool {
        available() && self.quality.is_invariant() && self.calibration_spread < 0.01
    }

    /// Every reason this counter should not be used to publish a number.
    pub fn objections(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !available() {
            out.push(
                "this build is not x86_64, so there is no cycle counter and the timing \
                 harness is inert."
                    .to_string(),
            );
            return out;
        }
        if let Some(why) = self.quality.why_not() {
            out.push(why);
        }
        if self.calibration_spread >= 0.01 {
            out.push(format!(
                "the calibration samples disagreed by {:.2}%, which is more than the 1% \
                 that would let a tick rate be quoted. The host was too noisy to establish \
                 what a tick is worth, so nothing measured with it can be published.",
                self.calibration_spread * 100.0
            ));
        }
        out
    }
}

impl fmt::Display for Tsc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:.1} MHz, timer overhead {} ticks, calibration spread {:.3}%{}",
            self.megahertz(),
            self.overhead_ticks,
            self.calibration_spread * 100.0,
            if self.is_trustworthy() {
                ""
            } else {
                " — NOT TRUSTWORTHY"
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_counter_moves_forward() {
        if !available() {
            return;
        }
        let a = start();
        let mut acc = 0u64;
        for i in 0..100_000u64 {
            acc = acc.wrapping_add(i);
        }
        let b = stop();
        assert!(b > a, "the cycle counter did not advance across real work");
        assert!(acc > 0);
    }

    #[test]
    fn calibration_lands_in_a_plausible_range() {
        // Not an assertion about *this* host's clock speed — a check that the
        // arithmetic is not off by three orders of magnitude, which is the
        // failure a calibration bug actually produces.
        if !available() {
            return;
        }
        let tsc = Tsc::calibrate_with(Duration::from_millis(20), 3);
        let mhz = tsc.megahertz();
        assert!(
            (100.0..=10_000.0).contains(&mhz),
            "calibrated to {mhz:.1} MHz, which is not a clock rate any CPU has"
        );
    }

    #[test]
    fn a_measured_sleep_comes_back_as_roughly_that_long() {
        // The end-to-end check on the conversion: ticks in, nanoseconds out,
        // against a duration the OS agrees about. Loose bounds on purpose — a
        // 2-core laptop under load will not hit a sleep deadline precisely, and
        // this is testing the arithmetic, not the scheduler.
        if !available() {
            return;
        }
        let tsc = Tsc::calibrate_with(Duration::from_millis(20), 3);
        if !tsc.ticks_per_nano.is_finite() || tsc.ticks_per_nano <= 0.0 {
            return;
        }
        let a = start();
        std::thread::sleep(Duration::from_millis(20));
        let b = stop();
        let nanos = tsc.ticks_to_nanos(b - a);
        assert!(
            (10_000_000..=200_000_000).contains(&nanos),
            "a 20ms sleep measured as {nanos}ns; the tick-to-nanosecond \
             conversion is wrong by more than scheduling can explain"
        );
    }

    #[test]
    fn an_untrustworthy_counter_says_so_and_says_why() {
        // The property that stops this host publishing a number. Under WSL2 the
        // TSC flags are masked, so `objections` must be non-empty here — and if
        // it ever is empty on a host that does advertise the flags, that is the
        // correct answer rather than a failure.
        let tsc = Tsc::calibrate_with(Duration::from_millis(10), 2);
        if tsc.is_trustworthy() {
            assert!(
                tsc.objections().is_empty(),
                "a trustworthy counter must not also carry objections"
            );
        } else {
            assert!(
                !tsc.objections().is_empty(),
                "a counter that is not trustworthy must say which of its \
                 preconditions failed, or the harness cannot explain its refusal"
            );
            for o in tsc.objections() {
                assert!(o.len() > 40, "an objection has to be an explanation: {o:?}");
            }
        }
    }

    #[test]
    fn the_overhead_is_subtracted_and_never_wraps() {
        // A region shorter than the timer itself must read as zero, not as
        // eighteen quintillion nanoseconds.
        let tsc = Tsc {
            ticks_per_nano: 3.0,
            quality: TscQuality {
                constant_tsc: true,
                nonstop_tsc: true,
                flags_readable: true,
                counter_hz: None,
            },
            calibration_spread: 0.0,
            overhead_ticks: 30,
        };
        assert_eq!(tsc.ticks_to_nanos(0), 0);
        assert_eq!(tsc.ticks_to_nanos(30), 0);
        assert_eq!(tsc.ticks_to_nanos(60), 10);
        assert_eq!(tsc.ticks_to_nanos(330), 100);
    }

    #[test]
    fn an_arm_timer_reports_its_granularity() {
        // The number an ARM report has to carry. A 25MHz generic timer is 40ns
        // a tick, which is coarser than the decode being measured — usable
        // because the measurement is per datagram, but not something to leave
        // out of a report.
        let q = TscQuality {
            constant_tsc: true,
            nonstop_tsc: true,
            flags_readable: true,
            counter_hz: Some(25_000_000),
        };
        assert_eq!(q.granularity_nanos(), Some(40.0));
        assert!(q.is_invariant());
        assert!(q.why_not().is_none());

        let x86 = TscQuality {
            constant_tsc: true,
            nonstop_tsc: true,
            flags_readable: true,
            counter_hz: None,
        };
        assert_eq!(
            x86.granularity_nanos(),
            None,
            "x86 has no fixed tick to report"
        );
    }

    #[test]
    fn missing_flags_produce_an_explanation_naming_them() {
        let q = TscQuality {
            constant_tsc: false,
            nonstop_tsc: true,
            flags_readable: true,
            counter_hz: None,
        };
        let why = q
            .why_not()
            .expect("a non-invariant TSC must explain itself");

        let both = TscQuality {
            constant_tsc: false,
            nonstop_tsc: false,
            flags_readable: true,
            counter_hz: None,
        };
        let why_both = both.why_not().unwrap();

        // The flag names are an x86 concept. On aarch64 the same value is
        // explained in terms of `cntfrq_el0`, because that is what is actually
        // missing there — and asserting the x86 wording unconditionally is what
        // failed this test on the arm64 runner the first time it ran.
        if cfg!(target_arch = "aarch64") {
            assert!(why.contains("cntfrq_el0"), "{why}");
            assert!(why_both.contains("cntfrq_el0"), "{why_both}");
        } else {
            assert!(why.contains("constant_tsc"), "{why}");
            assert!(
                !why.contains("nonstop_tsc and"),
                "it named a flag that is present"
            );
            assert!(why_both.contains("constant_tsc") && why_both.contains("nonstop_tsc"));
        }

        let good = TscQuality {
            constant_tsc: true,
            nonstop_tsc: true,
            flags_readable: true,
            counter_hz: None,
        };
        assert!(good.why_not().is_none());
        assert!(good.is_invariant());
    }
}
