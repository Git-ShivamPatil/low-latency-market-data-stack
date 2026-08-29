//! What the handler saw, and what it can prove about it.
//!
//! Everything per-arm comes from the arbitrator, which is the only thing that
//! knows which copy of a datagram actually did work. The split matters: a single
//! "messages received" total cannot tell a healthy redundant feed from one where
//! B has been dead since startup — both look identical downstream, right up until
//! A drops something.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use book::BookDigest;

use crate::arbitration::{Arbitrator, FeedState};

/// Counters the arbitrator does not own: book-level and message-level facts.
#[derive(Debug, Default, Clone, Copy)]
pub struct HandlerStats {
    pub messages: u64,
    pub trades: u64,
    pub shares_traded: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    /// Datagrams that would not decode at all.
    pub bad_datagrams: u64,
    /// Messages that did not apply while the stream was supposed to be complete.
    /// These are real bugs.
    pub apply_errors: u64,
    /// Messages that did not apply after a gap or a mid-stream join. Expected
    /// fallout, counted separately so it cannot mask the field above.
    pub apply_errors_after_gap: u64,
    pub joined_mid_stream: bool,
}

impl HandlerStats {
    pub fn report(&self, arb: &Arbitrator, started: Instant) {
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        let a = arb.arm(0);
        let b = arb.arm(1);
        eprintln!(
            "  {:>10} msgs  {:.0} msg/s  {}  A {}/{}  B {}/{}  seq {}..{}  {} gaps  window {}/{}",
            self.messages,
            self.messages as f64 / elapsed,
            arb.state(),
            a.messages_first,
            a.datagrams,
            b.messages_first,
            b.datagrams,
            self.first_sequence,
            self.last_sequence,
            arb.gaps().len(),
            arb.max_window_used(),
            arb.window_capacity(),
        );

        // The health question is whether an arm is carrying traffic at all.
        // First-arrival *share* is not a health signal: on a quiet host the two
        // arms deliver the same bytes microseconds apart, so which one wins is
        // decided by poll order rather than by anything worth alerting on.
        for (counters, name) in [(a, "A"), (b, "B")] {
            if self.messages > 0 && counters.datagrams == 0 {
                eprintln!(
                    "  WARNING: arm {name} has received no datagrams at all. The feed is \
                     running unprotected — a single loss on the live arm is now a gap."
                );
            }
        }
        let _ = io::stderr().flush();
    }

    /// Writes what this run saw as `key=value` lines.
    ///
    /// `scripts/smoke.sh` asserts against this rather than scraping the log: a
    /// test that greps human-readable output breaks the moment someone improves
    /// the wording, and then gets "fixed" by loosening the assertion.
    pub fn write_summary(
        &self,
        path: &Path,
        arb: &Arbitrator,
        digest: BookDigest,
        elapsed_secs: f64,
    ) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let a = arb.arm(0);
        let b = arb.arm(1);
        let mut f = File::create(path)?;

        writeln!(f, "state={}", arb.state())?;
        writeln!(f, "messages={}", self.messages)?;
        writeln!(f, "first_sequence={}", self.first_sequence)?;
        writeln!(f, "last_sequence={}", self.last_sequence)?;
        writeln!(f, "joined_mid_stream={}", self.joined_mid_stream)?;

        writeln!(f, "gaps={}", arb.gaps().len())?;
        writeln!(f, "messages_missed={}", arb.messages_missed())?;
        // Every gap, so a test can assert on the exact ranges rather than a
        // count. A gap at the wrong place is not the same bug as a gap of the
        // wrong size.
        for (i, gap) in arb.gaps().iter().enumerate() {
            writeln!(f, "gap_{i}_from={}", gap.from)?;
            writeln!(f, "gap_{i}_through={}", gap.through)?;
        }

        writeln!(f, "bad_datagrams={}", self.bad_datagrams)?;
        writeln!(f, "apply_errors={}", self.apply_errors)?;
        writeln!(f, "apply_errors_after_gap={}", self.apply_errors_after_gap)?;

        for (counters, name) in [(a, "a"), (b, "b")] {
            writeln!(f, "datagrams_{name}={}", counters.datagrams)?;
            writeln!(f, "datagrams_first_{name}={}", counters.datagrams_first)?;
            writeln!(
                f,
                "datagrams_duplicate_{name}={}",
                counters.datagrams_duplicate
            )?;
            writeln!(
                f,
                "datagrams_buffered_{name}={}",
                counters.datagrams_buffered
            )?;
            writeln!(f, "messages_first_{name}={}", counters.messages_first)?;
            writeln!(f, "malformed_{name}={}", counters.malformed)?;
            writeln!(f, "bytes_{name}={}", counters.bytes)?;
        }

        writeln!(f, "reorder_window_used={}", arb.max_window_used())?;
        writeln!(f, "reorder_window_capacity={}", arb.window_capacity())?;
        writeln!(f, "trades={}", self.trades)?;
        writeln!(f, "shares_traded={}", self.shares_traded)?;
        writeln!(f, "final_top={:016x}", digest.top)?;
        writeln!(f, "final_full={:016x}", digest.full)?;
        writeln!(f, "final_orders={}", digest.orders)?;
        writeln!(f, "elapsed_seconds={elapsed_secs:.3}")?;
        f.flush()
    }

    /// A run is clean when the stream was complete and everything applied.
    ///
    /// `Gapped` is deliberately fatal here even though the handler kept running:
    /// the whole point of naming the range is that the book downstream of it is
    /// not trustworthy, and an exit code that shrugged at that would undo the
    /// honesty.
    pub fn is_clean(&self, arb: &Arbitrator) -> bool {
        arb.state() != FeedState::Gapped
            && arb.gaps().is_empty()
            && self.bad_datagrams == 0
            && self.apply_errors == 0
    }
}
