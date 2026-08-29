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
use crate::recovery::RecoveryStats;

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
    /// Held messages discarded during a recovery without evidence that the
    /// recovery covered them. Should always be zero; see
    /// `RecoveryBuffer::drain_contiguous`.
    pub unverified_drops: u64,
    /// Messages that did not apply after a gap or a mid-stream join. Expected
    /// fallout, counted separately so it cannot mask the field above.
    pub apply_errors_after_gap: u64,
    pub joined_mid_stream: bool,
    /// Recoveries that returned the handler to a trustworthy book.
    pub recoveries: u64,
    /// Recovery attempts that ran out of buffer or time.
    pub recovery_failures: u64,
    /// Snapshot fragments ignored because they predated the gap.
    pub snapshots_discarded: u64,
    /// Which recovery attempt a replay has already been requested for, so one
    /// gap does not produce a stream of duplicate requests.
    pub replay_requested: u64,
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
            arb.gap_count(),
            arb.max_window_used(),
            arb.window_capacity(),
        );
        if self.recoveries > 0 || self.recovery_failures > 0 {
            eprintln!(
                "  recovered {} times, {} failed",
                self.recoveries, self.recovery_failures
            );
        }

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

        writeln!(f, "gaps={}", arb.gap_count())?;
        writeln!(f, "messages_missed={}", arb.messages_missed())?;
        // Every gap the log kept, so a test can assert on the exact ranges
        // rather than a count: a gap at the wrong place is not the same bug as
        // a gap of the wrong size. `gaps=` above is the true total, which can
        // exceed this list.
        for (i, gap) in arb.gaps().iter().enumerate() {
            writeln!(f, "gap_{i}_from={}", gap.from)?;
            writeln!(f, "gap_{i}_through={}", gap.through)?;
        }

        writeln!(f, "bad_datagrams={}", self.bad_datagrams)?;
        writeln!(f, "apply_errors={}", self.apply_errors)?;
        writeln!(f, "unverified_drops={}", self.unverified_drops)?;
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
            // Counted since milestone 3 and reported by nothing until now. A
            // datagram dropped here is real message loss that no gap covers,
            // and a run that did it looked clean.
            writeln!(
                f,
                "dropped_window_full_{name}={}",
                counters.dropped_window_full
            )?;
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

    /// Recovery counters, written alongside the rest of the summary.
    pub fn write_recovery(&self, path: &Path, r: RecoveryStats, resyncs: u64) -> io::Result<()> {
        use std::fs::OpenOptions;
        let mut f = OpenOptions::new().append(true).open(path)?;
        writeln!(f, "recoveries={}", self.recoveries)?;
        writeln!(f, "recovery_failures={}", self.recovery_failures)?;
        writeln!(f, "recovery_attempts={}", r.attempts)?;
        writeln!(f, "recovery_datagrams_buffered={}", r.datagrams_buffered)?;
        writeln!(f, "recovery_messages_replayed={}", r.messages_replayed)?;
        writeln!(f, "recovery_messages_skipped={}", r.messages_skipped)?;
        writeln!(f, "snapshots_seen={}", r.snapshots_seen)?;
        writeln!(f, "snapshots_discarded={}", self.snapshots_discarded)?;
        // The worst case, not the most recent: a threshold assertion that
        // watched only the last recovery would miss the one that blew it.
        writeln!(f, "recovery_worst_millis={}", r.worst_recovery_millis)?;
        writeln!(f, "recovery_last_millis={}", r.last_recovery_millis)?;
        writeln!(f, "resyncs={resyncs}")?;
        writeln!(f, "recovered_by_replay={}", r.by_replay)?;
        writeln!(f, "recovered_by_snapshot={}", r.by_snapshot)?;
        writeln!(f, "replay_refused={}", r.replay_refused)?;
        writeln!(f, "replay_messages={}", r.replay_messages)?;
        writeln!(
            f,
            "still_recovering={}",
            r.attempts > self.recoveries + self.recovery_failures
        )?;
        f.flush()
    }

    /// A run is clean when it ends holding a book that can be trusted.
    ///
    /// A gap is no longer automatically fatal — that is what the snapshot cycle
    /// changed. What matters is whether the run *ended* with one outstanding.
    ///
    /// Note what this deliberately does not do: compare the gap count against
    /// the recovery count. Several gaps can open while a single recovery is in
    /// flight, and one snapshot closes all of them, so requiring one recovery
    /// per gap would fail a run that behaved perfectly. `Gapped` is cleared only
    /// by an actual resync, so the state is the honest signal.
    ///
    /// `recovering` is passed in because a run that stops while still holding
    /// traffic never finished the recovery it started, and its books are as
    /// stale as the moment the gap opened.
    pub fn is_clean(&self, arb: &Arbitrator, recovering: bool) -> bool {
        arb.state() != FeedState::Gapped
            && !recovering
            && self.recovery_failures == 0
            && self.bad_datagrams == 0
            && self.apply_errors == 0
            // A datagram dropped because the reorder window was full is message
            // loss that no gap range covers, so nothing downstream reports it.
            // It was counted from milestone 3 and read by nothing, which is the
            // definition of a silent failure.
            && arb.arm(0).dropped_window_full == 0
            && arb.arm(1).dropped_window_full == 0
            // Held traffic discarded without evidence anything applied it.
            && self.unverified_drops == 0
    }
}
