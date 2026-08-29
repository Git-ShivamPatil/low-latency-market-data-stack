//! What the handler saw, per arm.
//!
//! The per-arm split is the point. A single "messages received" counter cannot
//! tell a healthy redundant feed from one where B has been silently dead since
//! startup — both look identical downstream, right up until A drops a packet.
//! Counting first-arrivals and duplicates separately makes a dead arm visible
//! immediately: its first-arrival count sits at zero while A's climbs.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use book::BookDigest;

#[derive(Debug, Default, Clone, Copy)]
pub struct HandlerStats {
    /// Datagrams read, indexed by arm: 0 is A, 1 is B.
    pub datagrams: [u64; 2],
    /// Messages this arm delivered first. On a healthy pair both climb.
    pub first_arrivals: [u64; 2],
    /// Messages this arm delivered that the other arm had already delivered.
    pub duplicates: [u64; 2],
    pub bytes: u64,
    pub messages: u64,
    pub trades: u64,
    pub shares_traded: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    /// Times the stream jumped forward.
    pub gaps: u64,
    pub messages_missed: u64,
    /// Datagrams that would not decode.
    pub bad_datagrams: u64,
    /// Messages that did not apply to the book on an otherwise complete stream.
    /// These are real bugs.
    pub apply_errors: u64,
    /// Messages that did not apply after a gap. Expected fallout, not a bug —
    /// counted separately so it cannot mask the previous field.
    pub apply_errors_after_gap: u64,
    pub joined_mid_stream: bool,
}

impl HandlerStats {
    pub fn report(&self, started: Instant) {
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        eprintln!(
            "  {:>10} msgs  {:.0} msg/s  A {}/{}  B {}/{}  seq {}..{}  {} gaps",
            self.messages,
            self.messages as f64 / elapsed,
            self.first_arrivals[0],
            self.datagrams[0],
            self.first_arrivals[1],
            self.datagrams[1],
            self.first_sequence,
            self.last_sequence,
            self.gaps,
        );
        if self.messages > 0 && self.first_arrivals[1] == 0 {
            eprintln!(
                "  note: arm B has delivered nothing first. Either it is dead, or it is \
                 consistently behind A — both are worth knowing before relying on redundancy."
            );
        }
        let _ = io::stderr().flush();
    }
    /// Writes what this run saw as `key=value` lines.
    ///
    /// `scripts/smoke.sh` asserts against this rather than scraping the log:
    /// a test that greps human-readable output breaks the moment someone
    /// improves the wording, and then gets "fixed" by loosening the assertion.
    pub fn write_summary(
        &self,
        path: &Path,
        digest: BookDigest,
        elapsed_secs: f64,
    ) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let mut f = File::create(path)?;
        writeln!(f, "messages={}", self.messages)?;
        writeln!(f, "first_sequence={}", self.first_sequence)?;
        writeln!(f, "last_sequence={}", self.last_sequence)?;
        writeln!(f, "gaps={}", self.gaps)?;
        writeln!(f, "messages_missed={}", self.messages_missed)?;
        writeln!(f, "bad_datagrams={}", self.bad_datagrams)?;
        writeln!(f, "apply_errors={}", self.apply_errors)?;
        writeln!(f, "apply_errors_after_gap={}", self.apply_errors_after_gap)?;
        writeln!(f, "joined_mid_stream={}", self.joined_mid_stream)?;
        writeln!(f, "datagrams_a={}", self.datagrams[0])?;
        writeln!(f, "datagrams_b={}", self.datagrams[1])?;
        writeln!(f, "first_arrivals_a={}", self.first_arrivals[0])?;
        writeln!(f, "first_arrivals_b={}", self.first_arrivals[1])?;
        writeln!(f, "duplicates_a={}", self.duplicates[0])?;
        writeln!(f, "duplicates_b={}", self.duplicates[1])?;
        writeln!(f, "bytes={}", self.bytes)?;
        writeln!(f, "trades={}", self.trades)?;
        writeln!(f, "shares_traded={}", self.shares_traded)?;
        writeln!(f, "final_top={:016x}", digest.top)?;
        writeln!(f, "final_full={:016x}", digest.full)?;
        writeln!(f, "final_orders={}", digest.orders)?;
        writeln!(f, "elapsed_seconds={elapsed_secs:.3}")?;
        f.flush()
    }
}
