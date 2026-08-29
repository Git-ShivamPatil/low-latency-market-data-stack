//! Milestone 3's verification: does publishing twice actually buy anything?
//!
//! These drive the arbitrator directly rather than through sockets. That is not
//! a shortcut — it is the only way to inject an exact, repeatable loss pattern
//! and then assert on the exact set of gaps that came out. Two processes over
//! real UDP cannot tell you *which* datagrams the kernel dropped, so a
//! cross-process test can only observe that something went wrong, never that
//! the right thing went right. `scripts/smoke.sh` covers the socket path; this
//! covers the logic.
//!
//! # The arithmetic the milestone's spec got wrong
//!
//! The plan says: "with 2% independent loss on each arm, the arbitrated output
//! stream has ZERO gaps across 10M messages."
//!
//! That is not achievable, and not because of any defect. With genuinely
//! independent loss at rate `p` per arm, a datagram is lost on both arms with
//! probability `p²`. At `p = 0.02` that is 0.04%, and 10M messages at 32 per
//! datagram is ~312K datagrams — about 125 total losses that no amount of
//! redundancy can recover, because both copies are gone.
//!
//! So the claim is split into the two things it was conflating:
//!
//! - **Single-arm loss costs nothing.** Tested under `Exclusive` loss, where a
//!   dropped datagram is dropped on exactly one arm. Zero gaps here is a
//!   property of the code, and it either holds or it does not.
//! - **Double loss is detected, not silently skipped.** Tested under
//!   `Independent` loss by predicting exactly which datagrams vanish entirely
//!   and requiring the reported gaps to match that set, and under `Correlated`
//!   loss where every drop is a double loss.
//!
//! Both are stronger statements than the original, and both are true.

use feed_handler::arbitration::{Accepted, Arbitrator, FeedState, Gap};
use wire::{PacketWriter, Side};

/// SplitMix64. Six lines rather than a dependency, and identical on every
/// platform so a failure here reproduces exactly.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn chance(&mut self, p: f64) -> bool {
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// Each arm decides on its own. Some datagrams die on both.
    Independent,
    /// Exactly one arm, chosen fairly. Redundancy can always recover.
    Exclusive,
    /// Both arms. Redundancy can never recover.
    Correlated,
}

const BATCH: u16 = 32;
/// How far B trails A, in datagrams.
///
/// Without a lag the two copies arrive adjacently and a loss on A is filled by
/// B before anything else is delivered — which never exercises the reorder
/// window at all. Real redundant feeds are not synchronised like that, and the
/// whole difficulty of arbitration is that the replacement arrives *after*
/// datagrams the surviving arm already delivered.
const LAG: usize = 3;

/// One datagram body, built once. Only the header changes per sequence, and
/// arbitration only ever reads the header.
fn template() -> Vec<u8> {
    let mut buf = vec![0u8; 2048];
    let mut w = PacketWriter::new(&mut buf, 0, 0, 1, 0).unwrap();
    for i in 0..BATCH {
        w.add_order(u64::from(i), 1_000_000, 1, 1, Side::Bid)
            .unwrap();
    }
    let n = w.finish();
    buf.truncate(n);
    buf
}

fn stamp(dst: &mut [u8], template: &[u8], channel: u8, first: u64) {
    dst[..template.len()].copy_from_slice(template);
    wire::encode_packet_header(dst, channel, 0, first, 0).unwrap();
    wire::patch_message_count(dst, BATCH).unwrap();
}

struct Outcome {
    delivered: u64,
    gaps: Vec<Gap>,
    state: FeedState,
    first_arrivals: [u64; 2],
    /// Datagrams the simulator killed on both arms — the losses redundancy
    /// cannot cover, predicted independently of what the arbitrator reported.
    doomed: Vec<u64>,
    window_used: usize,
}

/// Runs `datagrams` datagrams through the arbitrator under a loss model.
fn simulate(datagrams: usize, loss: Loss, rate: f64, seed: u64, window: usize) -> Outcome {
    let tmpl = template();
    let mut arb = Arbitrator::new(window, 2048);
    let mut rng = Rng::new(seed);

    let mut buf_a = vec![0u8; tmpl.len()];
    let mut buf_b = vec![0u8; tmpl.len()];
    // B's copy of datagram i is offered at step i + LAG.
    let mut pending: Vec<Option<u64>> = vec![None; LAG + 1];
    let mut delivered = 0u64;
    let mut doomed = Vec::new();

    let deliver = |arb: &mut Arbitrator, arm: u8, buf: &[u8]| -> u64 {
        let mut count = 0u64;
        if let Accepted::Ready { count: c, .. } = arb.accept(arm, buf) {
            count += u64::from(c);
        }
        arb.drain_ready(|_f, c, _bytes| count += u64::from(c));
        count
    };

    for i in 0..datagrams + LAG {
        // A's copy of datagram i.
        if i < datagrams {
            let first = 1 + (i as u64) * u64::from(BATCH);
            let (drop_a, drop_b) = match loss {
                Loss::Independent => (rng.chance(rate), rng.chance(rate)),
                Loss::Exclusive => {
                    if rng.chance(rate) {
                        if rng.chance(0.5) {
                            (true, false)
                        } else {
                            (false, true)
                        }
                    } else {
                        (false, false)
                    }
                }
                Loss::Correlated => {
                    let lost = rng.chance(rate);
                    (lost, lost)
                }
            };
            if drop_a && drop_b {
                doomed.push(first);
            }
            if !drop_a {
                stamp(&mut buf_a, &tmpl, 0, first);
                delivered += deliver(&mut arb, 0, &buf_a);
            }
            pending[i % (LAG + 1)] = (!drop_b).then_some(first);
        }
        // B's copy of datagram i - LAG.
        if i >= LAG {
            let slot = (i - LAG) % (LAG + 1);
            if let Some(first) = pending[slot].take() {
                stamp(&mut buf_b, &tmpl, 1, first);
                delivered += deliver(&mut arb, 1, &buf_b);
            }
        }
    }

    // The feed has ended. Anything still held behind a hole is not coming.
    while arb.declare_gap_if_stalled().is_some() {
        arb.drain_ready(|_f, c, _b| delivered += u64::from(c));
    }

    Outcome {
        delivered,
        gaps: arb.gaps().to_vec(),
        state: arb.state(),
        first_arrivals: [arb.arm(0).messages_first, arb.arm(1).messages_first],
        doomed,
        window_used: arb.max_window_used(),
    }
}

/// 10M messages, and every loss recoverable. This is the milestone's headline.
#[test]
fn single_arm_loss_costs_nothing_across_ten_million_messages() {
    const MESSAGES: u64 = 10_000_000;
    let datagrams = (MESSAGES / u64::from(BATCH)) as usize;

    let out = simulate(datagrams, Loss::Exclusive, 0.02, 0xA11CE, 64);

    assert!(
        out.doomed.is_empty(),
        "exclusive loss must never kill both copies; the simulator is wrong"
    );
    assert_eq!(
        out.gaps,
        Vec::new(),
        "with every loss recoverable from the other arm, there is no excuse for a gap"
    );
    assert_eq!(out.state, FeedState::Live);
    assert_eq!(
        out.delivered,
        datagrams as u64 * u64::from(BATCH),
        "every message must be delivered exactly once"
    );
    assert!(
        out.first_arrivals[0] > 0 && out.first_arrivals[1] > 0,
        "both arms must contribute first arrivals, saw A={} B={}",
        out.first_arrivals[0],
        out.first_arrivals[1]
    );
    // Roughly 1% of datagrams are dropped on A and 1% on B, so each arm should
    // be doing real work rather than one carrying everything.
    let total: u64 = out.first_arrivals.iter().sum();
    let share = out.first_arrivals[1] as f64 / total as f64;
    assert!(
        share > 0.001,
        "arm B delivered only {:.4}% of messages first; the lag means A wins most \
         races, but B must still be covering A's losses",
        share * 100.0
    );
    println!(
        "10M messages, 2% exclusive loss: 0 gaps, A first {} / B first {}, window peak {}/{}",
        out.first_arrivals[0], out.first_arrivals[1], out.window_used, 64
    );
}

/// The honest version of "2% independent loss": some datagrams die outright, and
/// every one of them must be reported as a gap covering exactly its range.
#[test]
fn independent_loss_reports_exactly_the_datagrams_that_died_on_both_arms() {
    const MESSAGES: u64 = 10_000_000;
    let datagrams = (MESSAGES / u64::from(BATCH)) as usize;

    let out = simulate(datagrams, Loss::Independent, 0.02, 0xB0B, 64);

    assert!(
        !out.doomed.is_empty(),
        "at 2% independent loss over {datagrams} datagrams, some must die on both \
         arms — if none did, the loss model is not independent"
    );

    // Each doomed datagram covers BATCH sequences. The arbitrator does not know
    // datagram boundaries in a hole, so consecutive doomed datagrams merge into
    // one reported gap. Compare the covered sequence sets instead of the counts.
    let mut expected: Vec<u64> = Vec::new();
    for first in &out.doomed {
        for i in 0..u64::from(BATCH) {
            expected.push(first + i);
        }
    }
    let mut reported: Vec<u64> = Vec::new();
    for gap in &out.gaps {
        for s in gap.from..=gap.through {
            reported.push(s);
        }
    }
    expected.sort_unstable();
    reported.sort_unstable();
    assert_eq!(
        reported, expected,
        "the reported gaps must cover exactly the sequences that were lost on \
         both arms — no more (over-reporting hides recoveries) and no less \
         (under-reporting is silent data loss)"
    );

    assert_eq!(out.state, FeedState::Gapped);
    assert_eq!(
        out.delivered + expected.len() as u64,
        datagrams as u64 * u64::from(BATCH),
        "everything not lost must have been delivered"
    );

    let pct = out.doomed.len() as f64 / datagrams as f64 * 100.0;
    println!(
        "10M messages, 2% independent loss: {} datagrams lost on both arms ({pct:.3}%, \
         predicted ~0.04%), {} gaps reported covering exactly those {} sequences",
        out.doomed.len(),
        out.gaps.len(),
        expected.len()
    );
}

/// Loss on the same sequences on both arms. Redundancy cannot help, so the only
/// acceptable behaviour is to say so and name the range.
#[test]
fn correlated_loss_is_named_rather_than_silently_skipped() {
    let out = simulate(2_000, Loss::Correlated, 0.05, 0xC0FFEE, 64);

    assert_eq!(out.state, FeedState::Gapped);
    assert!(!out.gaps.is_empty(), "correlated loss must produce gaps");

    let mut expected: Vec<u64> = Vec::new();
    for first in &out.doomed {
        for i in 0..u64::from(BATCH) {
            expected.push(first + i);
        }
    }
    let mut reported: Vec<u64> = Vec::new();
    for gap in &out.gaps {
        for s in gap.from..=gap.through {
            reported.push(s);
        }
    }
    expected.sort_unstable();
    reported.sort_unstable();
    assert_eq!(
        reported, expected,
        "every lost sequence must appear in a named range"
    );

    // And the ranges must be real ranges, not a count.
    for gap in &out.gaps {
        assert!(
            gap.from <= gap.through,
            "a gap must not be inverted: {gap:?}"
        );
        assert!(gap.messages() > 0);
    }
    println!(
        "2% of 2000 datagrams lost on both arms: {} gaps naming {} sequences",
        out.gaps.len(),
        expected.len()
    );
}

/// A clean feed must not invent gaps, and must not double count.
#[test]
fn a_lossless_feed_delivers_everything_once_and_reports_nothing() {
    let out = simulate(5_000, Loss::Exclusive, 0.0, 1, 64);
    assert_eq!(out.gaps, Vec::new());
    assert_eq!(out.state, FeedState::Live);
    assert_eq!(out.delivered, 5_000 * u64::from(BATCH));
    assert!(out.doomed.is_empty());
    assert_eq!(
        out.window_used, 0,
        "with nothing lost, nothing should ever need buffering"
    );
}

/// The window is a bound, and a bound that is never enforced is not a bound.
#[test]
fn a_window_too_small_for_the_lag_still_terminates_and_reports() {
    // LAG is 3, so a window of 1 cannot hold the reordering. The arbitrator must
    // give up on holes rather than stall or lose track.
    let out = simulate(1_000, Loss::Exclusive, 0.05, 0xDEAD, 1);
    assert!(
        !out.gaps.is_empty(),
        "a window smaller than the reordering must force gaps, not hide them"
    );
    // Even then, nothing may be delivered twice or out of order: delivered plus
    // missed must still account for the whole stream.
    let missed: u64 = out.gaps.iter().map(Gap::messages).sum();
    assert_eq!(
        out.delivered + missed,
        1_000 * u64::from(BATCH),
        "every sequence must be either delivered or accounted for as missing"
    );
}
