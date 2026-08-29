//! The binary wire format for the market-data feed.
//!
//! Everything in [`generated`] is produced from `schema/market-data.xml` by
//! `schema/codegen.py`, which emits this module and the matching C++ header
//! from the same parse. No field offset is written by hand anywhere in the
//! repository, in either language.
//!
//! # Framing
//!
//! ```text
//! datagram := packetHeader , message*
//! message  := messageHeader , rootBlock , group?
//! ```
//!
//! A message carries no sequence number. The sequence of message `i` in a
//! datagram is `packetHeader.firstSequence + i`. That is deliberate: the feed
//! packs many messages behind one header, so per-message framing stays at 8
//! bytes and the publisher reads its clock once per datagram rather than once
//! per message. See `docs/WIRE.md` for why the batch factor decides whether
//! the throughput target is reachable at all.
//!
//! # Allocation
//!
//! Decoding borrows. [`PacketReader`] and every `*Decoder` are views over the
//! caller's buffer, and encoding writes into a caller-supplied slice. Nothing
//! in this crate allocates, on the happy path or the error path.
//!
//! # Example
//!
//! ```
//! use wire::{Message, PacketReader, PacketWriter, Side};
//!
//! let mut buf = [0u8; 1500];
//! let mut w = PacketWriter::new(&mut buf, 0, 0, 42, 1_700_000_000_000_000_000)?;
//! w.add_order(1001, 1_012_500, 250, 7, Side::Bid)?;
//! w.add_order(1002, 1_012_600, 100, 7, Side::Ask)?;
//! let len = w.finish();
//!
//! let reader = PacketReader::new(&buf[..len])?;
//! assert_eq!(reader.header().message_count(), 2);
//!
//! let mut seen = Vec::new();
//! for m in reader.messages() {
//!     let (seq, msg) = m?;
//!     if let Message::AddOrder(a) = msg {
//!         seen.push((seq, a.order_id(), a.price()));
//!     }
//! }
//! assert_eq!(seen, vec![(42, 1001, 1_012_500), (43, 1002, 1_012_600)]);
//! # Ok::<(), wire::WireError>(())
//! ```

pub mod error;
pub mod generated;

pub use error::WireError;
pub use generated::*;

/// Convert a wire price to a human-readable decimal string.
///
/// Only for logging, reports and test failure messages — never the hot path.
/// Prices are fixed-point integers on the wire and stay that way in the books;
/// this exists so a failing assertion prints `101.2500` instead of `1012500`.
pub fn format_price(ticks: i64) -> String {
    let scale = 10i64.pow((-(PRICE_EXPONENT as i32)) as u32);
    let sign = if ticks < 0 { "-" } else { "" };
    let abs = ticks.unsigned_abs();
    let whole = abs / scale as u64;
    let frac = abs % scale as u64;
    let width = (-(PRICE_EXPONENT as i32)) as usize;
    format!("{sign}{whole}.{frac:0width$}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_formatting_round_trips_the_exponent() {
        assert_eq!(format_price(1_012_500), "101.2500");
        assert_eq!(format_price(-1_012_500), "-101.2500");
        assert_eq!(format_price(1), "0.0001");
        assert_eq!(format_price(0), "0.0000");
    }

    #[test]
    fn schema_identity_is_what_the_schema_says() {
        assert_eq!(SCHEMA_ID, 1);
        assert_eq!(SCHEMA_VERSION, 1);
        assert_eq!(PRICE_EXPONENT, -4);
        assert_eq!(PACKET_HEADER_LEN, 24);
        assert_eq!(MESSAGE_HEADER_LEN, 8);
        assert_eq!(GROUP_SIZE_ENCODING_LEN, 4);
    }
}
