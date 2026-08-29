//! Decode and encode failures.
//!
//! Every variant carries only `Copy` data — no `String`, no `Box`, no
//! formatting until something actually prints it. A malformed datagram is an
//! ordinary event on a live feed, and building an error for one must not
//! allocate, or the zero-allocation claim in the README dies on the error path
//! rather than the happy path. `crates/alloc-guard` asserts exactly that.

use core::fmt;

/// Anything that can go wrong reading or writing the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    /// The buffer is shorter than the layout requires.
    ShortBuffer { needed: usize, got: usize },
    /// The datagram or message was encoded against a different schema.
    SchemaMismatch { expected: u16, got: u16 },
    /// A decoder was pointed at a message of a different type.
    TemplateMismatch { expected: u16, got: u16 },
    /// `messageHeader.blockLength` is smaller than this schema version's root
    /// block. A newer publisher may send a *larger* block and still be read;
    /// a smaller one means fields this decoder needs are simply not there.
    BlockTooSmall {
        message: &'static str,
        needed: u16,
        got: u16,
    },
    /// A template id no version of this schema defines.
    UnknownTemplate(u16),
    /// An enum field held a value outside its `validValue` set.
    InvalidEnum { name: &'static str, value: u64 },
    /// More entries than the 16-bit count can express.
    GroupOverflow { group: &'static str },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortBuffer { needed, got } => {
                write!(f, "short buffer: need {needed} bytes, have {got}")
            }
            Self::SchemaMismatch { expected, got } => {
                write!(f, "schema mismatch: expected id {expected}, got {got}")
            }
            Self::TemplateMismatch { expected, got } => {
                write!(f, "template mismatch: expected {expected}, got {got}")
            }
            Self::BlockTooSmall {
                message,
                needed,
                got,
            } => write!(
                f,
                "{message}: root block is {got} bytes, this build needs at least {needed}"
            ),
            Self::UnknownTemplate(id) => write!(f, "unknown template id {id}"),
            Self::InvalidEnum { name, value } => {
                write!(f, "{value} is not a valid {name}")
            }
            Self::GroupOverflow { group } => {
                write!(f, "too many entries for the {group} count field")
            }
        }
    }
}

impl std::error::Error for WireError {}
