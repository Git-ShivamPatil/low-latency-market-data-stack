//! A single-producer single-consumer ring in shared memory, shared with C++.
//!
//! # Why this shape
//!
//! SPSC is the only lock-free queue whose correctness argument fits in a
//! paragraph, and the paragraph is this: the producer owns `writeIndex`, the
//! consumer owns `readIndex`, and the release store that publishes an index is
//! what makes the slot's bytes visible to the acquire load that reads it. The
//! moment a second producer appears, that argument is gone — which is why the
//! order path uses four rings rather than one with a fan-in.
//!
//! Indices are free-running 64-bit counters that are **never wrapped**.
//! `write - read` is the depth, so full and empty are not the same state and
//! the ring needs no spare slot to tell them apart. At a billion messages a
//! second it would take 584 years to overflow one.
//!
//! # The layout is not written here
//!
//! Every offset comes from `wire::layout::ring_header`, generated from
//! `schema/market-data.xml`. The C++ side reads the same constants from the same
//! schema. A ring whose two ends disagree about where `writeIndex` lives does
//! not fail loudly — it reads a number that was never written, which is the
//! failure mode this whole arrangement exists to make impossible.
//!
//! # On `unsafe`, and one honest caveat
//!
//! Two processes read and write the same pages. That is what a shared-memory
//! ring is, and Rust's aliasing rules have nothing to say about another
//! process — no `&mut` this program holds can be invalidated by one.
//!
//! Within the formal memory model, the slot bytes are a data race: they are
//! copied non-atomically by one process and read non-atomically by another,
//! ordered only by the release/acquire pair on the indices. Every shared-memory
//! ring in existence is built this way, and on every architecture this project
//! targets the pair compiles to exactly the fence the argument needs. The
//! alternative — treating each slot byte as an `AtomicU8` — is correct by the
//! letter of the model and defeats the vectorised copy that makes the ring worth
//! having. The choice is deliberate and it is written down rather than left for
//! a reader to notice.
//!
//! See `docs/ORDER-PATH.md`.

// The unsafety is `mmap`, `munmap`, and forming references to atomics at known
// offsets inside the mapping. It is confined to this file, every block names
// what it is relying on, and nothing above the `Producer`/`Consumer` API is
// unsafe to call.
#![allow(unsafe_code)]

use std::ffi::CString;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use wire::layout::{ring_header as hdr, ring_slot as slot};

/// Ring layout version, separate from the schema version. Bump it when the
/// meaning of the bytes changes rather than when a message is added.
pub const RING_VERSION: u32 = 1;

/// `"MDSTKRNG"` in ASCII, so `head -c8` on the file says what it is.
pub const RING_MAGIC: u64 = 0x474e_524b_5453_444d;

/// Slots are cache-line aligned so two adjacent slots are never on one line —
/// the producer writing slot *n* must not invalidate the line the consumer is
/// reading slot *n-1* from.
pub const SLOT_ALIGN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError {
    /// `capacity` was not a power of two, so the index-to-slot map could not be
    /// a mask.
    CapacityNotPowerOfTwo(u32),
    CapacityZero,
    /// `slot_size` was not a multiple of [`SLOT_ALIGN`], or was too small to
    /// hold the per-slot prefix plus a byte.
    BadSlotSize(u32),
    /// The file is not a ring, or is a ring this build does not understand.
    /// Refused rather than guessed at: a mis-read header produces a queue that
    /// appears to work and delivers nothing.
    BadMagic(u64),
    BadVersion(u32),
    /// The file is shorter than its own header says it should be.
    Truncated {
        need: usize,
        got: usize,
    },
    Io(i32),
}

impl fmt::Display for RingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityNotPowerOfTwo(n) => {
                write!(f, "capacity {n} is not a power of two")
            }
            Self::CapacityZero => write!(f, "capacity must be at least 1"),
            Self::BadSlotSize(n) => write!(
                f,
                "slot size {n} must be a multiple of {SLOT_ALIGN} and larger than {}",
                slot::LEN
            ),
            Self::BadMagic(m) => write!(f, "not a ring: magic {m:#018x}"),
            Self::BadVersion(v) => write!(
                f,
                "ring layout version {v}, this build speaks {RING_VERSION}"
            ),
            Self::Truncated { need, got } => {
                write!(f, "ring file is {got} bytes; its header describes {need}")
            }
            Self::Io(e) => write!(f, "{}", std::io::Error::from_raw_os_error(*e)),
        }
    }
}

impl std::error::Error for RingError {}

impl From<RingError> for std::io::Error {
    fn from(e: RingError) -> Self {
        match e {
            RingError::Io(code) => std::io::Error::from_raw_os_error(code),
            other => std::io::Error::new(std::io::ErrorKind::InvalidData, other),
        }
    }
}

/// Why a push did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// The consumer has not kept up. **Not something to swallow:** the caller
    /// must tell whoever sent the order, because blocking stalls the session and
    /// dropping loses an order silently.
    Full,
    /// The message is larger than a slot. A sizing mistake, not a runtime
    /// condition — it cannot be retried.
    TooLarge { len: usize, capacity: usize },
    /// The fill closure declined to write. Reported rather than folded into
    /// `TooLarge`, because inventing a length for an error message is how a log
    /// line ends up describing something that never happened.
    Abandoned,
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "the ring is full"),
            Self::TooLarge { len, capacity } => {
                write!(
                    f,
                    "a {len}-byte message does not fit a {capacity}-byte slot"
                )
            }
            Self::Abandoned => write!(f, "the fill closure wrote nothing"),
        }
    }
}

impl std::error::Error for PushError {}

// ---------------------------------------------------------------------------
// the mapping
// ---------------------------------------------------------------------------

/// An `mmap`'d region that unmaps itself.
struct Mapping {
    ptr: *mut u8,
    len: usize,
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` are exactly what `mmap` returned and nothing else
        // unmaps them; `Mapping` is not `Clone`.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

impl fmt::Debug for Mapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mapping").field("len", &self.len).finish()
    }
}

fn last_errno() -> i32 {
    std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EIO)
}

fn c_path(path: &Path) -> Result<CString, RingError> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| RingError::Io(libc::EINVAL))
}

/// Opens `path`, sizing it to `len` when `create` is set, and maps it shared.
fn map_file(path: &Path, len: usize, create: bool) -> Result<Mapping, RingError> {
    let cpath = c_path(path)?;
    let flags = if create {
        libc::O_RDWR | libc::O_CREAT
    } else {
        libc::O_RDWR
    };
    // SAFETY: `cpath` is NUL-terminated and outlives the call.
    let fd = unsafe { libc::open(cpath.as_ptr(), flags, 0o600 as libc::c_uint) };
    if fd < 0 {
        return Err(RingError::Io(last_errno()));
    }

    // Everything below has to close `fd` on the way out, so the result is
    // computed first and the close happens once.
    let result = (|| {
        if create {
            // SAFETY: `fd` is open for writing.
            if unsafe { libc::ftruncate(fd, len as libc::off_t) } != 0 {
                return Err(RingError::Io(last_errno()));
            }
        }
        // SAFETY: `fd` is open; `st_size` is only read on success.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } != 0 {
            return Err(RingError::Io(last_errno()));
        }
        let got = st.st_size as usize;
        if got < len {
            return Err(RingError::Truncated { need: len, got });
        }
        // SAFETY: `fd` is open for read and write and is at least `len` bytes
        // long, which `fstat` has just established.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(RingError::Io(last_errno()));
        }
        Ok(Mapping {
            ptr: ptr.cast::<u8>(),
            len,
        })
    })();

    // The mapping outlives the descriptor; that is what `MAP_SHARED` means.
    // SAFETY: `fd` is open and nothing else holds it.
    unsafe {
        libc::close(fd);
    }
    result
}

// ---------------------------------------------------------------------------
// the ring
// ---------------------------------------------------------------------------

/// The mapping plus its validated geometry. Take a [`Producer`] or a
/// [`Consumer`] from it; a process holds exactly one role.
#[derive(Debug)]
pub struct Ring {
    map: Mapping,
    capacity: u64,
    slot_size: usize,
}

// SAFETY: `Ring` owns its mapping and hands out no interior references. Moving
// it to another thread is fine; sharing it is not, which is why `Sync` is not
// implemented — the SPSC argument needs exactly one thread per role.
unsafe impl Send for Ring {}

fn check_geometry(capacity: u32, slot_size: u32) -> Result<(), RingError> {
    if capacity == 0 {
        return Err(RingError::CapacityZero);
    }
    if !capacity.is_power_of_two() {
        return Err(RingError::CapacityNotPowerOfTwo(capacity));
    }
    let s = slot_size as usize;
    if !s.is_multiple_of(SLOT_ALIGN) || s <= slot::LEN {
        return Err(RingError::BadSlotSize(slot_size));
    }
    Ok(())
}

impl Ring {
    /// Bytes a ring of this geometry occupies on disk.
    pub fn file_size(capacity: u32, slot_size: u32) -> usize {
        hdr::LEN + capacity as usize * slot_size as usize
    }

    /// Creates or re-creates the ring at `path`, zeroing the indices.
    ///
    /// The creator is whichever side starts first, and it is the only side that
    /// may write the header. A ring created twice concurrently is a deployment
    /// mistake this cannot detect, which is why `docs/ORDER-PATH.md` names the
    /// creator for each of the four rings.
    pub fn create(path: &Path, capacity: u32, slot_size: u32) -> Result<Self, RingError> {
        check_geometry(capacity, slot_size)?;
        let len = Self::file_size(capacity, slot_size);
        let map = map_file(path, len, true)?;

        // Zero the header before anything else so a reopened file cannot carry
        // an old index into a new ring.
        // SAFETY: the mapping is at least `hdr::LEN` bytes.
        unsafe {
            std::ptr::write_bytes(map.ptr, 0, hdr::LEN);
        }
        let ring = Self {
            map,
            capacity: capacity as u64,
            slot_size: slot_size as usize,
        };
        ring.store_u64(hdr::MAGIC, RING_MAGIC);
        ring.store_u32(hdr::VERSION, RING_VERSION);
        ring.store_u32(hdr::SLOT_SIZE, slot_size);
        ring.store_u32(hdr::CAPACITY, capacity);
        // The magic is written first in program order but published last: the
        // release store on writeIndex is what a joining process's acquire load
        // pairs with. Zero here is not a no-op, it is the publication.
        ring.write_index().store(0, Ordering::Release);
        ring.read_index().store(0, Ordering::Release);
        Ok(ring)
    }

    /// Opens a ring somebody else created, taking its geometry from the header.
    pub fn open(path: &Path) -> Result<Self, RingError> {
        // Map the header alone first: the geometry that says how big the file
        // should be is inside it, so it cannot be known before reading it.
        let probe = map_file(path, hdr::LEN, false)?;
        let head = Self {
            map: probe,
            capacity: 0,
            slot_size: 0,
        };
        let magic = head.load_u64(hdr::MAGIC);
        if magic != RING_MAGIC {
            return Err(RingError::BadMagic(magic));
        }
        let version = head.load_u32(hdr::VERSION);
        if version != RING_VERSION {
            return Err(RingError::BadVersion(version));
        }
        let capacity = head.load_u32(hdr::CAPACITY);
        let slot_size = head.load_u32(hdr::SLOT_SIZE);
        drop(head);
        check_geometry(capacity, slot_size)?;

        let len = Self::file_size(capacity, slot_size);
        let map = map_file(path, len, false)?;
        Ok(Self {
            map,
            capacity: capacity as u64,
            slot_size: slot_size as usize,
        })
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// The largest message a slot can hold.
    pub fn max_message_len(&self) -> usize {
        self.slot_size - slot::LEN
    }

    /// Takes the producer role. One process, one thread, one role.
    pub fn into_producer(self) -> Producer {
        Producer {
            cached_read: self.read_index().load(Ordering::Acquire),
            ring: self,
        }
    }

    /// Takes the consumer role.
    pub fn into_consumer(self) -> Consumer {
        Consumer {
            cached_write: self.write_index().load(Ordering::Acquire),
            ring: self,
        }
    }

    // --- header access ----------------------------------------------------

    fn atomic_at(&self, offset: usize) -> &AtomicU64 {
        debug_assert!(offset + 8 <= hdr::LEN);
        debug_assert!(
            offset.is_multiple_of(8),
            "an atomic must be naturally aligned"
        );
        // SAFETY: `mmap` returns page-aligned memory, `offset` is a multiple of
        // 8 inside the header, and the header is at least `offset + 8` bytes.
        // The reference borrows `self`, so it cannot outlive the mapping.
        unsafe { &*(self.map.ptr.add(offset).cast::<AtomicU64>()) }
    }

    fn write_index(&self) -> &AtomicU64 {
        self.atomic_at(hdr::WRITE_INDEX)
    }

    fn read_index(&self) -> &AtomicU64 {
        self.atomic_at(hdr::READ_INDEX)
    }

    fn store_u64(&self, offset: usize, v: u64) {
        // SAFETY: `offset + 8` is inside the header.
        unsafe {
            std::ptr::copy_nonoverlapping(v.to_le_bytes().as_ptr(), self.map.ptr.add(offset), 8);
        }
    }

    fn store_u32(&self, offset: usize, v: u32) {
        // SAFETY: `offset + 4` is inside the header.
        unsafe {
            std::ptr::copy_nonoverlapping(v.to_le_bytes().as_ptr(), self.map.ptr.add(offset), 4);
        }
    }

    fn load_u64(&self, offset: usize) -> u64 {
        let mut b = [0u8; 8];
        // SAFETY: `offset + 8` is inside the header.
        unsafe {
            std::ptr::copy_nonoverlapping(self.map.ptr.add(offset), b.as_mut_ptr(), 8);
        }
        u64::from_le_bytes(b)
    }

    fn load_u32(&self, offset: usize) -> u32 {
        let mut b = [0u8; 4];
        // SAFETY: `offset + 4` is inside the header.
        unsafe {
            std::ptr::copy_nonoverlapping(self.map.ptr.add(offset), b.as_mut_ptr(), 4);
        }
        u32::from_le_bytes(b)
    }

    /// The first byte of the slot `index` maps to.
    fn slot_ptr(&self, index: u64) -> *mut u8 {
        let n = (index & (self.capacity - 1)) as usize;
        // SAFETY: `n < capacity`, so the offset is inside the mapping.
        unsafe { self.map.ptr.add(hdr::LEN + n * self.slot_size) }
    }
}

// ---------------------------------------------------------------------------
// producer
// ---------------------------------------------------------------------------

/// The writing end. Owns `writeIndex` and reads `readIndex`.
#[derive(Debug)]
pub struct Producer {
    ring: Ring,
    /// The last `readIndex` we saw. Re-read only when this says the ring is
    /// full, so the common case never touches the consumer's cache line.
    cached_read: u64,
}

impl Producer {
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    /// Slots currently occupied.
    pub fn depth(&self) -> u64 {
        self.ring.write_index().load(Ordering::Relaxed)
            - self.ring.read_index().load(Ordering::Acquire)
    }

    /// Writes a message straight into the next slot.
    ///
    /// `fill` receives the slot's usable bytes and returns how many it wrote,
    /// or `None` to abandon the push without consuming a slot. Nothing is
    /// published until `fill` returns, so a message that turns out not to fit
    /// leaves the ring exactly as it was.
    ///
    /// This is the path the order flow uses: the encoder writes into the shared
    /// page once, rather than into a buffer that is then copied in.
    pub fn push_with<F>(&mut self, fill: F) -> Result<usize, PushError>
    where
        F: FnOnce(&mut [u8]) -> Option<usize>,
    {
        let w = self.ring.write_index().load(Ordering::Relaxed);
        if w.wrapping_sub(self.cached_read) >= self.ring.capacity {
            // Only now is the consumer's cache line worth touching.
            self.cached_read = self.ring.read_index().load(Ordering::Acquire);
            if w.wrapping_sub(self.cached_read) >= self.ring.capacity {
                return Err(PushError::Full);
            }
        }

        let base = self.ring.slot_ptr(w);
        let usable = self.ring.max_message_len();
        // SAFETY: the slot is `slot_size` bytes inside the mapping and the
        // consumer will not touch it until the release store below, so this
        // process is the only one writing here.
        let body = unsafe { std::slice::from_raw_parts_mut(base.add(slot::LEN), usable) };

        let Some(n) = fill(body) else {
            return Err(PushError::Abandoned);
        };
        if n > usable {
            // `fill` lied about how much it wrote. It has already scribbled on
            // the slot, but the slot is not published, so the damage is
            // confined to bytes nobody will read.
            return Err(PushError::TooLarge {
                len: n,
                capacity: usable,
            });
        }

        // The length prefix is part of the slot and must be visible before the
        // slot is, which the release store below guarantees.
        // SAFETY: `slot::LENGTH + 4` is inside the slot.
        unsafe {
            std::ptr::copy_nonoverlapping(
                (n as u32).to_le_bytes().as_ptr(),
                base.add(slot::LENGTH),
                4,
            );
        }
        // Publication. Everything written above happens-before the acquire load
        // in `Consumer::pop_with` that observes this value.
        self.ring.write_index().store(w + 1, Ordering::Release);
        Ok(n)
    }

    /// Copies `bytes` into the next slot. Convenience over [`push_with`];
    /// the order path uses `push_with` so the encoder writes the page directly.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        let usable = self.ring.max_message_len();
        if bytes.len() > usable {
            return Err(PushError::TooLarge {
                len: bytes.len(),
                capacity: usable,
            });
        }
        self.push_with(|slot| {
            slot[..bytes.len()].copy_from_slice(bytes);
            Some(bytes.len())
        })
        .map(|_| ())
    }
}

// ---------------------------------------------------------------------------
// consumer
// ---------------------------------------------------------------------------

/// The reading end. Owns `readIndex` and reads `writeIndex`.
#[derive(Debug)]
pub struct Consumer {
    ring: Ring,
    /// The last `writeIndex` we saw. Re-read only when this says the ring is
    /// empty, so a burst of queued messages is drained without re-reading the
    /// producer's cache line per message.
    cached_write: u64,
}

impl Consumer {
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    pub fn depth(&self) -> u64 {
        self.ring.write_index().load(Ordering::Acquire)
            - self.ring.read_index().load(Ordering::Relaxed)
    }

    /// Hands the next message to `visit`, or returns `None` if the ring is empty.
    ///
    /// The slice borrows the shared page rather than a copy. That is sound
    /// because the slot is not reusable until `readIndex` advances, which
    /// happens after `visit` returns.
    pub fn pop_with<F, R>(&mut self, visit: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let r = self.ring.read_index().load(Ordering::Relaxed);
        // `>=` and not `==`. The only thing the cache is allowed to be is
        // stale-low, and a stale-low `==` would sail past this check and read a
        // slot the producer has not written. With `>=`, any cache value that is
        // not strictly ahead of `r` forces a re-read, so the hint can never
        // cause a wrong answer — only an occasional extra load.
        if r >= self.cached_write {
            self.cached_write = self.ring.write_index().load(Ordering::Acquire);
            if r >= self.cached_write {
                return None;
            }
        }

        let base = self.ring.slot_ptr(r);
        // SAFETY: the producer's release store on `writeIndex` happens-before
        // the acquire load above, so every byte it wrote into this slot is
        // visible here.
        let mut len_bytes = [0u8; 4];
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(slot::LENGTH), len_bytes.as_mut_ptr(), 4);
        }
        // The clamp is a bounds guarantee for the `unsafe` below, not error
        // handling: a correct producer cannot write a length past the slot,
        // because `push_with` refuses one. It is here so that a corrupted or
        // hostile page cannot turn into an out-of-bounds read.
        let n = (u32::from_le_bytes(len_bytes) as usize).min(self.ring.max_message_len());
        // SAFETY: `n` is clamped to the slot's usable length, so the slice is
        // inside the mapping. It borrows `base`, which stays valid until the
        // release store below.
        let body = unsafe { std::slice::from_raw_parts(base.add(slot::LEN), n) };

        let out = visit(body);
        // Release, so a producer that observes this index also observes that we
        // are done reading the slot it is about to overwrite.
        self.ring.read_index().store(r + 1, Ordering::Release);
        Some(out)
    }

    /// Drains up to `limit` messages, calling `visit` on each.
    ///
    /// Bounded rather than "until empty" on purpose: a consumer that drains an
    /// unbounded queue can be held in this loop indefinitely by a fast producer
    /// and never get back to its timers.
    pub fn drain<F>(&mut self, limit: usize, mut visit: F) -> usize
    where
        F: FnMut(&[u8]),
    {
        let mut n = 0;
        while n < limit && self.pop_with(&mut visit).is_some() {
            n += 1;
        }
        n
    }
}

#[cfg(test)]
mod tests;
