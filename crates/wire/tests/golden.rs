//! The Rust half of milestone 1's verification.
//!
//! Runs against the same `schema/golden/*.bin` files as the C++ `wire_golden`
//! test, asserts the same field values (both assertion bodies are generated from
//! one definition in `schema/goldens.py`), and re-encodes each vector expecting
//! the bytes back.
//!
//! The re-encode half is what makes every byte load-bearing. A decode-only test
//! ignores padding and reserved bytes, so a corruption there would slip through
//! — which is exactly what `scripts/verify-golden-corruption.sh` checks for.

mod golden_generated;

use std::path::PathBuf;

use golden_generated::VECTORS;

/// `MDSTACK_GOLDEN_DIR` overrides the location so the corruption script can
/// point this suite at a deliberately damaged copy of the vectors.
fn golden_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("MDSTACK_GOLDEN_DIR") {
        return PathBuf::from(dir);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("schema")
        .join("golden")
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Reports the first differing byte rather than dumping both buffers, so a
/// failure names an offset that can be looked up in the matching `.txt` dump.
fn first_difference(expected: &[u8], actual: &[u8]) -> Option<String> {
    if expected.len() != actual.len() {
        return Some(format!(
            "length: expected {} bytes, got {}",
            expected.len(),
            actual.len()
        ));
    }
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        if e != a {
            return Some(format!("byte {i}: expected 0x{e:02x}, got 0x{a:02x}"));
        }
    }
    None
}

#[test]
fn golden_vectors_decode_to_the_expected_fields() {
    let dir = golden_dir();
    let mut checked = 0;
    for v in VECTORS {
        let path = dir.join(v.file);
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("{}: cannot read {}: {e}", v.name, path.display()));
        if let Err(e) = (v.check)(&bytes) {
            panic!("{}: {e}\n  why this vector exists: {}", v.name, v.why);
        }
        checked += 1;
    }
    assert!(checked > 0, "no vectors found in {}", dir.display());
}

#[test]
fn golden_vectors_re_encode_byte_for_byte() {
    let dir = golden_dir();
    let mut buf = vec![0u8; 4096];
    for v in VECTORS {
        let path = dir.join(v.file);
        let expected = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("{}: cannot read {}: {e}", v.name, path.display()));
        // Poison the buffer so a field the encoder forgets to write shows up as
        // a mismatch rather than as a lucky zero.
        buf.fill(0xAA);
        let n = (v.build)(&mut buf).unwrap_or_else(|e| panic!("{}: encode failed: {e}", v.name));
        if let Some(d) = first_difference(&expected, &buf[..n]) {
            panic!(
                "{}: re-encode differs: {d}\n  expected: {}\n  actual:   {}",
                v.name,
                hex(&expected),
                hex(&buf[..n])
            );
        }
    }
}

/// The milestone's own verification step, as a test rather than a shell script:
/// a one-byte edit anywhere in any vector must be caught.
///
/// Every byte is covered because the re-encode comparison is byte-exact —
/// including padding and reserved bytes, which no field accessor ever reads.
#[test]
fn every_single_byte_flip_is_detected() {
    let dir = golden_dir();
    let mut buf = vec![0u8; 4096];
    let mut flips = 0;
    for v in VECTORS {
        let original = std::fs::read(dir.join(v.file)).expect("read vector");
        for i in 0..original.len() {
            let mut damaged = original.clone();
            damaged[i] ^= 0x01;

            let decode_ok = (v.check)(&damaged).is_ok();
            let reencode_ok = {
                buf.fill(0xAA);
                match (v.build)(&mut buf) {
                    Ok(n) => first_difference(&damaged, &buf[..n]).is_none(),
                    Err(_) => false,
                }
            };

            assert!(
                !(decode_ok && reencode_ok),
                "{}: flipping bit 0 of byte {i} was not detected — that byte is \
                 not covered by any assertion, so a corruption there would ship",
                v.name
            );
            flips += 1;
        }
    }
    assert!(flips > 0, "no vectors found in {}", dir.display());
    println!("{flips} single-byte corruptions, all detected");
}
