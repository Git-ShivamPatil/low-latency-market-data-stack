//! Writing book checkpoints for the smoke test to reconcile.
//!
//! One line per checkpoint: `<sequence> <top> <full> <orders>`. Both the engine
//! and the handler write this format at the same interval, so `scripts/smoke.sh`
//! can join the two files on sequence and require every shared row to match.
//!
//! Every line is flushed as it is written. A run that gets Ctrl-C'd mid-demo
//! should still leave a usable file behind, and buffering would throw away the
//! last few seconds of exactly the evidence this exists to produce.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::digest::BookDigest;

#[derive(Debug, Default)]
pub struct DigestLog {
    sink: Option<BufWriter<File>>,
}

impl DigestLog {
    /// `None` disables checkpointing entirely.
    pub fn open(path: Option<&Path>) -> io::Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(Self {
            sink: Some(BufWriter::new(File::create(path)?)),
        })
    }

    pub fn write(&mut self, sequence: u64, digest: BookDigest) -> io::Result<()> {
        let Some(sink) = self.sink.as_mut() else {
            return Ok(());
        };
        writeln!(sink, "{sequence} {}", digest.to_fields())?;
        sink.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(sink) = self.sink.as_mut() {
            sink.flush()?;
        }
        Ok(())
    }
}
