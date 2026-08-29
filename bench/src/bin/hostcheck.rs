//! Says whether this machine may publish a performance number, and why not.
//!
//! ```text
//! cargo run --release -p bench --bin hostcheck
//! cargo run --release -p bench --bin hostcheck -- --fields   # for a report header
//! ```
//!
//! Exit code 0 when the host is publishable, 1 when it is not. `scripts/bench.sh`
//! runs this first and will not write a report that looks like a result when it
//! comes back non-zero.
//!
//! Run it on the rented host **before** provisioning anything expensive. It
//! takes a fraction of a second and it is the difference between a burn day that
//! produces a number and one that produces a lesson about `constant_tsc`.

use std::process::ExitCode;

use bench_support::{report_header, HostFacts, Tsc};

fn main() -> ExitCode {
    let fields = std::env::args().any(|a| a == "--fields");

    // Calibrating takes a moment and is worth doing even when the verdict is
    // already refused: the calibration spread is one of the more informative
    // things about a host, and on a noisy one it is the first symptom.
    let tsc = Tsc::calibrate();

    if fields {
        print!("{}", report_header(&tsc));
        return exit_code(&HostFacts::gather());
    }

    let facts = HostFacts::gather();
    println!("host");
    println!("  cpu           {}", facts.cpu_model);
    println!(
        "  cores         {} physical / {} logical",
        facts
            .physical_cores
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unknown".into()),
        facts.logical_cores
    );
    println!("  kernel        {}", facts.kernel);
    println!(
        "  virtualised   {}",
        facts.virtualised.unwrap_or("no (as far as this can tell)")
    );
    println!(
        "  governor      {}",
        facts.governor.as_deref().unwrap_or("unknown")
    );
    println!(
        "  turbo         {}",
        match facts.turbo_enabled {
            Some(true) => "on",
            Some(false) => "off",
            None => "unknown",
        }
    );
    println!(
        "  build         {}",
        if facts.optimised_build {
            "optimised"
        } else {
            "DEBUG — not a benchmarkable program"
        }
    );
    println!();
    println!("clock");
    println!("  {tsc}");
    println!(
        "  constant_tsc  {}   nonstop_tsc  {}",
        facts.tsc.constant_tsc, facts.tsc.nonstop_tsc
    );
    println!();
    print!("{}", facts.verdict());

    exit_code(&facts)
}

fn exit_code(facts: &HostFacts) -> ExitCode {
    if facts.verdict().is_publishable() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
