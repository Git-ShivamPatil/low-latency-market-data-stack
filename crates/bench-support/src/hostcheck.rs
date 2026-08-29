//! Deciding whether this machine is allowed to produce a publishable number.
//!
//! # Why this is code and not a note in a README
//!
//! The portfolio page for this project already advertises `1M+ msg/s`. The
//! development host is a 2-core i3 under WSL2. The gap between those two facts
//! is the single largest integrity risk in the whole repository, and the way it
//! goes wrong is not malice — it is a benchmark that runs, prints a number, and
//! gets pasted somewhere three weeks later by someone who no longer remembers
//! which machine it came from.
//!
//! So the refusal is mechanical. [`HostFacts::gather`] reads what the host
//! actually is, [`Verdict`] says whether a number from it may be published, and
//! `scripts/bench.sh` will not write a report when the verdict says no. A human
//! can override it — but has to do so explicitly, and the override is stamped
//! into the report.
//!
//! # What is checked, and why each one
//!
//! **Physical cores.** The publisher, the engine and the handler each need one,
//! and the measurement needs the machine not to be scheduling them against each
//! other. This project's own prerequisites say four minimum, six to eight
//! preferred. On two, the throughput figure is unreachable and the p99 is a
//! measurement of the scheduler.
//!
//! **An invariant TSC.** Without `constant_tsc` and `nonstop_tsc` a cycle count
//! cannot be converted to a duration at all. See [`crate::tsc`].
//!
//! **The CPU governor.** `powersave` or `ondemand` means the core changes
//! frequency during the run. The median survives that; the p99.9 does not, and
//! the p99.9 is the number that says something.
//!
//! **Turbo.** Not disqualifying, but a run with turbo enabled is not
//! reproducible across thermal states, and "reproduced three times within 10%"
//! is part of what this project promises.
//!
//! **Virtualisation.** WSL2 is a VM with a masked TSC and a scheduler this
//! process cannot see. Numbers from it are not merely noisy.
//!
//! **Optimisation.** A debug build is not a slow version of the release build;
//! it is a different program. Benchmarking one is not a small error.

use std::fmt;

use crate::tsc::{Tsc, TscQuality};

/// The minimum physical core count this project's own prerequisites name.
pub const MINIMUM_PHYSICAL_CORES: usize = 4;
/// What they call strongly preferred.
pub const PREFERRED_PHYSICAL_CORES: usize = 6;

#[derive(Debug, Clone)]
pub struct HostFacts {
    pub cpu_model: String,
    pub physical_cores: Option<usize>,
    pub logical_cores: usize,
    pub kernel: String,
    pub virtualised: Option<&'static str>,
    pub governor: Option<String>,
    pub turbo_enabled: Option<bool>,
    pub tsc: TscQuality,
    pub optimised_build: bool,
    pub target_cpu_native: bool,
}

impl HostFacts {
    pub fn gather() -> Self {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        Self {
            // `model name` is an x86 field. aarch64 has `CPU implementer` and
            // `CPU part` instead, and the arm64 runner reported "unknown"
            // until this looked for them.
            cpu_model: field(&cpuinfo, "model name")
                .or_else(|| {
                    let imp = field(&cpuinfo, "CPU implementer")?;
                    let part = field(&cpuinfo, "CPU part")?;
                    Some(format!("aarch64 implementer {imp} part {part}"))
                })
                .or_else(|| read_trimmed("/sys/devices/virtual/dmi/id/product_name"))
                .unwrap_or_else(|| "unknown".into()),
            physical_cores: physical_cores_from_sysfs().or_else(|| physical_cores(&cpuinfo)),
            logical_cores: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
            kernel: std::fs::read_to_string("/proc/version")
                .unwrap_or_default()
                .lines()
                .next()
                .unwrap_or("unknown")
                .to_string(),
            virtualised: detect_virtualisation(),
            governor: read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
            turbo_enabled: detect_turbo(),
            tsc: TscQuality::detect(),
            // `debug_assertions` is off in release and on in debug, which is the
            // only build distinction visible from inside the binary.
            optimised_build: !cfg!(debug_assertions),
            // Set by `.cargo/config.toml`. Recorded because every number this
            // project produces comes from a binary compiled for its host, and a
            // report that omits that is misleading.
            target_cpu_native: cfg!(target_feature = "sse4.2"),
        }
    }

    /// Whether a performance number measured here may be published.
    pub fn verdict(&self) -> Verdict {
        let mut blockers = Vec::new();
        let mut warnings = Vec::new();

        match self.physical_cores {
            Some(n) if n < MINIMUM_PHYSICAL_CORES => blockers.push(format!(
                "{n} physical cores. This project needs at least {MINIMUM_PHYSICAL_CORES} \
                 ({PREFERRED_PHYSICAL_CORES} preferred): the publisher, the engine and the \
                 handler each need one, and on fewer the throughput figure is unreachable \
                 and the tail latencies measure the scheduler rather than the code."
            )),
            Some(n) if n < PREFERRED_PHYSICAL_CORES => warnings.push(format!(
                "{n} physical cores, below the {PREFERRED_PHYSICAL_CORES} this project \
                 prefers. Usable, but the report has to say so."
            )),
            Some(_) => {}
            // Unknown is not the same as fine.
            //
            // This was a caveat, and the free arm64 runner walked straight
            // through it: `/proc/cpuinfo` on aarch64 carries no `core id`, so
            // the count came back `None`, and the gate printed PUBLISHABLE while
            // admitting in the next line that the most important precondition
            // was unverified. That is exactly the failure this whole crate
            // exists to prevent, so it blocks.
            None => blockers.push(
                "the physical core count could not be determined, so the most important \
                 precondition is unverified. A number from a host that has not been shown \
                 to meet the requirement is not publishable, whatever the requirement \
                 turns out to be."
                    .to_string(),
            ),
        }

        // Virtualisation is a caveat, not a blocker.
        //
        // It used to be a blocker, which was wrong in a way that only showed up
        // once the budget for this programme went to zero: every free host is a
        // VM, so blocking on virtualisation blocks on everything and the gate
        // becomes a gate against measuring at all.
        //
        // What actually matters about a VM is whether it masks the cycle counter
        // and whether it gives you real cores — and both of those are checked on
        // their own terms above. WSL2 fails both and is still refused. A
        // dedicated ephemeral runner that exposes an invariant counter and four
        // unshared cores is a legitimate host, and the requirement that three
        // runs agree within 10% is what decides whether it was quiet enough.
        if let Some(kind) = self.virtualised {
            warnings.push(format!(
                "running under {kind}. The scheduler belongs to a host this process cannot \
                 see, so the tail latencies are partly a measurement of that host. The \
                 report has to say so, and the three-runs-within-10% requirement is what \
                 decides whether it was quiet enough to matter."
            ));
        }

        if let Some(why) = self.tsc.why_not() {
            blockers.push(format!("the cycle counter cannot be trusted: {why}"));
        }

        if !self.optimised_build {
            blockers.push(
                "this is a debug build. A debug build is not a slow release build, it is a \
                 different program, and benchmarking it produces a number about nothing."
                    .to_string(),
            );
        }

        match self.governor.as_deref() {
            Some("performance") => {}
            Some(g) => warnings.push(format!(
                "the CPU governor is `{g}`, so the core changes frequency during the run. \
                 The median tolerates that; the p99.9 does not, and the p99.9 is the number \
                 that says something."
            )),
            None => warnings.push(
                "the CPU governor could not be read, so frequency scaling during the run is \
                 unknown."
                    .to_string(),
            ),
        }

        if self.turbo_enabled == Some(true) {
            warnings.push(
                "turbo is enabled. Not disqualifying, but the clock depends on thermal state, \
                 and this project promises three runs within 10% of each other."
                    .to_string(),
            );
        }

        if blockers.is_empty() {
            Verdict::Publishable { warnings }
        } else {
            Verdict::Refused { blockers, warnings }
        }
    }

    /// `key=value` lines for the report header. Every field a reader needs to
    /// judge the number, whether or not it flatters the result.
    pub fn to_fields(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("host_cpu={}\n", self.cpu_model));
        s.push_str(&format!(
            "host_physical_cores={}\n",
            self.physical_cores
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into())
        ));
        s.push_str(&format!("host_logical_cores={}\n", self.logical_cores));
        s.push_str(&format!("host_kernel={}\n", self.kernel));
        s.push_str(&format!(
            "host_virtualised={}\n",
            self.virtualised.unwrap_or("no")
        ));
        s.push_str(&format!(
            "host_governor={}\n",
            self.governor.as_deref().unwrap_or("unknown")
        ));
        s.push_str(&format!(
            "host_turbo={}\n",
            match self.turbo_enabled {
                Some(true) => "on",
                Some(false) => "off",
                None => "unknown",
            }
        ));
        s.push_str(&format!("host_constant_tsc={}\n", self.tsc.constant_tsc));
        s.push_str(&format!("host_nonstop_tsc={}\n", self.tsc.nonstop_tsc));
        s.push_str(&format!("host_optimised_build={}\n", self.optimised_build));
        s.push_str(&format!(
            "host_publishable={}",
            self.verdict().is_publishable()
        ));
        s
    }
}

/// Whether a number from this host may be published.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// Every precondition holds. `warnings` still belong in the report.
    Publishable { warnings: Vec<String> },
    /// At least one precondition fails. A number measured here describes the
    /// host, not the code.
    Refused {
        blockers: Vec<String>,
        warnings: Vec<String>,
    },
}

impl Verdict {
    pub fn is_publishable(&self) -> bool {
        matches!(self, Verdict::Publishable { .. })
    }

    pub fn blockers(&self) -> &[String] {
        match self {
            Verdict::Publishable { .. } => &[],
            Verdict::Refused { blockers, .. } => blockers,
        }
    }

    pub fn warnings(&self) -> &[String] {
        match self {
            Verdict::Publishable { warnings } => warnings,
            Verdict::Refused { warnings, .. } => warnings,
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Publishable { warnings } => {
                writeln!(f, "PUBLISHABLE: this host meets the preconditions.")?;
                for w in warnings {
                    writeln!(f, "  caveat: {w}")?;
                }
            }
            Verdict::Refused { blockers, warnings } => {
                writeln!(
                    f,
                    "REFUSED: a performance number measured here would describe this host, \
                     not this code."
                )?;
                for b in blockers {
                    writeln!(f, "  blocker: {b}")?;
                }
                for w in warnings {
                    writeln!(f, "  caveat: {w}")?;
                }
                writeln!(
                    f,
                    "\nThe harness still runs — the code paths are worth exercising anywhere. \
                     What it will not do is write a report that looks publishable."
                )?;
            }
        }
        Ok(())
    }
}

/// The full header a report opens with: the host, the clock, and the verdict.
pub fn report_header(tsc: &Tsc) -> String {
    let facts = HostFacts::gather();
    let verdict = facts.verdict();
    let mut s = String::new();
    s.push_str(&facts.to_fields());
    s.push('\n');
    s.push_str(&format!("tsc_megahertz={:.1}\n", tsc.megahertz()));
    s.push_str(&format!("tsc_overhead_ticks={}\n", tsc.overhead_ticks()));
    s.push_str(&format!(
        "tsc_calibration_spread={:.5}\n",
        tsc.calibration_spread()
    ));
    s.push_str(&format!("tsc_trustworthy={}\n", tsc.is_trustworthy()));
    for (i, b) in verdict.blockers().iter().enumerate() {
        s.push_str(&format!("blocker_{i}={b}\n"));
    }
    for (i, w) in verdict.warnings().iter().enumerate() {
        s.push_str(&format!("caveat_{i}={w}\n"));
    }
    s
}

// --- reading the host ------------------------------------------------------

fn field(cpuinfo: &str, key: &str) -> Option<String> {
    cpuinfo
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Distinct physical cores, from sysfs.
///
/// `/sys/devices/system/cpu/cpuN/topology/` is the portable source: it exists on
/// aarch64, where `/proc/cpuinfo` has no `core id` at all. Preferred over the
/// `/proc/cpuinfo` scan below, which only ever worked on x86.
fn physical_cores_from_sysfs() -> Option<usize> {
    let mut pairs = std::collections::BTreeSet::new();
    let entries = std::fs::read_dir("/sys/devices/system/cpu").ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("cpu") || !name[3..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if name.len() == 3 {
            continue;
        }
        let base = entry.path().join("topology");
        let core = read_trimmed(base.join("core_id").to_str()?);
        // Single-socket machines sometimes omit the package id; treating a
        // missing one as socket 0 is right there and harmless elsewhere,
        // because two sockets that both omit it would be indistinguishable
        // anyway and that is a machine nobody is benchmarking on.
        let package = read_trimmed(base.join("physical_package_id").to_str()?)
            .unwrap_or_else(|| "0".to_string());
        if let Some(core) = core {
            pairs.insert((package, core));
        }
    }
    (!pairs.is_empty()).then_some(pairs.len())
}

/// Distinct `(physical id, core id)` pairs — logical CPUs that share a core
/// through SMT collapse to one.
fn physical_cores(cpuinfo: &str) -> Option<usize> {
    let mut pairs = std::collections::BTreeSet::new();
    let mut physical_id: Option<String> = None;
    let mut core_id: Option<String> = None;
    for line in cpuinfo.lines() {
        if let Some((k, v)) = line.split_once(':') {
            match k.trim() {
                "physical id" => physical_id = Some(v.trim().to_string()),
                "core id" => core_id = Some(v.trim().to_string()),
                _ => {}
            }
        }
        if line.trim().is_empty() {
            if let (Some(p), Some(c)) = (physical_id.take(), core_id.take()) {
                pairs.insert((p, c));
            }
        }
    }
    if let (Some(p), Some(c)) = (physical_id, core_id) {
        pairs.insert((p, c));
    }
    // WSL2 omits the topology fields entirely, which is itself informative:
    // "unknown" is the honest answer and the verdict treats it as unverified
    // rather than as a pass.
    if pairs.is_empty() {
        None
    } else {
        Some(pairs.len())
    }
}

fn detect_virtualisation() -> Option<&'static str> {
    if let Ok(v) = std::fs::read_to_string("/proc/version") {
        let v = v.to_ascii_lowercase();
        if v.contains("microsoft") || v.contains("wsl") {
            return Some("WSL2");
        }
    }
    if let Ok(t) = std::fs::read_to_string("/sys/hypervisor/type") {
        let t = t.trim().to_string();
        if t == "xen" {
            return Some("Xen");
        }
    }
    // A bare-metal Linux host has no `hypervisor` flag; every common x86 VM sets
    // it. Not conclusive on its own, which is why it is not first.
    if let Ok(c) = std::fs::read_to_string("/proc/cpuinfo") {
        if c.lines()
            .find(|l| l.starts_with("flags"))
            .is_some_and(|l| l.split_whitespace().any(|f| f == "hypervisor"))
        {
            return Some("a hypervisor");
        }
    }
    // The `hypervisor` flag is x86 CPUID and does not exist on aarch64, so the
    // check above reported the arm64 CI runner — an Azure VM — as bare metal.
    // DMI knows better and is architecture-independent.
    if let Some(vendor) = read_trimmed("/sys/devices/virtual/dmi/id/sys_vendor") {
        let v = vendor.to_ascii_lowercase();
        for (needle, name) in [
            ("microsoft", "a Microsoft hypervisor"),
            ("amazon", "an Amazon hypervisor"),
            ("google", "a Google hypervisor"),
            ("qemu", "QEMU/KVM"),
            ("vmware", "VMware"),
            ("xen", "Xen"),
            ("oracle", "an Oracle hypervisor"),
        ] {
            if v.contains(needle) {
                return Some(name);
            }
        }
    }
    None
}

fn detect_turbo() -> Option<bool> {
    if let Some(v) = read_trimmed("/sys/devices/system/cpu/intel_pstate/no_turbo") {
        return Some(v == "0");
    }
    if let Some(v) = read_trimmed("/sys/devices/system/cpu/cpufreq/boost") {
        return Some(v == "1");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(cores: Option<usize>, virt: Option<&'static str>, optimised: bool) -> HostFacts {
        HostFacts {
            cpu_model: "test".into(),
            physical_cores: cores,
            logical_cores: cores.unwrap_or(1) * 2,
            kernel: "test".into(),
            virtualised: virt,
            governor: Some("performance".into()),
            turbo_enabled: Some(false),
            tsc: TscQuality {
                constant_tsc: true,
                nonstop_tsc: true,
                flags_readable: true,
                counter_hz: None,
            },
            optimised_build: optimised,
            target_cpu_native: true,
        }
    }

    #[test]
    fn a_host_that_meets_every_precondition_is_publishable() {
        let v = facts(Some(8), None, true).verdict();
        assert!(v.is_publishable(), "{v}");
        assert!(v.blockers().is_empty());
        assert!(v.warnings().is_empty());
    }

    #[test]
    fn too_few_cores_is_a_blocker_and_names_the_number() {
        let v = facts(Some(2), None, true).verdict();
        assert!(!v.is_publishable());
        assert!(
            v.blockers().iter().any(|b| b.contains("2 physical cores")),
            "the refusal has to say what it found: {v}"
        );
    }

    #[test]
    fn a_workable_but_thin_core_count_is_a_caveat_rather_than_a_refusal() {
        // Four cores meets the stated minimum. That is a real distinction: the
        // gate exists to stop numbers from a 2-core laptop, not to make the
        // project unbuildable on anything short of ideal hardware.
        let v = facts(Some(4), None, true).verdict();
        assert!(v.is_publishable(), "{v}");
        assert_eq!(v.warnings().len(), 1);
        assert!(v.warnings()[0].contains("prefers"));
    }

    #[test]
    fn virtualisation_is_a_caveat_not_a_refusal() {
        // Every free host is a VM. Blocking on virtualisation blocks on
        // everything, which turns the gate into a gate against measuring at all.
        // What matters about a VM is checked on its own terms: a masked counter
        // and too few cores are both still blockers, and WSL2 fails both.
        let v = facts(Some(16), Some("a hypervisor"), true).verdict();
        assert!(v.is_publishable(), "{v}");
        assert!(v.warnings().iter().any(|w| w.contains("hypervisor")));
    }

    #[test]
    fn wsl2_is_still_refused_on_its_own_merits() {
        // The case the gate exists for. It must keep failing after
        // virtualisation stopped being a blocker, or demoting that check
        // quietly opened the door this whole crate is here to hold shut.
        let mut f = facts(Some(2), Some("WSL2"), true);
        f.tsc = TscQuality {
            constant_tsc: false,
            nonstop_tsc: false,
            flags_readable: true,
            counter_hz: None,
        };
        let v = f.verdict();
        assert!(!v.is_publishable(), "{v}");
        assert_eq!(v.blockers().len(), 2, "cores and the counter, both: {v}");
    }

    #[test]
    fn a_debug_build_blocks() {
        let v = facts(Some(16), None, false).verdict();
        assert!(!v.is_publishable());
        assert!(v.blockers().iter().any(|b| b.contains("debug build")));
    }

    #[test]
    fn a_masked_tsc_blocks() {
        let mut f = facts(Some(16), None, true);
        f.tsc = TscQuality {
            constant_tsc: false,
            nonstop_tsc: false,
            flags_readable: true,
            counter_hz: None,
        };
        let v = f.verdict();
        assert!(!v.is_publishable());
        // The property, not the wording. A counter that cannot be trusted must
        // block; *why* it cannot be trusted is architecture-specific, and
        // asserting the x86 phrasing failed this test on the arm64 runner where
        // the honest explanation names `cntfrq_el0` instead.
        assert!(
            v.blockers().iter().any(|b| b.contains("cycle counter")),
            "a masked counter must block, whatever the architecture calls it: {v}"
        );
    }

    #[test]
    fn a_scaling_governor_is_a_caveat_not_a_refusal() {
        let mut f = facts(Some(8), None, true);
        f.governor = Some("powersave".into());
        let v = f.verdict();
        assert!(v.is_publishable(), "a governor alone should not block: {v}");
        assert!(v.warnings().iter().any(|w| w.contains("powersave")));
    }

    #[test]
    fn every_blocker_is_reported_not_just_the_first() {
        // A host that fails three ways should say so once, rather than being
        // fixed one round-trip at a time.
        let mut f = facts(Some(2), Some("WSL2"), false);
        f.tsc = TscQuality {
            constant_tsc: false,
            nonstop_tsc: false,
            flags_readable: true,
            counter_hz: None,
        };
        let v = f.verdict();
        assert_eq!(
            v.blockers().len(),
            3,
            "cores, counter and build profile. Virtualisation is a caveat: {v}"
        );
    }

    #[test]
    fn the_fields_carry_the_verdict_so_a_report_cannot_omit_it() {
        let text = facts(Some(2), None, true).to_fields();
        assert!(text.contains("host_physical_cores=2"));
        assert!(
            text.contains("host_publishable=false"),
            "the report header has to carry the verdict: {text}"
        );
    }

    #[test]
    fn physical_cores_collapses_smt_siblings() {
        // Two logical CPUs sharing one core must count once, or a 2-core laptop
        // with hyperthreading passes a 4-core gate.
        let cpuinfo = "\
processor\t: 0
physical id\t: 0
core id\t\t: 0

processor\t: 1
physical id\t: 0
core id\t\t: 0

processor\t: 2
physical id\t: 0
core id\t\t: 1

processor\t: 3
physical id\t: 0
core id\t\t: 1
";
        assert_eq!(physical_cores(cpuinfo), Some(2));
    }

    #[test]
    fn an_unknown_core_count_blocks_rather_than_passing_with_a_note() {
        // The hole the free arm64 runner found. `/proc/cpuinfo` on aarch64 has
        // no `core id`, the count came back None, and the gate printed
        // PUBLISHABLE while saying in the next line that the most important
        // precondition was unverified.
        let cpuinfo = "processor\t: 0\nmodel name\t: something\n";
        assert_eq!(physical_cores(cpuinfo), None);
        let v = facts(None, None, true).verdict();
        assert!(!v.is_publishable(), "unknown is not the same as fine: {v}");
        assert!(v.blockers().iter().any(|b| b.contains("unverified")));
    }

    #[test]
    fn this_host_is_described_without_panicking() {
        // Whatever it says, it has to say it. The gate is worthless if gathering
        // the facts is what falls over.
        let f = HostFacts::gather();
        let text = f.to_fields();
        assert!(text.contains("host_cpu="));
        assert!(text.contains("host_publishable="));
        println!("{}", f.verdict());
    }
}
