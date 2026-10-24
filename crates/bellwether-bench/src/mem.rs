//! Process memory probes for the ADR 018 streaming guard.
//!
//! The guard has to be able to see retention, so it measures the process,
//! not the generator's own bookkeeping:
//!
//! - **Linux:** the probe writes `5` to `/proc/self/clear_refs`, which resets
//!   the kernel's peak-RSS mark (`VmHWM`) to the current RSS, records `VmRSS`
//!   as the baseline, and reads `VmHWM` when the operation ends. That is the
//!   exact peak over the operation. If the kernel refuses the reset, the
//!   probe samples `VmRSS` instead.
//! - **Other unix systems:** the probe samples `ps -o rss= -p <pid>` at most
//!   every [`SAMPLE_EVERY`], plus whenever the caller forces a sample. A
//!   sampled peak can miss a short spike, but not retention, which only
//!   grows until the operation ends and is sampled then.
//! - **Elsewhere:** no probe; the guard reports the check as skipped.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Minimum interval between throttled samples.
pub const SAMPLE_EVERY: Duration = Duration::from_millis(25);

/// What a probe measured over one operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemReport {
    /// `linux_vmhwm` (exact peak after `clear_refs`), `linux_vmrss_sampled`,
    /// or `ps_rss_sampled`.
    pub method: String,
    /// Resident bytes when the operation started.
    pub baseline_bytes: u64,
    /// Peak resident bytes during the operation.
    pub peak_bytes: u64,
    /// `peak_bytes - baseline_bytes`: what the operation added.
    pub growth_bytes: u64,
    /// Samples taken (1 plus the final reading for `linux_vmhwm`).
    pub samples: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Method {
    LinuxHwm,
    LinuxSampled,
    PsSampled,
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Self::LinuxHwm => "linux_vmhwm",
            Self::LinuxSampled => "linux_vmrss_sampled",
            Self::PsSampled => "ps_rss_sampled",
        }
    }
}

/// A running probe. Start it before the operation, call [`MemProbe::sample`]
/// often (it throttles itself), and [`MemProbe::finish`] while everything
/// the operation holds is still alive.
#[derive(Debug)]
pub struct MemProbe {
    method: Method,
    baseline: u64,
    peak: u64,
    samples: u64,
    last: Instant,
}

fn proc_status_kb(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with(field))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn ps_rss_bytes() -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let kb: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kb * 1024)
}

impl MemProbe {
    /// Starts a probe, or `None` where the platform offers no safe reading.
    #[must_use]
    pub fn start() -> Option<Self> {
        let (method, baseline) = if std::path::Path::new("/proc/self/status").exists() {
            let reset = std::fs::write("/proc/self/clear_refs", "5").is_ok();
            let method = if reset {
                Method::LinuxHwm
            } else {
                Method::LinuxSampled
            };
            (method, proc_status_kb("VmRSS:")? * 1024)
        } else if cfg!(unix) {
            (Method::PsSampled, ps_rss_bytes()?)
        } else {
            return None;
        };
        Some(Self {
            method,
            baseline,
            peak: baseline,
            samples: 1,
            last: Instant::now(),
        })
    }

    fn current(&self) -> Option<u64> {
        match self.method {
            Method::LinuxHwm | Method::LinuxSampled => proc_status_kb("VmRSS:").map(|kb| kb * 1024),
            Method::PsSampled => ps_rss_bytes(),
        }
    }

    /// Samples if [`SAMPLE_EVERY`] has passed since the last sample. A no-op
    /// when the kernel tracks the peak itself.
    pub fn sample(&mut self) {
        if self.method != Method::LinuxHwm && self.last.elapsed() >= SAMPLE_EVERY {
            self.force_sample();
        }
    }

    /// Samples now.
    pub fn force_sample(&mut self) {
        if let Some(rss) = self.current() {
            self.peak = self.peak.max(rss);
            self.samples += 1;
        }
        self.last = Instant::now();
    }

    /// Ends the probe. Call it before dropping what the operation holds.
    #[must_use]
    pub fn finish(mut self) -> MemReport {
        if self.method == Method::LinuxHwm {
            if let Some(hwm) = proc_status_kb("VmHWM:") {
                self.peak = self.peak.max(hwm * 1024);
                self.samples += 1;
            }
        } else {
            self.force_sample();
        }
        MemReport {
            method: self.method.name().to_string(),
            baseline_bytes: self.baseline,
            peak_bytes: self.peak,
            growth_bytes: self.peak.saturating_sub(self.baseline),
            samples: self.samples,
        }
    }
}
