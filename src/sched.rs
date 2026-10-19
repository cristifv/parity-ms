//! Scheduling policy and priority for each kind of thread.
//!
//! Each thread class (main, rtp, io, log, watchdog) gets its own setting. RTP threads
//! default to the real-time FIFO class, well above everything else: a 20 ms media cycle
//! must not wait behind logging or disk writes. The watchdog defaults higher still, so a
//! busy RTP thread can't keep it from noticing a hung one.
//!
//! A setting is written as:
//! - `fifo:N` or `rr:N`: real-time, priority 1–99;
//! - `other:N` or `batch:N`: normal time sharing, nice −20–19;
//! - `idle`;
//! - `inherit`: leave the thread as it starts.
//!
//! Real-time classes need `CAP_SYS_NICE` or an `RLIMIT_RTPRIO` allowance (e.g.
//! `LimitRTPRIO=` in systemd, `--cap-add SYS_NICE` or `--ulimit rtprio=99` in Docker).
//! Without it the thread keeps running with its current scheduling, and a warning is logged
//! once per thread class.

use std::fmt;
use std::io;
use std::str::FromStr;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Inherit,
    /// Normal time sharing (SCHED_OTHER); the value is the nice level.
    Other(i32),
    /// Time sharing for batch work (SCHED_BATCH); the value is the nice level.
    Batch(i32),
    /// Lowest (SCHED_IDLE).
    Idle,
    /// Real-time, first in first out (SCHED_FIFO); the value is the priority.
    Fifo(i32),
    /// Real-time, round robin (SCHED_RR); the value is the priority.
    RoundRobin(i32),
}

impl FromStr for Policy {
    type Err = String;

    fn from_str(s: &str) -> Result<Policy, String> {
        let (name, value) = match s.split_once(':') {
            Some((n, v)) => (n, Some(v.trim().parse::<i32>().map_err(|_| format!("bad number in {s:?}"))?)),
            None => (s, None),
        };
        let nice = |v: Option<i32>| match v.unwrap_or(0) {
            n @ -20..=19 => Ok(n),
            n => Err(format!("nice level {n} is outside -20..=19")),
        };
        let rt = |v: Option<i32>| match v {
            Some(p @ 1..=99) => Ok(p),
            Some(p) => Err(format!("real-time priority {p} is outside 1..=99")),
            None => Err(format!("{s:?} needs a priority, e.g. {name}:50")),
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "inherit" => Ok(Policy::Inherit),
            "other" | "normal" => Ok(Policy::Other(nice(value)?)),
            "batch" => Ok(Policy::Batch(nice(value)?)),
            "idle" => Ok(Policy::Idle),
            "fifo" => Ok(Policy::Fifo(rt(value)?)),
            "rr" => Ok(Policy::RoundRobin(rt(value)?)),
            _ => Err(format!("unknown scheduling {s:?}: use fifo:N, rr:N, other:N, batch:N, idle or inherit")),
        }
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::Inherit => write!(f, "inherit"),
            Policy::Other(n) => write!(f, "other:{n}"),
            Policy::Batch(n) => write!(f, "batch:{n}"),
            Policy::Idle => write!(f, "idle"),
            Policy::Fifo(p) => write!(f, "fifo:{p}"),
            Policy::RoundRobin(p) => write!(f, "rr:{p}"),
        }
    }
}

/// The setting for each thread class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadPolicies {
    pub main: Policy,
    pub rtp: Policy,
    pub io: Policy,
    pub log: Policy,
    pub watchdog: Policy,
}

impl Default for ThreadPolicies {
    /// Leave every thread alone (tests, embedding). The binary's defaults are set on the
    /// command line.
    fn default() -> Self {
        ThreadPolicies {
            main: Policy::Inherit,
            rtp: Policy::Inherit,
            io: Policy::Inherit,
            log: Policy::Inherit,
            watchdog: Policy::Inherit,
        }
    }
}

impl Policy {
    /// Applies the policy to the calling thread.
    pub fn apply(self) -> io::Result<()> {
        imp::apply(self)
    }

    /// Applies the policy to the calling thread; on failure, warns once per `class`.
    pub fn apply_or_warn(self, class: &'static str) {
        if let Err(e) = self.apply() {
            static WARNED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
            let mut warned = WARNED.lock().unwrap();
            if !warned.contains(&class) {
                warned.push(class);
                tracing::warn!(
                    thread = class,
                    policy = %self,
                    error = %e,
                    "can't set thread scheduling; real-time needs CAP_SYS_NICE or an RLIMIT_RTPRIO allowance"
                );
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::io;

    use super::Policy;

    pub fn apply(policy: Policy) -> io::Result<()> {
        let (class, priority, nice) = match policy {
            Policy::Inherit => return Ok(()),
            Policy::Other(n) => (libc::SCHED_OTHER, 0, Some(n)),
            Policy::Batch(n) => (libc::SCHED_BATCH, 0, Some(n)),
            Policy::Idle => (libc::SCHED_IDLE, 0, None),
            Policy::Fifo(p) => (libc::SCHED_FIFO, p, None),
            Policy::RoundRobin(p) => (libc::SCHED_RR, p, None),
        };
        let param = libc::sched_param { sched_priority: priority };
        // On Linux, pid 0 with these calls means the calling thread, not the whole process.
        if unsafe { libc::sched_setscheduler(0, class, &param) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if let Some(n) = nice {
            let tid = unsafe { libc::gettid() } as libc::id_t;
            if unsafe { libc::setpriority(libc::PRIO_PROCESS, tid, n) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;

    use super::Policy;

    pub fn apply(policy: Policy) -> io::Result<()> {
        match policy {
            Policy::Inherit => Ok(()),
            _ => Err(io::Error::new(io::ErrorKind::Unsupported, "thread scheduling is only set on Linux")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_prints_settings() {
        for s in ["fifo:80", "rr:10", "other:-5", "batch:19", "idle", "inherit"] {
            assert_eq!(s.parse::<Policy>().unwrap().to_string(), s);
        }
        assert_eq!("other".parse::<Policy>(), Ok(Policy::Other(0)));
        assert_eq!("FIFO:1".parse::<Policy>(), Ok(Policy::Fifo(1)));
        assert!("fifo".parse::<Policy>().is_err(), "real-time needs a priority");
        assert!("fifo:0".parse::<Policy>().is_err());
        assert!("rr:100".parse::<Policy>().is_err());
        assert!("other:20".parse::<Policy>().is_err());
        assert!("deadline:5".parse::<Policy>().is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn applies_to_the_calling_thread_only() {
        std::thread::spawn(|| {
            // Lowering our own priority is always allowed.
            Policy::Other(10).apply().unwrap();
            let tid = unsafe { libc::gettid() } as libc::id_t;
            assert_eq!(unsafe { libc::getpriority(libc::PRIO_PROCESS, tid) }, 10);
            Policy::Batch(15).apply().unwrap();
            assert_eq!(unsafe { libc::sched_getscheduler(0) }, libc::SCHED_BATCH);
        })
        .join()
        .unwrap();
        // The test thread itself is untouched.
        assert_eq!(unsafe { libc::sched_getscheduler(0) }, libc::SCHED_OTHER);
    }
}
