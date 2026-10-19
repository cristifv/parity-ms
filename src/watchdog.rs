//! The watchdog thread. Every cycle it checks that each thread has started a cycle
//! recently. A thread that hasn't for the watchdog timeout (or two of its own cycles, if
//! that's longer) is hung:
//! - a critical error is logged;
//! - `thread.stalled` is published.
//!
//! When the thread cycles again, `thread.recovered` is published. The watchdog does nothing
//! else: it doesn't restart, move or end anything.
//!
//! It should run at a higher priority than the threads it watches (`--sched-watchdog`), so
//! a busy real-time RTP thread can't keep it from noticing.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cycle::{CycleMeter, Registry, ThreadStats};
use crate::sched::Policy;

/// A thread the watchdog found hung, or found running again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stall {
    Stalled { thread: String, cycle: Duration, stalled: Duration },
    Recovered { thread: String, stalled: Duration },
}

/// Receives every stall and recovery (the server publishes them as events).
pub type StallListener = Arc<dyn Fn(&Stall) + Send + Sync>;

pub struct Watchdog {
    stop: Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    pub fn start(
        registry: Registry,
        cycle: Duration,
        timeout: Duration,
        policy: Policy,
        listener: StallListener,
    ) -> io::Result<Watchdog> {
        let (stop, stopped) = mpsc::channel::<()>();
        let stats = registry.register("watchdog", cycle, true);
        let thread = std::thread::Builder::new().name("watchdog".into()).spawn(move || {
            policy.apply_or_warn("watchdog");
            let mut meter = CycleMeter::new(stats, Instant::now(), 0);
            let mut checker = Checker::new(timeout);
            loop {
                let now = Instant::now();
                meter.sleep(now);
                match stopped.recv_timeout(meter.deadline().saturating_duration_since(now)) {
                    Err(RecvTimeoutError::Timeout) => {}
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
                }
                let now = Instant::now();
                meter.wake(now);
                if !meter.is_due(now) {
                    continue;
                }
                meter.start_cycle(now);
                for stall in checker.check(&registry.threads(), now) {
                    report(&stall);
                    listener(&stall);
                }
                meter.load_changed();
            }
        })?;
        Ok(Watchdog { stop, thread: Some(thread) })
    }

    pub fn stop(mut self) {
        let _ = self.stop.send(());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn report(stall: &Stall) {
    match stall {
        Stall::Stalled { thread, cycle, stalled } => {
            tracing::error!(
                critical = true,
                thread = %thread,
                cycle_ms = cycle.as_millis() as u64,
                stalled_ms = stalled.as_millis() as u64,
                "CRITICAL: thread hung"
            );
            if thread == "log" {
                // The log thread can't write its own obituary.
                eprintln!("CRITICAL: thread hung: log, no cycle for {} ms", stalled.as_millis());
            }
        }
        Stall::Recovered { thread, stalled } => {
            tracing::warn!(thread = %thread, stalled_ms = stalled.as_millis() as u64, "thread recovered");
        }
    }
}

/// The watchdog's checks, kept apart from its thread.
pub struct Checker {
    timeout: Duration,
    /// Hung threads, with when each last started a cycle.
    hung: HashMap<String, Instant>,
}

impl Checker {
    pub fn new(timeout: Duration) -> Checker {
        Checker { timeout, hung: HashMap::new() }
    }

    /// Threads that became hung, or stopped being hung, since the last check.
    pub fn check(&mut self, threads: &[Arc<ThreadStats>], now: Instant) -> Vec<Stall> {
        let mut changes = Vec::new();
        for t in threads {
            let since = t.since_last_cycle(now);
            let last_cycle = now - since;
            let limit = self.timeout.max(t.cycle() * 2);
            match self.hung.get(t.name()) {
                None if since >= limit => {
                    self.hung.insert(t.name().to_string(), last_cycle);
                    changes.push(Stall::Stalled { thread: t.name().to_string(), cycle: t.cycle(), stalled: since });
                }
                Some(&before) if since < limit => {
                    self.hung.remove(t.name());
                    let stalled = last_cycle.saturating_duration_since(before);
                    changes.push(Stall::Recovered { thread: t.name().to_string(), stalled });
                }
                _ => {}
            }
        }
        // Threads that ended are no longer watched.
        self.hung.retain(|name, _| threads.iter().any(|t| t.name() == name));
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_without_cycles_is_reported_once_then_recovers() {
        let registry = Registry::new();
        let stats = registry.register("rtp-0", Duration::from_millis(20), true);
        let t0 = Instant::now();
        let mut meter = CycleMeter::new(stats, t0, 0);
        let mut checker = Checker::new(Duration::from_millis(500));

        meter.start_cycle(t0 + Duration::from_millis(20));
        assert!(checker.check(&registry.threads(), t0 + Duration::from_millis(400)).is_empty());

        let late = t0 + Duration::from_millis(600);
        match checker.check(&registry.threads(), late).as_slice() {
            [Stall::Stalled { thread, stalled, .. }] => {
                assert_eq!(thread, "rtp-0");
                assert!(*stalled >= Duration::from_millis(500));
            }
            other => panic!("{other:?}"),
        }
        assert!(checker.check(&registry.threads(), late).is_empty(), "reported once");

        let back = t0 + Duration::from_millis(900);
        meter.start_cycle(back);
        match checker.check(&registry.threads(), back).as_slice() {
            [Stall::Recovered { thread, stalled }] => {
                assert_eq!(thread, "rtp-0");
                assert!(*stalled >= Duration::from_millis(800));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_long_cycle_gets_two_cycles_before_it_counts_as_hung() {
        let registry = Registry::new();
        let stats = registry.register("io", Duration::from_secs(1), true);
        let t0 = Instant::now();
        let _meter = CycleMeter::new(stats, t0, 0);
        let mut checker = Checker::new(Duration::from_millis(500));
        let threads = registry.threads();
        assert!(checker.check(&threads, t0 + Duration::from_millis(1500)).is_empty(), "1.5 s < two 1 s cycles");
        assert_eq!(checker.check(&threads, t0 + Duration::from_millis(2100)).len(), 1);
    }
}
