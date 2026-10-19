//! Thread cycles. Every thread runs in cycles of its own length, set per thread class
//! (`--cycle-<class>-ms`): 20 ms for the RTP threads, 100 ms for the others. A
//! [`CycleMeter`] times each cycle on its own thread:
//! - **delta:** how long since the previous cycle started. It should be the cycle length.
//! - **late:** how far past its due time the cycle started. More than a quarter of a cycle
//!   late means the thread is **behind**. That is logged at most once a second, with how
//!   many cycles were late since the last time.
//! - **load:** the share of time the thread spent working rather than waiting, over 1 s
//!   windows. [`CycleMeter::load_changed`] says when it moved by 5 points or more, and only
//!   then publishes it. RTP thread binding compares the published loads.
//!
//! What a meter measures goes into its [`ThreadStats`], plain atomics written only by the
//! owning thread. The main thread reads them for `stats` and binding, and the watchdog
//! reads them to find hung threads.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Cycle length for each thread class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cycles {
    pub main: Duration,
    pub rtp: Duration,
    pub io: Duration,
    pub log: Duration,
    pub watchdog: Duration,
}

impl Default for Cycles {
    fn default() -> Self {
        let ms = Duration::from_millis;
        Cycles { main: ms(100), rtp: ms(20), io: ms(100), log: ms(100), watchdog: ms(100) }
    }
}

/// Load is measured over windows of this length (or one cycle, if that's longer).
const WINDOW: Duration = Duration::from_secs(1);
/// A cycle more than 1/4 of its length late means the thread is behind.
const BEHIND_DIVISOR: u32 = 4;
/// Being behind is logged at most this often per thread.
const BEHIND_LOG_EVERY: Duration = Duration::from_secs(1);
/// `load_changed` reports moves of at least this much, in permille (5 points).
pub const LOAD_STEP: u32 = 50;

/// All monotonic times in [`ThreadStats`] count from here.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn micros(t: Instant) -> u64 {
    t.saturating_duration_since(epoch()).as_micros() as u64
}

/// What one thread's meter publishes. Written only by that thread, readable from any.
pub struct ThreadStats {
    name: String,
    cycle: Duration,
    measures_load: bool,
    last_cycle_us: AtomicU64,
    cycles: AtomicU64,
    late_cycles: AtomicU64,
    /// Worst lateness in the last window.
    max_late_us: AtomicU64,
    /// Load as last reported by `load_changed`, in permille.
    load: AtomicU32,
    stopped: AtomicBool,
}

impl ThreadStats {
    fn new(name: String, cycle: Duration, measures_load: bool) -> ThreadStats {
        ThreadStats {
            name,
            cycle,
            measures_load,
            last_cycle_us: AtomicU64::new(micros(Instant::now())),
            cycles: AtomicU64::new(0),
            late_cycles: AtomicU64::new(0),
            max_late_us: AtomicU64::new(0),
            load: AtomicU32::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn cycle(&self) -> Duration {
        self.cycle
    }

    /// Cycles that started behind, since the thread started.
    pub fn late_cycles(&self) -> u64 {
        self.late_cycles.load(Ordering::Relaxed)
    }

    /// The load last published by `load_changed`, in permille.
    pub fn load(&self) -> u32 {
        self.load.load(Ordering::Relaxed)
    }

    /// How long since the thread last started a cycle.
    pub fn since_last_cycle(&self, now: Instant) -> Duration {
        Duration::from_micros(micros(now).saturating_sub(self.last_cycle_us.load(Ordering::Relaxed)))
    }

    /// The thread has ended (its meter was dropped).
    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self, now: Instant) -> ThreadSnapshot {
        ThreadSnapshot {
            thread: self.name.clone(),
            cycle_ms: self.cycle.as_millis() as u64,
            cycles: self.cycles.load(Ordering::Relaxed),
            late_cycles: self.late_cycles.load(Ordering::Relaxed),
            max_late_ms: round_ms(Duration::from_micros(self.max_late_us.load(Ordering::Relaxed))),
            load_pct: self.measures_load.then(|| self.load() as f64 / 10.0),
            since_last_cycle_ms: self.since_last_cycle(now).as_millis() as u64,
            legs: None,
        }
    }
}

/// A thread's cycle figures, as reported by `stats`.
#[derive(Debug, Clone, Serialize)]
pub struct ThreadSnapshot {
    pub thread: String,
    pub cycle_ms: u64,
    pub cycles: u64,
    /// Cycles that started more than a quarter of a cycle late.
    pub late_cycles: u64,
    /// The worst lateness over the last second.
    pub max_late_ms: f64,
    /// Share of time spent working, as last published. `None` where it isn't measured (main).
    pub load_pct: Option<f64>,
    pub since_last_cycle_ms: u64,
    /// RTP threads: legs bound to the thread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legs: Option<usize>,
}

fn round_ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 10_000.0).round() / 10.0
}

/// Every running thread's stats, for `stats` and the watchdog.
#[derive(Clone, Default)]
pub struct Registry {
    threads: Arc<Mutex<Vec<Arc<ThreadStats>>>>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<String> = self.threads().iter().map(|t| t.name.clone()).collect();
        f.debug_struct("Registry").field("threads", &names).finish()
    }
}

impl Registry {
    pub fn new() -> Registry {
        epoch();
        Registry::default()
    }

    /// Adds a thread. Its [`CycleMeter`] publishes into the returned stats. `measures_load`
    /// is false for a thread whose busy time can't be seen (the main thread's runtime).
    pub fn register(&self, name: impl Into<String>, cycle: Duration, measures_load: bool) -> Arc<ThreadStats> {
        let stats = Arc::new(ThreadStats::new(name.into(), cycle, measures_load));
        let mut threads = self.threads.lock().unwrap();
        threads.retain(|t| !t.stopped());
        threads.push(stats.clone());
        stats
    }

    /// The threads still running.
    pub fn threads(&self) -> Vec<Arc<ThreadStats>> {
        let mut threads = self.threads.lock().unwrap();
        threads.retain(|t| !t.stopped());
        threads.clone()
    }

    pub fn snapshot(&self) -> Vec<ThreadSnapshot> {
        let now = Instant::now();
        self.threads().iter().map(|t| t.snapshot(now)).collect()
    }
}

/// How one cycle went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CycleTiming {
    /// Since the previous cycle started.
    pub delta: Duration,
    /// Past the time the cycle was due.
    pub late: Duration,
    pub behind: bool,
    /// Cycles skipped because the thread was too far behind to catch up. Their work is not
    /// done; an RTP thread fills their frames with silence.
    pub skipped: u32,
}

/// Times one thread's cycles. Owned by that thread.
pub struct CycleMeter {
    stats: Arc<ThreadStats>,
    cycle: Duration,
    /// Cycles run back to back when late, before the rest are skipped.
    max_catch_up: u32,
    due: Instant,
    last_start: Option<Instant>,
    awake_since: Option<Instant>,
    window_start: Instant,
    window_busy: Duration,
    window_max_late: Duration,
    /// Load of the last finished window, until `load_changed` looks at it.
    pending: Option<u32>,
    reported: Option<u32>,
    last_behind_log: Option<Instant>,
    late_since_log: u64,
}

impl CycleMeter {
    /// The first cycle is due one cycle after `now`. A thread that is late runs up to
    /// `max_catch_up` overdue cycles back to back; beyond that they are skipped.
    pub fn new(stats: Arc<ThreadStats>, now: Instant, max_catch_up: u32) -> CycleMeter {
        let cycle = stats.cycle.max(Duration::from_millis(1));
        CycleMeter {
            stats,
            cycle,
            max_catch_up,
            due: now + cycle,
            last_start: None,
            awake_since: Some(now),
            window_start: now,
            window_busy: Duration::ZERO,
            window_max_late: Duration::ZERO,
            pending: None,
            reported: None,
            last_behind_log: None,
            late_since_log: 0,
        }
    }

    pub fn name(&self) -> &str {
        &self.stats.name
    }

    pub fn cycle(&self) -> Duration {
        self.cycle
    }

    /// When the next cycle is due: the latest the thread should wait until.
    pub fn deadline(&self) -> Instant {
        self.due
    }

    pub fn is_due(&self, now: Instant) -> bool {
        now >= self.due
    }

    /// The thread stopped waiting (its poll or receive returned).
    pub fn wake(&mut self, now: Instant) {
        if self.awake_since.is_none() {
            self.awake_since = Some(now);
        }
    }

    /// The thread is about to wait. The time since `wake` counts as work.
    pub fn sleep(&mut self, now: Instant) {
        if let Some(since) = self.awake_since.take() {
            self.window_busy += now.saturating_duration_since(since);
        }
    }

    /// Starts the cycle that is due, at `now`.
    pub fn start_cycle(&mut self, now: Instant) -> CycleTiming {
        let late = now.saturating_duration_since(self.due);
        let delta = self.last_start.map_or(self.cycle, |s| now.saturating_duration_since(s));
        self.last_start = Some(now);
        let behind = late > self.cycle / BEHIND_DIVISOR;

        // The next cycle keeps the phase. When more cycles are already overdue than the
        // thread may catch up on, they are skipped.
        self.due += self.cycle;
        let mut skipped = 0;
        if now >= self.due {
            let overdue = ((now - self.due).as_nanos() / self.cycle.as_nanos()) as u32 + 1;
            if overdue > self.max_catch_up {
                skipped = overdue;
                self.due += self.cycle * overdue;
            }
        }

        let stats = &self.stats;
        stats.cycles.fetch_add(1, Ordering::Relaxed);
        stats.last_cycle_us.store(micros(now), Ordering::Relaxed);
        self.window_max_late = self.window_max_late.max(late);
        if behind {
            stats.late_cycles.fetch_add(1, Ordering::Relaxed);
            self.late_since_log += 1;
            if self.last_behind_log.is_none_or(|t| now.saturating_duration_since(t) >= BEHIND_LOG_EVERY) {
                tracing::warn!(
                    thread = %stats.name,
                    cycle_ms = self.cycle.as_millis() as u64,
                    delta_ms = round_ms(delta),
                    late_ms = round_ms(late),
                    late_cycles = self.late_since_log,
                    skipped_cycles = skipped,
                    "thread is behind its cycle"
                );
                self.last_behind_log = Some(now);
                self.late_since_log = 0;
            }
        }
        self.close_window(now);
        CycleTiming { delta, late, behind, skipped }
    }

    fn close_window(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.window_start);
        if elapsed < WINDOW.max(self.cycle) {
            return;
        }
        // Work in progress counts up to now.
        if let Some(since) = &mut self.awake_since {
            self.window_busy += now.saturating_duration_since(*since);
            *since = now;
        }
        let load = (self.window_busy.as_nanos() * 1000 / elapsed.as_nanos().max(1)).min(1000) as u32;
        self.stats.max_late_us.store(self.window_max_late.as_micros() as u64, Ordering::Relaxed);
        if self.stats.measures_load {
            self.pending = Some(load);
        }
        self.window_start = now;
        self.window_busy = Duration::ZERO;
        self.window_max_late = Duration::ZERO;
    }

    /// Whether the thread's load has changed: `Some(permille)` when a measuring window has
    /// ended since the last call and its load is at least [`LOAD_STEP`] away from the last
    /// load reported (or none was reported yet). The new load is then published in the
    /// thread's stats; smaller moves are not.
    pub fn load_changed(&mut self) -> Option<u32> {
        let load = self.pending.take()?;
        if self.reported.is_some_and(|r| r.abs_diff(load) < LOAD_STEP) {
            return None;
        }
        self.reported = Some(load);
        self.stats.load.store(load, Ordering::Relaxed);
        Some(load)
    }
}

impl Drop for CycleMeter {
    fn drop(&mut self) {
        self.stats.stopped.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn meter(cycle_ms: u32, catch_up: u32) -> (Registry, CycleMeter, Instant) {
        let registry = Registry::new();
        let stats = registry.register("t", cycle_ms * MS, true);
        let t0 = Instant::now();
        (registry, CycleMeter::new(stats, t0, catch_up), t0)
    }

    #[test]
    fn cycles_on_time_have_the_cycle_as_delta_and_are_not_behind() {
        let (_r, mut m, t0) = meter(20, 0);
        for i in 1..=5u32 {
            let now = t0 + 20 * i * MS + MS / 10;
            assert!(m.is_due(now));
            let c = m.start_cycle(now);
            assert!(!c.behind && c.skipped == 0);
            if i > 1 {
                assert_eq!(c.delta, 20 * MS);
            }
            assert_eq!(m.deadline(), t0 + 20 * (i + 1) * MS, "the phase is kept");
        }
    }

    #[test]
    fn a_late_cycle_is_behind_and_counted() {
        let (r, mut m, t0) = meter(20, 0);
        m.start_cycle(t0 + 20 * MS);
        let c = m.start_cycle(t0 + 40 * MS + 8 * MS);
        assert!(c.behind, "8 ms late on a 20 ms cycle");
        assert_eq!((c.delta, c.late), (28 * MS, 8 * MS));
        assert_eq!(r.snapshot()[0].late_cycles, 1);
        assert!(!m.start_cycle(t0 + 60 * MS + 4 * MS).behind, "4 ms is within a quarter cycle");
    }

    #[test]
    fn overdue_cycles_are_caught_up_or_skipped() {
        // Woken 70 ms late: the cycles due at 40, 60 and 80 are overdue by then.
        let (_r, mut m, t0) = meter(20, 5);
        let c = m.start_cycle(t0 + 90 * MS);
        assert_eq!(c.skipped, 0, "within the catch-up allowance");
        assert_eq!(m.deadline(), t0 + 40 * MS, "the next cycle runs straight away");

        let (_r, mut m, t0) = meter(20, 1);
        let c = m.start_cycle(t0 + 90 * MS);
        assert_eq!(c.skipped, 3, "the cycles due at 40, 60 and 80");
        assert_eq!(m.deadline(), t0 + 100 * MS, "the phase is kept");
    }

    /// One cycle starting at `start` that works for `work`.
    fn busy_cycle(m: &mut CycleMeter, start: Instant, work: Duration) {
        m.wake(start);
        m.start_cycle(start);
        m.sleep(start + work);
    }

    /// A second of 20 ms cycles from `at` ms, each working `work_ms`; then `load_changed`.
    fn window(m: &mut CycleMeter, t0: Instant, at: &mut u32, work_ms: u32) -> Option<u32> {
        for _ in 0..50 {
            *at += 20;
            busy_cycle(m, t0 + *at * MS, work_ms * MS);
        }
        m.load_changed()
    }

    #[test]
    fn load_is_work_over_time_and_reported_only_when_it_changes() {
        let (r, mut m, t0) = meter(20, 0);
        m.sleep(t0);
        let mut at = 0;
        // 2 ms of work per 20 ms cycle: 10 %.
        let first = window(&mut m, t0, &mut at, 2).expect("the first window is always reported");
        assert!((95..=105).contains(&first), "{first}");
        assert_eq!(r.threads()[0].load(), first, "published");

        let second = window(&mut m, t0, &mut at, 4).expect("10 % to 20 % is reported");
        assert!(second.abs_diff(first) >= LOAD_STEP, "{second} vs {first}");
        assert_eq!(window(&mut m, t0, &mut at, 4), None, "unchanged load is not reported");
        assert_eq!(r.threads()[0].load(), second, "and not republished");
        assert!(window(&mut m, t0, &mut at, 6).is_some(), "30 % is reported");
        assert_eq!(m.load_changed(), None, "nothing new since");
    }

    #[test]
    fn a_dropped_meter_marks_its_thread_stopped() {
        let (r, m, _) = meter(20, 0);
        assert_eq!(r.threads().len(), 1);
        drop(m);
        assert!(r.threads().is_empty());
    }
}
