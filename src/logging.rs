//! The log thread. Log lines are formatted on the thread that logs them and handed to the
//! `log` thread, which writes them to stdout, so the RTP threads never wait on output. If
//! the log thread falls far behind, lines are dropped (and counted) rather than slowing
//! media down. Like every thread, it runs a cycle of its own (`--cycle-log-ms`), which the
//! watchdog checks.

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

use crate::cycle::{CycleMeter, Registry, ThreadStats};
use crate::sched::Policy;

/// Lines that can wait for the log thread.
const QUEUE_LINES: usize = 16_384;

/// Starts the log thread and installs the global logger. Keep the guard until exit:
/// dropping it writes out what's queued and stops the thread.
pub fn init(json: bool, policy: Policy, cycle: Duration, registry: &Registry) -> LogGuard {
    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(QUEUE_LINES);
    let dropped = Arc::new(AtomicU64::new(0));
    let (applied_tx, applied) = mpsc::channel();
    let stats = registry.register("log", cycle, true);
    let thread = std::thread::Builder::new()
        .name("log".into())
        .spawn(move || {
            let _ = applied_tx.send(policy.apply());
            write_loop(rx, stats)
        })
        .expect("start the log thread");
    let writer = LogWriter { tx: Arc::new(Mutex::new(Some(tx))), dropped: dropped.clone() };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder =
        tracing_subscriber::fmt().with_env_filter(filter).with_writer(writer.clone()).with_thread_names(true);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
    // Reported now that there is a logger to report it.
    if let Ok(Err(e)) = applied.recv() {
        tracing::warn!(thread = "log", %policy, error = %e, "can't set thread scheduling");
    }
    LogGuard { writer, dropped, thread: Some(thread) }
}

fn write_loop(rx: Receiver<Vec<u8>>, stats: Arc<ThreadStats>) {
    let mut meter = CycleMeter::new(stats, Instant::now(), 0);
    let mut out = io::stdout().lock();
    loop {
        let now = Instant::now();
        meter.sleep(now);
        let line = rx.recv_timeout(meter.deadline().saturating_duration_since(now));
        meter.wake(Instant::now());
        match line {
            Ok(line) => {
                let _ = out.write_all(&line);
                // Write out everything already queued before flushing.
                while let Ok(more) = rx.try_recv() {
                    let _ = out.write_all(&more);
                }
                let _ = out.flush();
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        if meter.is_due(now) {
            meter.start_cycle(now);
            meter.load_changed();
        }
    }
}

/// Hands formatted lines to the log thread.
#[derive(Clone)]
pub struct LogWriter {
    tx: Arc<Mutex<Option<SyncSender<Vec<u8>>>>>,
    dropped: Arc<AtomicU64>,
}

/// One log line being formatted.
pub struct Line {
    buf: Vec<u8>,
    writer: LogWriter,
}

impl Write for Line {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Line {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.buf);
        let tx = self.writer.tx.lock().unwrap();
        if let Some(tx) = tx.as_ref()
            && let Err(TrySendError::Full(_)) = tx.try_send(line)
        {
            self.writer.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = Line;

    fn make_writer(&'a self) -> Line {
        Line { buf: Vec::with_capacity(256), writer: self.clone() }
    }
}

pub struct LogGuard {
    writer: LogWriter,
    dropped: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            eprintln!("log: {dropped} lines dropped because the log thread fell behind");
        }
        // Closing the channel lets the thread write out what's left and stop.
        self.writer.tx.lock().unwrap().take();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
