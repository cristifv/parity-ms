//! The main thread: parses the command line, starts the log thread and the server (RTP,
//! IO and watchdog threads), then serves the TCP control channel until SIGTERM or Ctrl-C.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use media_server::cycle::{Cycles, Registry};
use media_server::jitter::{DEFAULT_CAPACITY_BYTES, JitterConfig};
use media_server::rebalance::RebalanceConfig;
use media_server::sched::{Policy, ThreadPolicies};
use media_server::{Config, Server, logging};

/// Media server: relays, transcodes, mixes and records RTP over UDP; control over TCP.
#[derive(Debug, Parser)]
#[command(name = "media-server", version)]
struct Args {
    #[arg(long, env = "MEDIA_NODE_ID", default_value = "media-1")]
    node_id: String,

    /// TCP address of the control channel.
    #[arg(long, env = "MEDIA_CONTROL_LISTEN", default_value = "127.0.0.1:2223")]
    control_listen: SocketAddr,

    /// Address the media sockets bind to.
    #[arg(long, env = "MEDIA_IP", default_value = "0.0.0.0")]
    media_ip: IpAddr,

    /// Address returned for the SDP. Required when --media-ip is 0.0.0.0 or ::.
    #[arg(long, env = "MEDIA_ADVERTISE_IP")]
    advertise_ip: Option<IpAddr>,

    /// First UDP port for media. Each leg takes an even port (RTP) and the next (RTCP).
    #[arg(long, env = "MEDIA_PORT_MIN", default_value_t = 30000)]
    port_min: u16,

    /// Last UDP port for media.
    #[arg(long, env = "MEDIA_PORT_MAX", default_value_t = 39999)]
    port_max: u16,

    /// End a call, or drop a conference participant, after this many seconds without media.
    #[arg(long, env = "MEDIA_IDLE_TIMEOUT_SECS", default_value_t = 60)]
    idle_timeout_secs: u64,

    /// Most legs at once.
    #[arg(long, env = "MEDIA_MAX_LEGS", default_value_t = 4000)]
    max_legs: usize,

    /// Directory for recordings (stereo 16-bit PCM .au files).
    #[arg(long, env = "MEDIA_RECORDING_DIR", default_value = "recordings")]
    recording_dir: PathBuf,

    /// Directory `play` takes its files from (WAV or .au, 8 kHz mono). Nothing outside it
    /// can be played.
    #[arg(long, env = "MEDIA_PROMPT_DIR", default_value = "prompts")]
    prompt_dir: PathBuf,

    /// Accept the test codec (x-pulse/8000) from endpoints that offer it. Only in builds
    /// with the `test-codec` feature, for tests.
    #[cfg(feature = "test-codec")]
    #[arg(long, env = "MEDIA_TEST_CODEC")]
    test_codec: bool,

    /// Jitter buffer: starting (and smallest) play-out delay, in milliseconds.
    #[arg(long, env = "MEDIA_JITTER_DELAY_MS", default_value_t = 20)]
    jitter_delay_ms: u64,

    /// Jitter buffer: largest play-out delay it adapts up to, in milliseconds.
    #[arg(long, env = "MEDIA_JITTER_MAX_DELAY_MS", default_value_t = 200)]
    jitter_max_delay_ms: u64,

    /// Jitter buffer: bytes of packets it can hold.
    #[arg(long, env = "MEDIA_JITTER_CAPACITY_BYTES", default_value_t = DEFAULT_CAPACITY_BYTES)]
    jitter_capacity_bytes: usize,

    /// Log as JSON lines instead of text.
    #[arg(long, env = "MEDIA_LOG_JSON")]
    log_json: bool,

    /// Scheduling of the main thread (TCP control): fifo:N, rr:N (real-time,
    /// 1-99), other:N, batch:N (nice -20..19), idle or inherit.
    #[arg(long, env = "MEDIA_SCHED_MAIN", default_value = "other:0")]
    sched_main: Policy,

    /// Scheduling of the RTP threads. Real-time needs CAP_SYS_NICE or an RLIMIT_RTPRIO
    /// allowance.
    #[arg(long, env = "MEDIA_SCHED_RTP", default_value = "fifo:70")]
    sched_rtp: Policy,

    /// Scheduling of the IO thread (recordings).
    #[arg(long, env = "MEDIA_SCHED_IO", default_value = "other:5")]
    sched_io: Policy,

    /// Scheduling of the log thread.
    #[arg(long, env = "MEDIA_SCHED_LOG", default_value = "other:10")]
    sched_log: Policy,

    /// Scheduling of the watchdog thread. Above the RTP threads, so they can't starve it.
    #[arg(long, env = "MEDIA_SCHED_WATCHDOG", default_value = "fifo:80")]
    sched_watchdog: Policy,

    /// RTP threads that legs are bound to. 0: one per CPU core.
    #[arg(long, env = "MEDIA_RTP_THREADS", default_value_t = 0)]
    rtp_threads: usize,

    /// Rebalancing: an RTP thread at or over this load (percent) gives up a call or
    /// conference to a cooler thread. 0 turns this trigger off.
    #[arg(long, env = "MEDIA_REBALANCE_LOAD_PCT", default_value_t = 70)]
    rebalance_load_pct: u32,

    /// Rebalancing: an RTP thread with this many late cycles in a second gives up a call
    /// or conference too (its core is crowded). 0 turns this trigger off.
    #[arg(long, env = "MEDIA_REBALANCE_LATE_CYCLES", default_value_t = 5)]
    rebalance_late_cycles: u64,

    /// Cycle of the main thread, in milliseconds.
    #[arg(long, env = "MEDIA_CYCLE_MAIN_MS", default_value_t = 100)]
    cycle_main_ms: u64,

    /// Cycle of the RTP threads, in milliseconds: a multiple of the 20 ms frame. Each cycle
    /// runs that many milliseconds of frames.
    #[arg(long, env = "MEDIA_CYCLE_RTP_MS", default_value_t = 20)]
    cycle_rtp_ms: u64,

    /// Cycle of the IO thread, in milliseconds.
    #[arg(long, env = "MEDIA_CYCLE_IO_MS", default_value_t = 100)]
    cycle_io_ms: u64,

    /// Cycle of the log thread, in milliseconds.
    #[arg(long, env = "MEDIA_CYCLE_LOG_MS", default_value_t = 100)]
    cycle_log_ms: u64,

    /// Cycle of the watchdog thread, in milliseconds.
    #[arg(long, env = "MEDIA_CYCLE_WATCHDOG_MS", default_value_t = 100)]
    cycle_watchdog_ms: u64,

    /// The watchdog reports a thread hung after this many milliseconds without a cycle
    /// (or two of the thread's cycles, if longer).
    #[arg(long, env = "MEDIA_WATCHDOG_TIMEOUT_MS", default_value_t = 1000)]
    watchdog_timeout_ms: u64,
}

impl Args {
    fn cycles(&self) -> anyhow::Result<Cycles> {
        let ms = Duration::from_millis;
        for (flag, value) in [
            ("--cycle-main-ms", self.cycle_main_ms),
            ("--cycle-io-ms", self.cycle_io_ms),
            ("--cycle-log-ms", self.cycle_log_ms),
            ("--cycle-watchdog-ms", self.cycle_watchdog_ms),
        ] {
            if !(1..=60_000).contains(&value) {
                bail!("{flag} must be between 1 and 60000");
            }
        }
        if self.cycle_rtp_ms == 0 || !self.cycle_rtp_ms.is_multiple_of(20) || self.cycle_rtp_ms > 200 {
            bail!("--cycle-rtp-ms must be a multiple of 20 (the frame), up to 200");
        }
        Ok(Cycles {
            main: ms(self.cycle_main_ms),
            rtp: ms(self.cycle_rtp_ms),
            io: ms(self.cycle_io_ms),
            log: ms(self.cycle_log_ms),
            watchdog: ms(self.cycle_watchdog_ms),
        })
    }

    fn into_config(self, registry: Registry) -> anyhow::Result<Config> {
        let cycles = self.cycles()?;
        let advertise_ip = match self.advertise_ip {
            Some(ip) => ip,
            None if self.media_ip.is_unspecified() => {
                bail!("--advertise-ip is required when --media-ip is {}", self.media_ip)
            }
            None => self.media_ip,
        };
        if self.port_min >= self.port_max {
            bail!("--port-min must be below --port-max");
        }
        if self.jitter_delay_ms == 0 || self.jitter_delay_ms > self.jitter_max_delay_ms {
            bail!("--jitter-delay-ms must be at least 1 and at most --jitter-max-delay-ms");
        }
        if self.rebalance_load_pct > 100 {
            bail!("--rebalance-load-pct is a percentage (0 turns it off)");
        }
        if self.jitter_capacity_bytes < 1500 {
            bail!("--jitter-capacity-bytes must hold at least one packet (1500 bytes)");
        }
        Ok(Config {
            node_id: self.node_id,
            control_listen: self.control_listen,
            media_ip: self.media_ip,
            advertise_ip,
            port_min: self.port_min,
            port_max: self.port_max,
            idle_timeout: Duration::from_secs(self.idle_timeout_secs),
            max_legs: self.max_legs,
            recording_dir: self.recording_dir,
            prompt_dir: self.prompt_dir,
            #[cfg(feature = "test-codec")]
            test_codec: self.test_codec,
            jitter: JitterConfig {
                min_delay: Duration::from_millis(self.jitter_delay_ms),
                max_delay: Duration::from_millis(self.jitter_max_delay_ms),
                capacity_bytes: self.jitter_capacity_bytes,
            },
            threads: ThreadPolicies {
                main: self.sched_main,
                rtp: self.sched_rtp,
                io: self.sched_io,
                log: self.sched_log,
                watchdog: self.sched_watchdog,
            },
            cycles,
            rebalance: RebalanceConfig { load_pct: self.rebalance_load_pct, late_cycles: self.rebalance_late_cycles },
            rtp_threads: match self.rtp_threads {
                0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
                n => n,
            },
            watchdog_timeout: Duration::from_millis(self.watchdog_timeout_ms.max(1)),
            registry,
        })
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let registry = Registry::new();
    let _log = logging::init(args.log_json, args.sched_log, args.cycles()?.log, &registry);
    let config = args.into_config(registry)?;
    config.threads.main.apply_or_warn("main");
    // The control plane runs here, on the main thread.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let server = Server::start(config).await?;
        wait_for_signal().await;
        tracing::info!("shutting down");
        server.shutdown().await;
        anyhow::Ok(())
    })
}

async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
