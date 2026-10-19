//! Wires the server together: the RTP threads, the IO thread, the watchdog, the engine
//! (which binds legs to the RTP threads), and on the calling thread's runtime the TCP
//! control channel, the idle reaper and the main thread's cycle.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::api::{self, Api, EndReason};
use crate::control;
use crate::cycle::{CycleMeter, Cycles, Registry, ThreadStats};
use crate::engine::{Engine, EngineConfig};
use crate::jitter::JitterConfig;
use crate::events::Events;
use crate::leg::{LegDeps, LegEvent, LegListener, Place, RtpThreads};
use crate::ports::PortPool;
use crate::rebalance::{self, RebalanceConfig, Reason};
use crate::recording::IoThread;
use crate::sched::ThreadPolicies;
use crate::watchdog::{Stall, StallListener, Watchdog};
use crate::webrtc::dtls::Identity;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: String,
    pub control_listen: SocketAddr,
    /// Address the media sockets bind to.
    pub media_ip: IpAddr,
    /// Address returned to the controller for the SDP.
    pub advertise_ip: IpAddr,
    pub port_min: u16,
    pub port_max: u16,
    pub idle_timeout: Duration,
    /// Most legs (call legs plus conference participants) at once.
    pub max_legs: usize,
    /// Where recordings are written.
    pub recording_dir: PathBuf,
    /// Where `play` finds its files.
    pub prompt_dir: PathBuf,
    /// Accept the test codec (`x-pulse`); test builds only.
    #[cfg(feature = "test-codec")]
    pub test_codec: bool,
    pub jitter: JitterConfig,
    /// Scheduling for each kind of thread. The server applies `rtp`, `io` and `watchdog`;
    /// `main` and `log` are applied by whoever owns those threads (the binary's `main`).
    pub threads: ThreadPolicies,
    /// Cycle length for each kind of thread. The RTP cycle is a multiple of 20 ms.
    pub cycles: Cycles,
    /// How many RTP threads legs are bound to.
    pub rtp_threads: usize,
    /// When an RTP thread is hot enough to give up a call or conference.
    pub rebalance: RebalanceConfig,
    /// A thread that hasn't started a cycle for this long (or two of its cycles, if longer)
    /// is reported hung by the watchdog.
    pub watchdog_timeout: Duration,
    /// Where threads publish their cycle figures. The log thread, started before the
    /// server, registers here too.
    pub registry: Registry,
}

pub struct Server {
    control_addr: SocketAddr,
    api: Arc<Api>,
    shutdown: CancellationToken,
    tasks: JoinSet<()>,
    io: Option<IoThread>,
    watchdog: Option<Watchdog>,
}

impl Server {
    /// Starts the server. Events go to the log until the service's own API delivers them.
    pub async fn start(cfg: Config) -> anyhow::Result<Server> {
        Self::start_with_events(cfg, Arc::new(Events::log_only())).await
    }

    /// Like `start`, with events going to `events` (tests capture them).
    pub async fn start_with_events(cfg: Config, events: Arc<Events>) -> anyhow::Result<Server> {
        let registry = &cfg.registry;
        let watchdog = Watchdog::start(
            registry.clone(),
            cfg.cycles.watchdog,
            cfg.watchdog_timeout,
            cfg.threads.watchdog,
            stall_events(&events, &cfg.node_id),
        )?;
        let io = IoThread::start(cfg.threads.io, cfg.cycles.io, registry)?;
        let pool = PortPool::new(cfg.media_ip, cfg.port_min, cfg.port_max)?;
        let deps = LegDeps { jitter: cfg.jitter, listener: leg_events(&events, &cfg.node_id), sched: cfg.threads.rtp };
        let threads = RtpThreads::start(cfg.rtp_threads, cfg.cycles.rtp, deps, registry)?;
        let identity = Arc::new(Identity::generate()?);
        tracing::info!(fingerprint = %identity.fingerprint(), "DTLS certificate for WebRTC legs");
        let rtp_threads = threads.len();
        let engine_cfg = EngineConfig {
            advertise_ip: cfg.advertise_ip,
            max_legs: cfg.max_legs,
            recording_dir: cfg.recording_dir.clone(),
            prompt_dir: cfg.prompt_dir.clone(),
            #[cfg(feature = "test-codec")]
            test_codec: cfg.test_codec,
        };
        let engine = Engine::new(engine_cfg, pool, io.handle(), threads, identity);
        let api = Arc::new(Api::new(Arc::new(engine), events.clone(), cfg.node_id.clone()));

        let listener = TcpListener::bind(cfg.control_listen).await?;
        let control_addr = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(control::serve(listener, api.clone(), shutdown.clone()));
        tasks.spawn(reap_idle(api.clone(), cfg.idle_timeout, shutdown.clone()));
        tasks.spawn(main_cycle(registry.register("main", cfg.cycles.main, false), shutdown.clone()));
        if cfg.rebalance.enabled() && rtp_threads > 1 {
            tasks.spawn(rebalance_loop(api.clone(), cfg.rebalance, shutdown.clone()));
        }

        tracing::info!(
            node = %cfg.node_id,
            control = %control_addr,
            media_ip = %cfg.media_ip,
            advertise_ip = %cfg.advertise_ip,
            ports = %format!("{}-{}", cfg.port_min, cfg.port_max),
            jitter_ms = cfg.jitter.min_delay.as_millis() as u64,
            jitter_bytes = cfg.jitter.capacity_bytes,
            rtp_threads,
            rebalance_load_pct = cfg.rebalance.load_pct,
            rebalance_late_cycles = cfg.rebalance.late_cycles,
            cycle_rtp_ms = cfg.cycles.rtp.as_millis() as u64,
            sched_rtp = %cfg.threads.rtp,
            sched_io = %cfg.threads.io,
            sched_watchdog = %cfg.threads.watchdog,
            "media server started"
        );
        Ok(Server { control_addr, api, shutdown, tasks, io: Some(io), watchdog: Some(watchdog) })
    }

    pub fn control_addr(&self) -> SocketAddr {
        self.control_addr
    }

    pub fn api(&self) -> &Arc<Api> {
        &self.api
    }

    /// Stops taking commands, ends every call and conference (finishing their recordings),
    /// and stops the RTP threads, the watchdog and the IO thread.
    pub async fn shutdown(mut self) {
        self.api.begin_shutdown();
        self.shutdown.cancel();
        while self.tasks.join_next().await.is_some() {}
        self.api.end_all();
        self.api.engine().stop_threads();
        if let Some(watchdog) = self.watchdog.take() {
            watchdog.stop();
        }
        if let Some(io) = self.io.take() {
            io.shutdown();
        }
        tracing::info!("media server stopped");
    }
}

/// Publishes what the legs report: a `dtmf` event for every digit a leg sends, and
/// `play.finished` when a file played into a leg ends.
fn leg_events(events: &Arc<Events>, node_id: &str) -> LegListener {
    let (events, node_id) = (events.clone(), node_id.to_string());
    Arc::new(move |place: &Place, event: LegEvent| {
        let (kind, extra) = match event {
            LegEvent::Dtmf(digit) => ("dtmf", serde_json::to_value(&digit)),
            LegEvent::PlayFinished(finished) => ("play.finished", serde_json::to_value(&finished)),
        };
        let mut body = place.fields();
        if let (Some(obj), Ok(Value::Object(extra))) = (body.as_object_mut(), extra) {
            obj.extend(extra);
        }
        events.publish(place.key(), &api::event(&node_id, kind, body));
    })
}

/// Publishes `thread.stalled` and `thread.recovered` for what the watchdog finds.
fn stall_events(events: &Arc<Events>, node_id: &str) -> StallListener {
    let (events, node_id) = (events.clone(), node_id.to_string());
    Arc::new(move |stall: &Stall| {
        let (kind, body) = match stall {
            Stall::Stalled { thread, cycle, stalled } => (
                "thread.stalled",
                json!({ "thread": thread, "cycle_ms": cycle.as_millis() as u64, "stalled_ms": stalled.as_millis() as u64 }),
            ),
            Stall::Recovered { thread, stalled } => {
                ("thread.recovered", json!({ "thread": thread, "stalled_ms": stalled.as_millis() as u64 }))
            }
        };
        events.publish(&node_id, &api::event(&node_id, kind, body));
    })
}

/// The main thread's cycle. Its lateness shows how long the runtime was kept from getting
/// back to it (a blocked event loop); the watchdog checks that it keeps coming.
async fn main_cycle(stats: Arc<ThreadStats>, shutdown: CancellationToken) {
    let mut meter = CycleMeter::new(stats, Instant::now(), 0);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep_until(meter.deadline().into()) => {}
        }
        meter.start_cycle(Instant::now());
    }
}

/// Rebalancing, once a second (see [`crate::rebalance`]).
async fn rebalance_loop(api: Arc<Api>, cfg: RebalanceConfig, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(rebalance::EVERY);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        for moved in api.engine().rebalance(&cfg) {
            let (why, value) = match moved.reason {
                Reason::Load(permille) => ("load_pct", permille as f64 / 10.0),
                Reason::Late(cycles) => ("late_cycles", cycles as f64),
            };
            tracing::info!(
                group = %moved.group,
                legs = moved.legs,
                from = format!("rtp-{}", moved.from),
                to = format!("rtp-{}", moved.to),
                why,
                value,
                "rebalanced: moved to a cooler rtp thread"
            );
        }
    }
}

/// Ends calls where neither leg has sent anything for `timeout`, and removes conference
/// participants that haven't.
async fn reap_idle(api: Arc<Api>, timeout: Duration, shutdown: CancellationToken) {
    let tick = (timeout / 4).clamp(Duration::from_millis(50), Duration::from_secs(1));
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        let (calls, participants) = api.engine().idle(timeout);
        for call_id in calls {
            // A concurrent delete may have ended it already; that's fine.
            let _ = api.end_call(&call_id, EndReason::IdleTimeout);
        }
        for (conference_id, participant_id) in participants {
            api.end_participant(&conference_id, &participant_id, EndReason::IdleTimeout);
        }
    }
}
