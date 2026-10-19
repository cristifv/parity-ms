//! RTP threads and RTP thread binding.
//!
//! A fixed set of RTP threads starts with the server (`--rtp-threads`, by default one per
//! CPU core). **RTP thread binding** puts each new leg on one of them, where it stays until
//! it ends:
//! - the thread with the lowest published load wins. Loads are compared in steps of 5
//!   points, the step at which a thread republishes its load
//!   ([`CycleMeter::load_changed`]);
//! - among threads in the same step, the one with the fewest legs wins, then the lowest
//!   number. Leg counts change at once, while loads are measured over a second, so a burst
//!   of new legs spreads out instead of piling onto one thread.
//!
//! **Moving a leg** (rebalancing, see [`crate::rebalance`]): the main thread posts
//! [`LegCmd::Move`] to the leg. Its thread then hands the leg over:
//! 1. it takes the leg out of its poll and its slots;
//! 2. while holding the leg's route lock, it sends the leg (state and sockets) to the new
//!    thread's inbox, forwards the leg's commands still queued here behind it, and points
//!    the route at the new thread;
//! 3. the new thread registers the sockets and runs the leg from its next cycle.
//!
//! Posting to a leg takes the same lock. So no command can slip in between: everything
//! sent before the swap reaches the new thread after the leg, in order, and everything
//! sent after goes there directly. The leg loses at most one frame.
//!
//! A thread waits on the sockets of all its legs and on its inbox. Every cycle (20 ms by
//! default, a multiple of the 20 ms frame) it runs the frames that are due: first every leg
//! receives (decodes and publishes what its endpoint sent), then every leg sends (mixes,
//! encodes, records). A late thread runs up to 5 overdue cycles back to back. Further
//! behind than that, it skips them and fills the missed frames with silence
//! ([`Leg::fill_blanks`]).
//!
//! Commands for a leg go through its thread's inbox, so they arrive in order and after the
//! leg was bound. A leg's state is touched by its own thread only.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};

use super::state::Leg;
use super::stream::MAX_DATAGRAM;
use super::{LegCmd, LegConfig, LegDeps, LegHandle, LegShared};
use crate::cycle::{CycleMeter, LOAD_STEP, Registry, ThreadSnapshot, ThreadStats};
use crate::ports::LegSockets;
use crate::recording::Recording;

const WAKE: Token = Token(0);
/// One media frame.
pub const FRAME: Duration = Duration::from_millis(20);
/// Overdue cycles run back to back before the rest are skipped and filled with silence.
const MAX_CATCH_UP: u32 = 5;

/// Names a leg within its RTP thread.
pub type LegId = u64;

pub(super) enum ThreadCmd {
    Bind(Box<NewLeg>),
    /// A leg handed over by another RTP thread.
    Adopt(Box<Bound>),
    Leg(LegId, LegCmd),
    Shutdown,
}

pub(super) struct NewLeg {
    shared: Arc<LegShared>,
    rtp: UdpSocket,
    rtcp: UdpSocket,
    config: Arc<LegConfig>,
}

/// The way into one RTP thread, shared by the legs bound to it.
pub struct ThreadLink {
    index: usize,
    name: String,
    tx: Sender<ThreadCmd>,
    waker: Waker,
    /// Legs bound to the thread. Counted by the main thread as it binds and unbinds.
    legs: AtomicUsize,
}

impl ThreadLink {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> usize {
        self.index
    }

    /// Queues a command and wakes the thread. False if the thread has stopped.
    pub(super) fn send(&self, cmd: ThreadCmd) -> bool {
        if self.tx.send(cmd).is_err() {
            return false;
        }
        let _ = self.waker.wake();
        true
    }

    pub(super) fn unbound(&self) {
        self.legs.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One thread's figures, for rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadLoad {
    pub index: usize,
    pub name: String,
    /// Published load, in permille.
    pub load: u32,
    /// Cycles that started behind since the previous snapshot.
    pub late: u64,
    pub legs: usize,
}

struct RtpThread {
    link: Arc<ThreadLink>,
    stats: Arc<ThreadStats>,
    join: Mutex<Option<JoinHandle<()>>>,
}

/// The RTP threads, and the binding of legs to them. Used by the main thread.
pub struct RtpThreads {
    threads: Vec<RtpThread>,
    next_id: AtomicU64,
    registry: Registry,
    /// Each thread's late-cycle count at the previous `loads`.
    late_seen: Mutex<Vec<u64>>,
}

impl RtpThreads {
    /// Starts `count` RTP threads (at least one), each with a cycle of `cycle`.
    pub fn start(count: usize, cycle: Duration, deps: LegDeps, registry: &Registry) -> io::Result<RtpThreads> {
        let mut set = RtpThreads {
            threads: Vec::new(),
            next_id: AtomicU64::new(1),
            registry: registry.clone(),
            late_seen: Mutex::new(Vec::new()),
        };
        for index in 0..count.max(1) {
            if let Err(e) = set.spawn(index, cycle, &deps) {
                set.shutdown();
                return Err(e);
            }
        }
        Ok(set)
    }

    fn spawn(&mut self, index: usize, cycle: Duration, deps: &LegDeps) -> io::Result<()> {
        let name = format!("rtp-{index}");
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), WAKE)?;
        let (tx, rx) = mpsc::channel();
        let link = Arc::new(ThreadLink { index, name: name.clone(), tx, waker, legs: AtomicUsize::new(0) });
        let stats = self.registry.register(name.clone(), cycle, true);
        let join = std::thread::Builder::new().name(name).spawn({
            let (link, stats, deps) = (link.clone(), stats.clone(), deps.clone());
            move || run(link, poll, rx, deps, stats)
        })?;
        self.threads.push(RtpThread { link, stats, join: Mutex::new(Some(join)) });
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.threads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.threads.is_empty()
    }

    /// RTP thread binding: hands a new leg to the least-loaded thread.
    pub fn bind(&self, sockets: LegSockets, config: Arc<LegConfig>) -> io::Result<LegHandle> {
        let link = &self.threads[self.choose()].link;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let port = sockets.port;
        let shared =
            Arc::new(LegShared::new(id, port, sockets.rtp.try_clone()?, sockets.rtcp.try_clone()?, link.clone()));
        let new = NewLeg {
            shared: shared.clone(),
            rtp: UdpSocket::from_std(sockets.rtp),
            rtcp: UdpSocket::from_std(sockets.rtcp),
            config,
        };
        link.legs.fetch_add(1, Ordering::Relaxed);
        if !link.send(ThreadCmd::Bind(Box::new(new))) {
            link.unbound();
            return Err(io::Error::other(format!("{} has stopped", link.name)));
        }
        tracing::debug!(port, thread = %link.name, "leg bound");
        Ok(LegHandle::new(shared, link.clone()))
    }

    /// Orders a leg moved to thread `to`. The leg's current thread hands it over; the leg
    /// counts change now. False if it is already there (or `to` doesn't exist).
    pub fn move_leg(&self, handle: &mut LegHandle, to: usize) -> bool {
        let Some(target) = self.threads.get(to).map(|t| t.link.clone()) else { return false };
        if handle.thread.index == to {
            return false;
        }
        handle.shared.post(LegCmd::Move(target.clone()));
        handle.thread.unbound();
        target.legs.fetch_add(1, Ordering::Relaxed);
        handle.thread = target;
        true
    }

    /// Every thread's published load, late cycles since the previous call, and legs.
    pub fn loads(&self) -> Vec<ThreadLoad> {
        let mut seen = self.late_seen.lock().unwrap();
        seen.resize(self.threads.len(), 0);
        self.threads
            .iter()
            .zip(seen.iter_mut())
            .map(|(t, seen)| {
                let late = t.stats.late_cycles();
                let since = late.saturating_sub(*seen);
                *seen = late;
                ThreadLoad {
                    index: t.link.index,
                    name: t.link.name.clone(),
                    load: t.stats.load(),
                    late: since,
                    legs: t.link.legs.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    fn choose(&self) -> usize {
        pick(self.threads.iter().map(|t| (t.stats.load(), t.link.legs.load(Ordering::Relaxed))))
    }

    /// Every thread's cycle figures, with the legs bound to each RTP thread.
    pub fn snapshot(&self) -> Vec<ThreadSnapshot> {
        let mut all = self.registry.snapshot();
        for s in &mut all {
            if let Some(t) = self.threads.iter().find(|t| t.link.name == s.thread) {
                s.legs = Some(t.link.legs.load(Ordering::Relaxed));
            }
        }
        all
    }

    /// Stops every RTP thread. Legs still bound end with it.
    pub fn shutdown(&self) {
        for t in &self.threads {
            t.link.send(ThreadCmd::Shutdown);
        }
        for t in &self.threads {
            if let Some(join) = t.join.lock().unwrap().take()
                && join.join().is_err()
            {
                tracing::error!(thread = %t.link.name, "rtp thread panicked");
            }
        }
    }
}

impl Drop for RtpThreads {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The binding rule, over each thread's (published load in permille, legs): lowest load
/// step, then fewest legs, then lowest index.
fn pick(threads: impl Iterator<Item = (u32, usize)>) -> usize {
    threads.enumerate().min_by_key(|&(i, (load, legs))| (load / LOAD_STEP, legs, i)).map_or(0, |(i, _)| i)
}

/// A leg on a thread: its state and sockets. Moves between threads whole.
pub(super) struct Bound {
    leg: Leg,
    rtp: UdpSocket,
    rtcp: UdpSocket,
}

/// The legs bound to one thread. A leg's slot number gives its sockets' poll tokens.
#[derive(Default)]
struct Slots {
    slots: Vec<Option<Bound>>,
    free: Vec<usize>,
    by_id: HashMap<LegId, usize>,
}

fn tokens(slot: usize) -> (Token, Token) {
    (Token(1 + 2 * slot), Token(2 + 2 * slot))
}

impl Slots {
    fn bind(&mut self, new: NewLeg, registry: &mio::Registry, deps: &LegDeps) {
        let NewLeg { shared, mut rtp, mut rtcp, config } = new;
        let slot = self.free.pop().unwrap_or_else(|| {
            self.slots.push(None);
            self.slots.len() - 1
        });
        let (rtp_token, rtcp_token) = tokens(slot);
        if let Err(e) = registry
            .register(&mut rtp, rtp_token, Interest::READABLE)
            .and_then(|_| registry.register(&mut rtcp, rtcp_token, Interest::READABLE))
        {
            tracing::error!(port = shared.port, error = %e, "rtp thread can't watch the leg's sockets");
            let _ = registry.deregister(&mut rtp);
            self.free.push(slot);
            return;
        }
        self.by_id.insert(shared.id, slot);
        self.slots[slot] = Some(Bound { leg: Leg::new(shared, config, deps.clone()), rtp, rtcp });
    }

    /// Takes in a leg handed over by another thread.
    fn adopt(&mut self, mut bound: Bound, registry: &mio::Registry) {
        let slot = self.free.pop().unwrap_or_else(|| {
            self.slots.push(None);
            self.slots.len() - 1
        });
        let (rtp_token, rtcp_token) = tokens(slot);
        let id = bound.leg.shared().id;
        if let Err(e) = registry
            .register(&mut bound.rtp, rtp_token, Interest::READABLE)
            .and_then(|_| registry.register(&mut bound.rtcp, rtcp_token, Interest::READABLE))
        {
            tracing::error!(port = bound.leg.shared().port, error = %e, "rtp thread can't watch a moved leg's sockets");
            let _ = registry.deregister(&mut bound.rtp);
            self.free.push(slot);
            return;
        }
        self.by_id.insert(id, slot);
        self.slots[slot] = Some(bound);
    }

    /// Takes a leg out of this thread: out of the poll and the slots.
    fn take(&mut self, id: LegId, registry: &mio::Registry) -> Option<Bound> {
        let slot = self.by_id.remove(&id)?;
        let mut bound = self.slots[slot].take()?;
        self.free.push(slot);
        let _ = registry.deregister(&mut bound.rtp);
        let _ = registry.deregister(&mut bound.rtcp);
        Some(bound)
    }

    /// Removes a leg, closing its sockets here. Returns its recording, still open.
    fn unbind(&mut self, id: LegId, registry: &mio::Registry) -> Option<Recording> {
        self.take(id, registry)?.leg.recording.take()
    }

    fn get_mut(&mut self, id: LegId) -> Option<&mut Bound> {
        let slot = *self.by_id.get(&id)?;
        self.slots[slot].as_mut()
    }

    /// The leg a socket event is for, and whether it is the RTCP socket.
    fn by_token(&mut self, token: Token) -> Option<(&mut Bound, bool)> {
        let n = token.0.checked_sub(1)?;
        self.slots.get_mut(n / 2)?.as_mut().map(|b| (b, n % 2 == 1))
    }

    fn legs_mut(&mut self) -> impl Iterator<Item = &mut Leg> {
        self.slots.iter_mut().flatten().map(|b| &mut b.leg)
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }
}

fn run(link: Arc<ThreadLink>, mut poll: Poll, inbox: Receiver<ThreadCmd>, deps: LegDeps, stats: Arc<ThreadStats>) {
    deps.sched.apply_or_warn("rtp");
    let mut meter = CycleMeter::new(stats, Instant::now(), MAX_CATCH_UP);
    let frames_per_cycle = (meter.cycle().as_millis() / FRAME.as_millis()).max(1) as u32;
    let mut legs = Slots::default();
    // Commands set aside while a leg is handed over, run before the inbox's next ones.
    let mut stash: VecDeque<ThreadCmd> = VecDeque::new();
    let mut events = Events::with_capacity(256);
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let now = Instant::now();
        meter.sleep(now);
        if let Err(e) = poll.poll(&mut events, Some(meter.deadline().saturating_duration_since(now)))
            && e.kind() != io::ErrorKind::Interrupted
        {
            tracing::error!(thread = %link.name, error = %e, "poll failed");
            std::thread::sleep(FRAME);
        }
        meter.wake(Instant::now());

        for event in events.iter() {
            let Some((bound, is_rtcp)) = legs.by_token(event.token()) else { continue };
            let Bound { leg, rtp, rtcp } = bound;
            if is_rtcp {
                drain(rtcp, &mut buf, |packet, src| leg.on_rtcp(packet, src));
            } else {
                drain(rtp, &mut buf, |packet, src| leg.on_rtp(packet, src));
            }
        }

        loop {
            let cmd = match stash.pop_front() {
                Some(cmd) => cmd,
                None => match inbox.try_recv() {
                    Ok(cmd) => cmd,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                },
            };
            match cmd {
                ThreadCmd::Bind(new) => legs.bind(*new, poll.registry(), &deps),
                ThreadCmd::Adopt(bound) => legs.adopt(*bound, poll.registry()),
                ThreadCmd::Leg(id, LegCmd::Unbind(reply)) => {
                    let _ = reply.send(legs.unbind(id, poll.registry()));
                }
                ThreadCmd::Leg(id, LegCmd::Move(to)) => {
                    if to.index != link.index
                        && let Some(bound) = legs.take(id, poll.registry())
                    {
                        hand_over(bound, &to, &inbox, &mut stash);
                    }
                }
                ThreadCmd::Leg(id, cmd) => {
                    // A leg already unbound may still get commands from its peers.
                    if let Some(bound) = legs.get_mut(id) {
                        bound.leg.command(cmd);
                    }
                }
                ThreadCmd::Shutdown => return,
            }
        }

        let now = Instant::now();
        if !meter.is_due(now) {
            continue;
        }
        let timing = meter.start_cycle(now);
        if timing.skipped > 0 {
            let missed = timing.skipped * frames_per_cycle;
            for leg in legs.legs_mut() {
                leg.fill_blanks(missed);
            }
            tracing::warn!(
                thread = %link.name,
                missed_frames = missed,
                legs = legs.len(),
                "rtp thread fell too far behind; the missed frames are filled with silence"
            );
        }
        for _ in 0..frames_per_cycle {
            for leg in legs.legs_mut() {
                leg.receive(now);
            }
            for leg in legs.legs_mut() {
                leg.send(now);
            }
        }
        if let Some(load) = meter.load_changed() {
            tracing::info!(thread = %link.name, load_pct = load as f64 / 10.0, legs = legs.len(), "rtp thread load changed");
        }
    }
}

/// Hands a leg to another thread (see the module docs). Holding the leg's route lock, so
/// nobody can post to it meanwhile: the leg goes first, then its commands still queued
/// here, in order; then the route points at the new thread. Commands for other legs found
/// on the way are set aside in `stash`, in order.
fn hand_over(bound: Bound, to: &Arc<ThreadLink>, inbox: &Receiver<ThreadCmd>, stash: &mut VecDeque<ThreadCmd>) {
    let shared = bound.leg.shared().clone();
    let mut route = shared.route.lock().unwrap();
    if !to.send(ThreadCmd::Adopt(Box::new(bound))) {
        // The target has stopped (shutdown): the leg ends with it.
        tracing::warn!(port = shared.port, thread = %to.name, "rtp thread gone; moved leg dropped");
    }
    while let Ok(cmd) = inbox.try_recv() {
        match cmd {
            ThreadCmd::Leg(id, cmd) if id == shared.id => {
                to.send(ThreadCmd::Leg(id, cmd));
            }
            other => stash.push_back(other),
        }
    }
    *route = to.clone();
    tracing::debug!(port = shared.port, from = %route.name, "leg handed over");
}

/// Reads everything waiting on a socket (mio wakes us once per batch).
fn drain(socket: &UdpSocket, buf: &mut [u8], mut on_packet: impl FnMut(&mut [u8], SocketAddr)) {
    loop {
        match socket.recv_from(buf) {
            Ok((len, src)) => on_packet(&mut buf[..len], src),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
            // An ICMP port-unreachable from an earlier send surfaces here on Linux;
            // it says nothing about this socket.
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => continue,
            Err(e) => {
                tracing::debug!(error = %e, "udp receive failed");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_prefers_lower_load_then_fewer_legs() {
        assert_eq!(pick([(0, 3), (0, 1), (0, 2)].into_iter()), 1, "same load: fewest legs");
        assert_eq!(pick([(120, 0), (20, 5)].into_iter()), 1, "lower load step wins over fewer legs");
        assert_eq!(pick([(30, 2), (10, 2)].into_iter()), 0, "within one step loads are equal: lowest index");
        assert_eq!(pick([(10, 2), (40, 1)].into_iter()), 1, "within one step: fewest legs");
    }

    #[test]
    fn slot_tokens_round_trip_and_skip_the_waker() {
        for slot in [0, 1, 7] {
            let (rtp, rtcp) = tokens(slot);
            assert_ne!(rtp, WAKE);
            let n = rtp.0 - 1;
            assert_eq!((n / 2, n % 2), (slot, 0));
            let n = rtcp.0 - 1;
            assert_eq!((n / 2, n % 2), (slot, 1));
        }
    }
}
