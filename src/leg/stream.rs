//! One UDP stream of a leg (its RTP or its RTCP): the socket packets are sent from, where
//! they go, and traffic counters. Shared by the leg's thread and its peers' threads, which
//! forward packets through it in passthrough.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Larger datagrams are dropped. RTP stays well under the path MTU, so anything this big is
/// not media.
pub const MAX_DATAGRAM: usize = 4096;

/// Where a stream's media is sent. It starts from the address the control side signalled
/// (taken from SDP, and possibly wrong behind NAT), then latches onto the source of the first
/// packet that arrives. After that, packets from any other source are dropped.
#[derive(Default)]
pub struct Remote {
    state: Mutex<RemoteState>,
}

#[derive(Default, Clone, Copy)]
struct RemoteState {
    addr: Option<SocketAddr>,
    latched: bool,
}

impl Remote {
    pub fn new(hint: Option<SocketAddr>) -> Remote {
        Remote { state: Mutex::new(RemoteState { addr: hint, latched: false }) }
    }

    /// Replaces the signalled address. Ignored once the stream has latched.
    pub fn set_hint(&self, addr: SocketAddr) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.latched {
            return false;
        }
        s.addr = Some(addr);
        true
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.state.lock().unwrap().addr
    }

    pub fn latched(&self) -> bool {
        self.state.lock().unwrap().latched
    }

    /// ICE: media goes to `addr` from now on (an address the browser checked, or the one it
    /// nominated). Unlike latching, a later nomination moves it: ICE checks are
    /// authenticated, so they may.
    pub fn nominate(&self, addr: SocketAddr) {
        let mut s = self.state.lock().unwrap();
        s.addr = Some(addr);
        s.latched = true;
    }

    /// The first packet latches the remote; later packets must come from the same address.
    pub fn admit(&self, src: SocketAddr) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.latched {
            return s.addr == Some(src);
        }
        s.addr = Some(src);
        s.latched = true;
        true
    }
}

#[derive(Default)]
pub struct Counter {
    packets: AtomicU64,
    bytes: AtomicU64,
}

impl Counter {
    pub fn add(&self, bytes: usize) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot { packets: self.packets.load(Ordering::Relaxed), bytes: self.bytes.load(Ordering::Relaxed) }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CounterSnapshot {
    pub packets: u64,
    pub bytes: u64,
}

pub struct Stream {
    socket: UdpSocket,
    pub remote: Remote,
    /// Accepted from the remote.
    pub received: Counter,
    /// Sent to the remote.
    pub sent: Counter,
    /// Received but not delivered: wrong source, oversized, or nowhere to send it yet.
    pub dropped: AtomicU64,
}

impl Stream {
    /// `socket` is a handle on the stream's socket (a clone of the one its thread reads).
    pub fn new(socket: UdpSocket, hint: Option<SocketAddr>) -> Stream {
        Stream {
            socket,
            remote: Remote::new(hint),
            received: Counter::default(),
            sent: Counter::default(),
            dropped: AtomicU64::new(0),
        }
    }

    /// Sends to the remote. `false` if there's no remote yet or the send failed.
    pub fn send(&self, packet: &[u8]) -> bool {
        self.send_counted(packet, &self.sent)
    }

    /// Sends to the remote, counting in `counter` (muxed RTCP leaves by the RTP socket but
    /// counts as RTCP).
    pub fn send_counted(&self, packet: &[u8], counter: &Counter) -> bool {
        let Some(dst) = self.remote.addr() else { return false };
        let sent = self.send_to(packet, dst);
        if sent {
            counter.add(packet.len());
        }
        sent
    }

    /// Sends to `dst`, outside the media counters (STUN and DTLS).
    pub fn send_to(&self, packet: &[u8], dst: SocketAddr) -> bool {
        match self.socket.send_to(packet, dst) {
            Ok(_) => true,
            Err(e) => {
                if e.kind() != io::ErrorKind::WouldBlock {
                    tracing::debug!(error = %e, %dst, "udp send failed");
                }
                false
            }
        }
    }

    pub fn drop_packet(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> StreamStats {
        StreamStats {
            remote: self.remote.addr(),
            latched: self.remote.latched(),
            received: self.received.snapshot(),
            sent: self.sent.snapshot(),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamStats {
    pub remote: Option<SocketAddr>,
    pub latched: bool,
    pub received: CounterSnapshot,
    pub sent: CounterSnapshot,
    pub dropped: u64,
}

/// Last time any packet arrived on a leg, in milliseconds since the leg started.
pub struct Activity {
    started: Instant,
    last_ms: AtomicU64,
}

impl Activity {
    pub fn new() -> Activity {
        Activity { started: Instant::now(), last_ms: AtomicU64::new(0) }
    }

    pub fn touch(&self) {
        self.last_ms.store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub fn age(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn idle(&self) -> Duration {
        self.age().saturating_sub(Duration::from_millis(self.last_ms.load(Ordering::Relaxed)))
    }
}

impl Default for Activity {
    fn default() -> Self {
        Activity::new()
    }
}
