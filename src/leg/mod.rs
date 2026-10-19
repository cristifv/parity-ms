//! A leg: one endpoint's media, a call leg or a conference participant. Each leg is bound
//! to one of the RTP threads ([`RtpThreads`]). That thread owns the leg's sockets, jitter
//! buffer, codecs, DTMF state and recording, and runs its frames in its cycle. Rebalancing
//! can move a leg to another RTP thread, whole: its state and sockets are handed over.
//!
//! **Shared state and data races.** Safe Rust rules out data races at compile time: what
//! one thread hands another must be moved, atomic or locked. The design questions are
//! which thread owns what, and in what order things happen:
//! - **Owned by the leg's RTP thread, never shared:** jitter buffer, transcoding
//!   source and destination, DTMF state, recording. It's all in `state::Leg`, which only the
//!   bound thread can reach. When the leg moves, the old thread hands the whole `Leg` to
//!   the new one through its inbox, so there is one owner at every moment.
//! - **[`LegShared`], seen from other threads:** the main thread and the leg's peers,
//!   which may be bound to other RTP threads and run at the same moment:
//!   - commands go through the inbox of the leg's current RTP thread, found through its
//!     route. Commands to a leg arrive in the order they were sent, and after its binding,
//!     even across a move: the old thread swaps the route while holding its lock, after
//!     handing the leg over and forwarding the leg's commands still queued there;
//!   - the decoded audio the leg publishes is in a [`FrameRing`]: one writer (the leg's
//!     thread), any number of readers, each with its own cursor, under a short lock;
//!   - the remote addresses are under a lock. They are latched by the leg's thread and read
//!     by peers that forward packets through the leg's sockets. A UDP send is atomic per
//!     datagram, so two threads may send on one socket;
//!   - counters are atomics, and the stats published for queries are under a lock.
//! - **Configuration is replaced, never edited:** the main thread builds a new
//!   [`LegConfig`] and sends it; the leg's thread swaps it in between packets.
//! - **Ordering between threads is not guaranteed.** A peer's newest frame may be from this
//!   cycle or the last, and a packet forwarded before the peer has latched its remote is
//!   dropped, as in any relay. Nothing waits on another RTP thread: they only post to
//!   inboxes, so one stalled thread can't stall the others. Only the main thread waits,
//!   when it unbinds a leg, and then with a timeout.

mod inbound;
mod outbound;
mod rtp_thread;
mod state;
pub mod stream;

pub use rtp_thread::{FRAME, LegId, RtpThreads, ThreadLink, ThreadLoad};
use rtp_thread::ThreadCmd;

use std::net::SocketAddr;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};

use crate::dtmf::Digit;
use crate::jitter::{JitterConfig, JitterStats};
use crate::media::mixer::FrameRing;
use crate::media::prompt::Prompt;
use crate::webrtc;
use srtp::openssl::OutboundSession;
use crate::media::passthrough::Passthrough;
use crate::media::{LegCodec, Side};
use crate::recording::{Recording, RecordingStatus};
use crate::sched::Policy;
use stream::{Activity, Stream, StreamStats};

/// Where a leg is: the call or conference it belongs to, and its name there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place {
    Call { call_id: String, side: Side },
    Conference { conference_id: String, participant_id: String },
}

impl Place {
    /// The call or conference id; events are keyed by it.
    pub fn key(&self) -> &str {
        match self {
            Place::Call { call_id, .. } => call_id,
            Place::Conference { conference_id, .. } => conference_id,
        }
    }

    /// The fields that identify the leg in events and responses.
    pub fn fields(&self) -> Value {
        match self {
            Place::Call { call_id, side } => json!({ "call_id": call_id, "leg": side }),
            Place::Conference { conference_id, participant_id } => {
                json!({ "conference_id": conference_id, "participant_id": participant_id })
            }
        }
    }

    /// `<call>/a` or `<conference>/<participant>`, for recording headers and timelines.
    pub fn key_label(&self) -> String {
        match self {
            Place::Call { call_id, side } => format!("{call_id}/{}", if *side == Side::A { "a" } else { "b" }),
            Place::Conference { conference_id, participant_id } => format!("{conference_id}/{participant_id}"),
        }
    }

    /// A readable name for files: `<call>-a`, `<conference>-<participant>`.
    pub fn label(&self) -> String {
        match self {
            Place::Call { call_id, side } => format!("{call_id}-{}", if *side == Side::A { "a" } else { "b" }),
            Place::Conference { conference_id, participant_id } => format!("{conference_id}-{participant_id}"),
        }
    }
}

/// Something a leg's RTP thread reports, for the events topic.
#[derive(Debug, Clone)]
pub enum LegEvent {
    /// A digit the leg sent.
    Dtmf(Digit),
    /// A file played into the leg has finished.
    PlayFinished(PlayFinished),
}

#[derive(Debug, Clone, Serialize)]
pub struct PlayFinished {
    pub file: String,
    pub reason: PlayEnd,
    pub played_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayEnd {
    /// Played to the end.
    Completed,
    /// `stop_play`.
    Stopped,
    /// Another `play` took its place.
    Replaced,
}

/// Receives what the legs report. Called on their RTP threads.
pub type LegListener = Arc<dyn Fn(&Place, LegEvent) + Send + Sync>;

/// A digit to send with `send_dtmf`.
#[derive(Debug, Clone, Copy)]
pub struct Injected {
    pub event: u8,
    pub duration: Duration,
    pub pause: Duration,
}

/// DTMF from the other leg of a call, for this leg's generated stream to reproduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerDtmf {
    Start(u8),
    End,
}

/// What a leg's thread needs to know about its place in a call or conference. Replaced as a
/// whole whenever something changes.
pub struct LegConfig {
    pub place: Place,
    /// What the leg negotiated. `None` before negotiation (static payload types are assumed).
    pub codec: Option<LegCodec>,
    /// The other legs of the call or conference, whose audio this leg hears.
    pub peers: Vec<Arc<LegShared>>,
    /// Two-party passthrough: this leg's packets go straight out to this peer.
    pub forward: Option<Forward>,
    /// This leg's stream is made here, from the peers' decoded audio (mixing, transcoding,
    /// DTMF). False when a peer forwards its packets to this leg instead.
    pub generate: bool,
    /// Calls: the peer whose stream is made here learns about this leg's DTMF.
    pub dtmf_peer: Option<DtmfPeer>,
    /// Soft mute: the leg's audio is replaced by silence, frame by frame. Peers hear
    /// silence, passthrough packets carry silence, and the leg's recording has silence on
    /// its own channel. It still hears the others, and its digits are still detected and
    /// reported, but not passed on.
    pub muted: bool,
}

pub struct Forward {
    pub to: Arc<LegShared>,
    pub passthrough: Passthrough,
}

pub struct DtmfPeer {
    pub leg: Arc<LegShared>,
    /// It negotiated RFC 4733 (otherwise it plays tones, and tones in the audio need no help).
    pub takes_events: bool,
}

pub enum LegCmd {
    Configure(Arc<LegConfig>),
    SetRemote(SocketAddr),
    StartRecording(Recording),
    /// Hands the recording back (to be closed), if there is one.
    StopRecording(Sender<Option<Recording>>),
    SendDtmf(Vec<Injected>),
    PeerDtmf(PeerDtmf),
    /// Plays a file into the leg, mixed into what it hears (replacing a file still
    /// playing).
    Play(Arc<Prompt>),
    StopPlay,
    /// Removes the leg from its RTP thread, which hands back its recording (still open).
    Unbind(Sender<Option<Recording>>),
    /// Rebalancing: the leg's thread hands it over, whole, to this RTP thread.
    Move(Arc<ThreadLink>),
    /// The leg is WebRTC: set up (or update) its ICE-lite and DTLS-SRTP transport.
    WebRtc(Box<webrtc::Setup>),
}

/// How packets sent to a leg are protected.
enum Protection {
    Plain,
    /// WebRTC, before DTLS has keyed SRTP: nothing goes out.
    Pending,
    Srtp(Box<OutboundSession>),
}

/// A WebRTC leg's SRTP, as `query` shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SrtpState {
    /// Waiting for ICE and DTLS.
    Pending,
    Active,
}

/// What other threads see of a leg.
pub struct LegShared {
    pub id: LegId,
    pub port: u16,
    pub rtp: Stream,
    pub rtcp: Stream,
    pub activity: Activity,
    /// The decoded audio this leg sends, for its peers to mix.
    pub frames: FrameRing,
    /// The RTP thread the leg is on, where its commands go. Only the thread handing the
    /// leg over changes it, while holding the lock (see the module docs).
    route: Mutex<Arc<ThreadLink>>,
    /// Every send to the leg goes through this, from whichever thread: plain, or SRTP with
    /// the leg's outbound session (WebRTC, where RTCP is muxed onto the RTP port).
    protection: Mutex<Protection>,
    published: Mutex<Published>,
}

#[derive(Default, Clone)]
struct Published {
    jitter: JitterStats,
    recording: Option<RecordingStatus>,
    playing: Option<PlayStatus>,
}

/// A file being played into a leg.
#[derive(Debug, Clone, Serialize)]
pub struct PlayStatus {
    pub file: String,
    pub position_ms: u64,
    pub duration_ms: u64,
}

impl LegShared {
    /// `rtp` and `rtcp` are handles on the leg's sockets, for sending.
    fn new(id: LegId, port: u16, rtp: std::net::UdpSocket, rtcp: std::net::UdpSocket, thread: Arc<ThreadLink>) -> LegShared {
        LegShared {
            id,
            port,
            rtp: Stream::new(rtp, None),
            rtcp: Stream::new(rtcp, None),
            activity: Activity::new(),
            frames: FrameRing::default(),
            route: Mutex::new(thread),
            protection: Mutex::new(Protection::Plain),
            published: Mutex::new(Published::default()),
        }
    }

    /// Sends the leg a command through its RTP thread's inbox, and wakes the thread.
    pub fn post(&self, cmd: LegCmd) {
        self.route.lock().unwrap().send(ThreadCmd::Leg(self.id, cmd));
    }

    /// Sends RTP to the leg: as it is, or encrypted for a WebRTC leg.
    pub fn send_rtp(&self, packet: &[u8]) -> bool {
        match self.protect(packet, false) {
            Ok(None) => self.rtp.send(packet),
            Ok(Some(srtp)) => self.rtp.send(&srtp),
            Err(()) => {
                self.rtp.drop_packet();
                false
            }
        }
    }

    /// Sends RTCP to the leg: to its RTCP port, or encrypted and muxed onto its RTP port
    /// for a WebRTC leg.
    pub fn send_rtcp(&self, packet: &[u8]) -> bool {
        match self.protect(packet, true) {
            Ok(None) => self.rtcp.send(packet),
            Ok(Some(srtcp)) => self.rtp.send_counted(&srtcp, &self.rtcp.sent),
            Err(()) => {
                self.rtcp.drop_packet();
                false
            }
        }
    }

    /// Sends a STUN or DTLS datagram from the leg's RTP port.
    pub fn send_raw(&self, packet: &[u8], to: SocketAddr) -> bool {
        self.rtp.send_to(packet, to)
    }

    /// `Ok(None)`: send as it is. `Ok(Some)`: send this protected copy. `Err`: not now.
    fn protect(&self, packet: &[u8], rtcp: bool) -> Result<Option<Vec<u8>>, ()> {
        let mut protection = self.protection.lock().unwrap();
        match &mut *protection {
            Protection::Plain => Ok(None),
            Protection::Pending => Err(()),
            Protection::Srtp(session) => {
                let mut buf = packet.to_vec();
                let done = if rtcp { session.protect_rtcp(&mut buf) } else { session.protect(&mut buf) };
                done.map(|()| Some(buf)).map_err(|_| ())
            }
        }
    }

    /// The leg is WebRTC: nothing is sent to it until DTLS has keyed SRTP.
    pub fn set_srtp_pending(&self) {
        *self.protection.lock().unwrap() = Protection::Pending;
    }

    fn set_srtp(&self, session: OutboundSession) {
        *self.protection.lock().unwrap() = Protection::Srtp(Box::new(session));
    }

    pub fn srtp(&self) -> Option<SrtpState> {
        match &*self.protection.lock().unwrap() {
            Protection::Plain => None,
            Protection::Pending => Some(SrtpState::Pending),
            Protection::Srtp(_) => Some(SrtpState::Active),
        }
    }

    /// The name of the RTP thread the leg is on.
    pub fn rtp_thread(&self) -> String {
        self.route.lock().unwrap().name().to_string()
    }

    pub fn set_remote(&self, addr: SocketAddr) {
        self.post(LegCmd::SetRemote(addr));
    }

    pub fn stats(&self) -> LegStats {
        let published = self.published.lock().unwrap().clone();
        LegStats {
            rtp_thread: self.rtp_thread(),
            srtp: self.srtp(),
            rtp_port: self.port,
            rtcp_port: self.port + 1,
            rtp: self.rtp.stats(),
            rtcp: self.rtcp.stats(),
            jitter: published.jitter,
            recording: published.recording,
            playing: published.playing,
            muted: false,
        }
    }

    pub fn recording(&self) -> Option<RecordingStatus> {
        self.published.lock().unwrap().recording.clone()
    }

    /// The file being played into the leg, as of its last frame.
    pub fn playing(&self) -> Option<PlayStatus> {
        self.published.lock().unwrap().playing.clone()
    }

    fn publish(&self, jitter: JitterStats, recording: Option<RecordingStatus>, playing: Option<PlayStatus>) {
        *self.published.lock().unwrap() = Published { jitter, recording, playing };
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LegStats {
    pub rtp_thread: String,
    /// WebRTC legs: whether SRTP is keyed yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub srtp: Option<SrtpState>,
    pub rtp_port: u16,
    pub rtcp_port: u16,
    pub rtp: StreamStats,
    pub rtcp: StreamStats,
    pub jitter: JitterStats,
    pub recording: Option<RecordingStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playing: Option<PlayStatus>,
    /// Set by the engine, which keeps the mute state.
    pub muted: bool,
}

/// Settings every RTP thread gets.
#[derive(Clone)]
pub struct LegDeps {
    pub jitter: JitterConfig,
    pub listener: LegListener,
    /// Scheduling for the RTP threads.
    pub sched: Policy,
}

/// How long the main thread waits for an RTP thread to give a leg back.
const UNBIND_TIMEOUT: Duration = Duration::from_secs(2);

/// The main thread's hold on a bound leg. Dropping it unbinds the leg.
pub struct LegHandle {
    pub shared: Arc<LegShared>,
    /// The RTP thread the leg is counted on. It changes when a move is ordered, a little
    /// before the leg's route does.
    thread: Arc<ThreadLink>,
    bound: bool,
}

impl LegHandle {
    fn new(shared: Arc<LegShared>, thread: Arc<ThreadLink>) -> LegHandle {
        LegHandle { shared, thread, bound: true }
    }

    /// The RTP thread the leg is counted on (where it is, or is being moved to).
    pub fn thread_index(&self) -> usize {
        self.thread.index()
    }

    /// Unbinds the leg from its RTP thread and waits for it. Returns the leg's recording,
    /// still open.
    pub fn stop(mut self) -> Option<Recording> {
        self.unbind()
    }

    fn unbind(&mut self) -> Option<Recording> {
        if !std::mem::replace(&mut self.bound, false) {
            return None;
        }
        let (reply, rx) = mpsc::channel();
        self.shared.post(LegCmd::Unbind(reply));
        self.thread.unbound();
        match rx.recv_timeout(UNBIND_TIMEOUT) {
            Ok(recording) => recording,
            // The thread has stopped, and the leg with it.
            Err(RecvTimeoutError::Disconnected) => None,
            Err(RecvTimeoutError::Timeout) => {
                tracing::error!(port = self.shared.port, thread = %self.shared.rtp_thread(), "rtp thread did not release the leg");
                None
            }
        }
    }
}

impl Drop for LegHandle {
    fn drop(&mut self) {
        if let Some(recording) = self.unbind() {
            drop(recording.close());
        }
    }
}
