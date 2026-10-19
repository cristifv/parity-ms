//! Calls and conferences: the structures the commands act on. They live on the main thread.
//! Each leg's media runs on the RTP thread it is bound to (see [`crate::leg`]); the engine
//! binds, configures and unbinds legs.
//!
//! - **Call:** two legs, A and B, negotiated by forwarding SDP between them. Either leg may
//!   offer (at the start, or later with a re-INVITE); the other answers. Media passes
//!   through untouched when the legs agree on a codec, even one this server can't decode,
//!   and is transcoded otherwise.
//! - **Conference:** any number of participants, each negotiating its own SDP with the
//!   server, and re-negotiating it with `reoffer`. Everyone hears the mix of the others. A participant can be alone, conferences
//!   can be merged, and a call can be moved into a conference.
//! - **WebRTC legs:** a leg whose SDP is `UDP/TLS/RTP/SAVPF`, or that `offer` is told is
//!   WebRTC, gets ICE-lite and DTLS-SRTP on its port (see [`crate::webrtc`]). It works with
//!   any other leg, plain or WebRTC: packets are decrypted on the way in and encrypted on
//!   the way out, and passthrough and transcoding work as usual in between.
//! - **Recordings** belong to legs and go with them wherever they move.
//! - **Mute and supervisor modes:** any leg can be soft-muted (its audio replaced by
//!   silence). A supervisor joins a conference muted (listen), then can whisper (heard by
//!   one participant only) or barge in (heard by everyone).
//! - **Playing files:** a file from the prompt directory is played into a leg, mixed into
//!   what it hears and recorded with it.
//! - **Rebalancing:** whole calls and conferences are moved off hot RTP threads (see
//!   [`crate::rebalance`]).

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

pub use crate::clock::unix_ms;
use crate::codec::Codec;
use crate::dtmf::{Source, char_to_event};
use crate::cycle::ThreadSnapshot;
use crate::leg::{
    DtmfPeer, Forward, Injected, LegCmd, LegConfig, LegHandle, LegStats, Place, PlayStatus, RtpThreads, ThreadLoad,
};
use crate::media::passthrough::Passthrough;
use crate::media::prompt::{self, PromptError};
use crate::media::{LegCodec, Mode, Side};
use crate::ports::PortPool;
use crate::rebalance::{self, RebalanceConfig, Reason};
use crate::recording::{IoHandle, RecordingStatus};
use crate::sdp::{self, Description, LocalWebRtc, Payload, SdpError, Setup};
use crate::webrtc::{self, dtls::Identity, dtls::Role, ice::Credentials};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0} not found")]
    NotFound(String),
    #[error("leg limit reached ({0})")]
    Full(usize),
    #[error("no free media ports")]
    NoPorts,
    #[error(transparent)]
    Sdp(#[from] SdpError),
    #[error("no usable codec: {0}")]
    UnsupportedCodec(String),
    #[error("{0}")]
    InvalidState(String),
    #[error("recording failed: {0}")]
    Recording(io::Error),
    #[error("leg setup failed: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Prompt(#[from] PromptError),
}

/// A conference participant's supervisor mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantMode {
    /// Hears everyone; nobody hears them (muted).
    Listen,
    /// Hears everyone; only one participant (the agent) hears them.
    Whisper,
    /// Hears everyone, and everyone hears them.
    Barge,
}

/// One leg as the engine sees it.
struct Slot {
    handle: LegHandle,
    codec: Option<LegCodec>,
    /// Digits or audio were sent to this leg, so its stream is made here from then on.
    injected: bool,
    /// Soft mute: its audio is replaced by silence for everyone.
    muted: bool,
    /// Whisper: only this participant hears it.
    whisper_to: Option<String>,
    /// The `o=` session id and version of the last SDP we sent a conference participant.
    sdp_origin: (u64, u64),
    /// WebRTC legs: our side of ICE and DTLS, and the peer's once its SDP is known.
    webrtc: Option<WebRtcLeg>,
    recording: bool,
    created_at_ms: u64,
}

/// A WebRTC leg's side of the SDP.
struct WebRtcLeg {
    local: Credentials,
    mid: String,
    remote: Option<webrtc::Remote>,
}

/// SDP state of a call: the latest offer, and which leg made it.
struct Negotiation {
    offerer: Side,
    /// The latest offer as parsed: its other `m=` lines are rejected in the answer.
    offer_sdp: Option<sdp::Audio>,
    /// Audio formats offered, in the offerer's order of preference. Empty before any offer.
    offer: Vec<Payload>,
    events: Vec<Payload>,
    sdp_session_id: u64,
    sdp_version: u64,
}

impl Negotiation {
    fn next_version(&mut self) -> u64 {
        self.sdp_version += 1;
        self.sdp_version
    }
}

struct Call {
    a: Slot,
    b: Slot,
    negotiation: Negotiation,
    created_at_ms: u64,
    /// When it last moved to another RTP thread.
    moved_at: Option<Instant>,
}

impl Call {
    fn slot(&self, side: Side) -> &Slot {
        match side {
            Side::A => &self.a,
            Side::B => &self.b,
        }
    }

    fn slot_mut(&mut self, side: Side) -> &mut Slot {
        match side {
            Side::A => &mut self.a,
            Side::B => &mut self.b,
        }
    }

    fn mode(&self) -> Mode {
        if can_forward(&self.a, &self.b) && can_forward(&self.b, &self.a) { Mode::Passthrough } else { Mode::Transcode }
    }
}

struct Conference {
    participants: BTreeMap<String, Slot>,
    created_at_ms: u64,
    /// When it last moved to another RTP thread.
    moved_at: Option<Instant>,
}

impl Conference {
    fn new() -> Conference {
        Conference { participants: BTreeMap::new(), created_at_ms: unix_ms(), moved_at: None }
    }
}

/// A call or a conference: what moves between RTP threads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Group {
    Call(String),
    Conference(String),
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Group::Call(id) => write!(f, "call {id}"),
            Group::Conference(id) => write!(f, "conference {id}"),
        }
    }
}

/// A group moved by rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rebalanced {
    pub group: Group,
    /// Legs that changed thread.
    pub legs: usize,
    pub from: usize,
    pub to: usize,
    pub reason: Reason,
}

#[derive(Default)]
struct State {
    calls: HashMap<String, Call>,
    conferences: HashMap<String, Conference>,
    legs: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LegPorts {
    pub rtp_port: u16,
    pub rtcp_port: u16,
}

/// A call's addresses: what to put in the SDP sent to each leg.
#[derive(Debug, Clone, Serialize)]
pub struct CallInfo {
    pub call_id: String,
    pub ip: IpAddr,
    pub a: LegPorts,
    pub b: LegPorts,
    /// False when the call already existed.
    pub created: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OfferResult {
    /// The leg the offer is for: the other one.
    pub to: Side,
    /// The offer to send to that leg.
    pub sdp: String,
    pub call: CallInfo,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnswerResult {
    /// The leg the answer is for: the one that offered.
    pub to: Side,
    /// The answer to send to that leg.
    pub sdp: String,
    pub mode: Mode,
    pub codecs: Codecs,
}

#[derive(Debug, Clone, Serialize)]
pub struct Codecs {
    pub a: Option<LegCodec>,
    pub b: Option<LegCodec>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CallStats {
    pub call_id: String,
    pub created_at_ms: u64,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub mode: Mode,
    pub codecs: Codecs,
    pub a: LegStats,
    pub b: LegStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct JoinResult {
    /// The answer to send to the participant.
    pub sdp: String,
    pub conference_id: String,
    pub participant_id: String,
    pub ip: IpAddr,
    pub rtp_port: u16,
    pub rtcp_port: u16,
    pub codec: LegCodec,
    /// The conference was created by this join.
    pub conference_created: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReofferResult {
    /// The answer to send to the participant.
    pub sdp: String,
    pub codec: LegCodec,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantStats {
    pub participant_id: String,
    pub joined_at_ms: u64,
    pub idle_ms: u64,
    pub codec: Option<LegCodec>,
    /// Whispering: only this participant hears it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whisper_to: Option<String>,
    #[serde(flatten)]
    pub leg: LegStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConferenceStats {
    pub conference_id: String,
    pub created_at_ms: u64,
    pub participants: Vec<ParticipantStats>,
}

/// A leg that was stopped, with its final stats and its finished recording.
#[derive(Debug, Clone)]
pub struct Ended {
    pub place: Place,
    pub stats: ParticipantStats,
    pub recording: Option<RecordingStatus>,
}

/// What a `leave` did.
pub struct Left {
    pub ended: Ended,
    /// The conference is gone: that was its last participant.
    pub conference_ended: bool,
}

pub struct Engine {
    state: Mutex<State>,
    pool: PortPool,
    advertise_ip: IpAddr,
    max_legs: usize,
    recording_dir: PathBuf,
    io: IoHandle,
    threads: RtpThreads,
    /// Where `play` finds its files.
    prompt_dir: PathBuf,
    /// The DTLS certificate for every WebRTC leg.
    identity: Arc<Identity>,
    #[cfg(feature = "test-codec")]
    test_codec: bool,
}

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// The address put in our SDP.
    pub advertise_ip: IpAddr,
    pub max_legs: usize,
    pub recording_dir: PathBuf,
    pub prompt_dir: PathBuf,
    /// Accept the test codec (`x-pulse`) from endpoints that offer it.
    #[cfg(feature = "test-codec")]
    pub test_codec: bool,
}

impl Engine {
    pub fn new(cfg: EngineConfig, pool: PortPool, io: IoHandle, threads: RtpThreads, identity: Arc<Identity>) -> Engine {
        Engine {
            state: Mutex::new(State::default()),
            pool,
            advertise_ip: cfg.advertise_ip,
            max_legs: cfg.max_legs,
            recording_dir: cfg.recording_dir,
            io,
            threads,
            prompt_dir: cfg.prompt_dir,
            identity,
            #[cfg(feature = "test-codec")]
            test_codec: cfg.test_codec,
        }
    }

    /// Parses an SDP. The test codec counts as one we don't know unless it's switched on.
    fn parse_sdp(&self, text: &str) -> Result<sdp::Audio, Error> {
        #[allow(unused_mut)]
        let mut audio = sdp::parse(text)?;
        #[cfg(feature = "test-codec")]
        if !self.test_codec {
            for p in audio.payloads.iter_mut().filter(|p| p.codec == Some(Codec::Pulse)) {
                p.codec = None;
            }
        }
        Ok(audio)
    }

    /// Makes `slot` a WebRTC leg, or updates it: `remote` from the peer's SDP (`None` while
    /// our offer to it is out), `mid` from its SDP. New ICE credentials if the peer
    /// restarted ICE (a new ufrag).
    fn setup_webrtc(&self, slot: &mut Slot, remote: Option<webrtc::Remote>, mid: Option<String>) {
        let restart = match (&slot.webrtc, &remote) {
            (Some(w), Some(r)) => w.remote.as_ref().is_some_and(|old| old.ufrag != r.ufrag),
            _ => false,
        };
        if slot.webrtc.is_none() {
            // Nothing goes to it in clear, not even before its thread sets it up.
            slot.handle.shared.set_srtp_pending();
        }
        if slot.webrtc.is_none() || restart {
            slot.webrtc = Some(WebRtcLeg { local: Credentials::random(), mid: "0".into(), remote: None });
        }
        let w = slot.webrtc.as_mut().expect("set above");
        if let Some(mid) = mid {
            w.mid = mid;
        }
        if remote.is_some() {
            w.remote = remote;
        }
        let setup = webrtc::Setup { identity: self.identity.clone(), local: w.local.clone(), remote: w.remote.clone() };
        slot.handle.shared.post(LegCmd::WebRtc(Box::new(setup)));
    }

    /// The SDP for a leg: WebRTC if it is one, with `setup` (`actpass` when we offer).
    fn describe(&self, slot: &Slot, d: Description<'_>, setup: Setup) -> String {
        let webrtc = slot.webrtc.as_ref().map(|w| LocalWebRtc {
            ufrag: &w.local.ufrag,
            pwd: &w.local.pwd,
            fingerprint: self.identity.fingerprint(),
            setup,
            mid: &w.mid,
        });
        sdp::describe(&Description { webrtc, ..d })
    }

    /// Every thread's cycle figures (delta, lateness, load), with the legs on each RTP thread.
    pub fn thread_stats(&self) -> Vec<ThreadSnapshot> {
        self.threads.snapshot()
    }

    /// Rebalancing, once a second: moves whole calls and conferences off hot RTP threads.
    pub fn rebalance(&self, cfg: &RebalanceConfig) -> Vec<Rebalanced> {
        self.rebalance_with(cfg, &self.threads.loads())
    }

    /// `rebalance` with the threads' figures given (tests use made-up ones).
    pub fn rebalance_with(&self, cfg: &RebalanceConfig, threads: &[ThreadLoad]) -> Vec<Rebalanced> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let movable = |moved_at: Option<Instant>| moved_at.is_none_or(|t| now.duration_since(t) >= rebalance::MIN_STAY);
        let mut keys = Vec::new();
        let mut groups = Vec::new();
        for (id, call) in &state.calls {
            keys.push(Group::Call(id.clone()));
            groups.push(rebalance::Group {
                legs: vec![call.a.handle.thread_index(), call.b.handle.thread_index()],
                movable: movable(call.moved_at),
            });
        }
        for (id, conf) in &state.conferences {
            keys.push(Group::Conference(id.clone()));
            groups.push(rebalance::Group {
                legs: conf.participants.values().map(|s| s.handle.thread_index()).collect(),
                movable: movable(conf.moved_at),
            });
        }
        let moves = rebalance::plan(cfg, threads, &groups);
        moves
            .into_iter()
            .map(|m| {
                let group = keys[m.group].clone();
                let legs = self.move_slots(&mut state, &group, m.to, now);
                Rebalanced { group, legs, from: m.from, to: m.to, reason: m.reason }
            })
            .collect()
    }

    /// Moves every leg of a call or conference to RTP thread `to`. Returns how many legs
    /// changed thread.
    pub fn move_group(&self, group: &Group, to: usize) -> Result<usize, Error> {
        if to >= self.threads.len() {
            return Err(Error::InvalidState(format!("there is no RTP thread {to}")));
        }
        let mut state = self.state.lock().unwrap();
        let exists = match group {
            Group::Call(id) => state.calls.contains_key(id),
            Group::Conference(id) => state.conferences.contains_key(id),
        };
        if !exists {
            return Err(Error::NotFound(group.to_string()));
        }
        Ok(self.move_slots(&mut state, group, to, Instant::now()))
    }

    fn move_slots(&self, state: &mut State, group: &Group, to: usize, now: Instant) -> usize {
        let threads = &self.threads;
        let moved = match group {
            Group::Call(id) => state.calls.get_mut(id).map(|call| {
                call.moved_at = Some(now);
                [&mut call.a, &mut call.b].into_iter().map(|s| threads.move_leg(&mut s.handle, to)).filter(|&m| m).count()
            }),
            Group::Conference(id) => state.conferences.get_mut(id).map(|conf| {
                conf.moved_at = Some(now);
                conf.participants.values_mut().map(|s| threads.move_leg(&mut s.handle, to)).filter(|&m| m).count()
            }),
        };
        moved.unwrap_or(0)
    }

    /// Stops the RTP threads. Called at shutdown, after every leg has ended.
    pub fn stop_threads(&self) {
        self.threads.shutdown();
    }

    // ---- legs ----

    fn new_slot(&self, state: &mut State, place: Place, remote: Option<SocketAddr>) -> Result<Slot, Error> {
        if state.legs >= self.max_legs {
            return Err(Error::Full(self.max_legs));
        }
        let sockets = self.pool.allocate().map_err(|_| Error::NoPorts)?;
        let port = sockets.port;
        let config = Arc::new(LegConfig {
            place,
            codec: None,
            peers: Vec::new(),
            forward: None,
            generate: false,
            dtmf_peer: None,
            muted: false,
        });
        let handle = self.threads.bind(sockets, config).inspect_err(|_| self.pool.release(port))?;
        if let Some(addr) = remote {
            handle.shared.set_remote(addr);
        }
        state.legs += 1;
        Ok(Slot {
            handle,
            codec: None,
            injected: false,
            muted: false,
            whisper_to: None,
            sdp_origin: (random_u64() >> 1, 0),
            webrtc: None,
            recording: false,
            created_at_ms: unix_ms(),
        })
    }

    /// Unbinds a leg from its RTP thread, finishes its recording and frees its ports.
    fn end_slot(&self, state: &mut State, place: Place, participant_id: String, slot: Slot) -> Ended {
        let mut stats = participant_stats(participant_id, &slot);
        let port = slot.handle.shared.port;
        let recording = slot.handle.stop().and_then(|r| match r.close().recv() {
            Ok(Ok(status)) => Some(status),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "recording did not finish cleanly");
                None
            }
            Err(_) => None,
        });
        stats.leg.recording = recording.clone();
        state.legs -= 1;
        self.pool.release(port);
        Ended { place, stats, recording }
    }

    fn configure_call(call_id: &str, call: &Call) {
        let (a, b) = (&call.a, &call.b);
        for (me, peer, side) in [(a, b, Side::A), (b, a, Side::B)] {
            let forward_out = can_forward(me, peer);
            let forward_in = can_forward(peer, me);
            let peer_leg = peer.handle.shared.clone();
            let config = LegConfig {
                place: Place::Call { call_id: call_id.to_string(), side },
                codec: me.codec.clone(),
                peers: vec![peer_leg.clone()],
                forward: forward_out.then(|| Forward {
                    to: peer_leg.clone(),
                    passthrough: Passthrough::new(me.codec.as_ref(), peer.codec.as_ref()),
                }),
                generate: !forward_in,
                // The peer's stream is made here, so it has to be told about this leg's DTMF.
                dtmf_peer: (!forward_out).then(|| DtmfPeer {
                    leg: peer_leg,
                    takes_events: peer.codec.as_ref().is_some_and(|c| c.dtmf.is_some()),
                }),
                muted: me.muted,
            };
            me.handle.shared.post(LegCmd::Configure(Arc::new(config)));
        }
    }

    fn configure_conference(conference_id: &str, conf: &Conference) {
        for (pid, slot) in &conf.participants {
            let peers = conf
                .participants
                .iter()
                .filter(|(other, s)| *other != pid && heard_by(s, pid))
                .map(|(_, s)| s.handle.shared.clone())
                .collect();
            let config = LegConfig {
                place: Place::Conference { conference_id: conference_id.to_string(), participant_id: pid.clone() },
                codec: slot.codec.clone(),
                peers,
                forward: None,
                generate: true,
                dtmf_peer: None,
                muted: slot.muted,
            };
            slot.handle.shared.post(LegCmd::Configure(Arc::new(config)));
        }
    }

    // ---- calls ----

    fn get_or_start_call<'s>(&self, state: &'s mut State, call_id: &str) -> Result<(&'s mut Call, bool), Error> {
        if !state.calls.contains_key(call_id) {
            let a = self.new_slot(state, Place::Call { call_id: call_id.into(), side: Side::A }, None)?;
            let b = match self.new_slot(state, Place::Call { call_id: call_id.into(), side: Side::B }, None) {
                Ok(b) => b,
                Err(e) => {
                    self.end_slot(state, Place::Call { call_id: call_id.into(), side: Side::A }, "a".into(), a);
                    return Err(e);
                }
            };
            let negotiation =
                Negotiation {
                    offerer: Side::A,
                    offer_sdp: None,
                    offer: Vec::new(),
                    events: Vec::new(),
                    sdp_session_id: random_u64() >> 1,
                    sdp_version: 0,
                };
            let call = Call { a, b, negotiation, created_at_ms: unix_ms(), moved_at: None };
            Self::configure_call(call_id, &call);
            state.calls.insert(call_id.to_string(), call);
            return Ok((state.calls.get_mut(call_id).expect("just inserted"), true));
        }
        Ok((state.calls.get_mut(call_id).expect("checked"), false))
    }

    fn call_info(&self, call_id: &str, call: &Call, created: bool) -> CallInfo {
        let ports = |s: &Slot| LegPorts { rtp_port: s.handle.shared.port, rtcp_port: s.handle.shared.port + 1 };
        CallInfo { call_id: call_id.to_string(), ip: self.advertise_ip, a: ports(&call.a), b: ports(&call.b), created }
    }

    /// A call without SDP: both legs pass media through as it comes.
    pub fn create(&self, call_id: &str, a: Option<SocketAddr>, b: Option<SocketAddr>) -> Result<CallInfo, Error> {
        let mut state = self.state.lock().unwrap();
        let (call, created) = self.get_or_start_call(&mut state, call_id)?;
        set_remotes(call, a, b);
        Ok(self.call_info(call_id, call, created))
    }

    pub fn update(&self, call_id: &str, a: Option<SocketAddr>, b: Option<SocketAddr>) -> Result<CallStats, Error> {
        let state = self.state.lock().unwrap();
        let call = state.calls.get(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
        set_remotes(call, a, b);
        Ok(call_stats(call_id, call))
    }

    /// A leg's SDP offer (`from`: A at the start, either leg later). Creates the call if
    /// needed; returns the offer for the other leg:
    /// - all the offered audio formats, in their order, including ones we can only pass
    ///   through;
    /// - then, if the offer has something we can decode, the other codecs we can transcode
    ///   to;
    /// - then `telephone-event` for each clock rate: the offerer's own where it has one, ours
    ///   where we can generate DTMF ourselves.
    ///
    /// `to_webrtc`: the other leg is a WebRTC endpoint (e.g. an agent's browser), so the
    /// offer to it is WebRTC.
    pub fn offer(&self, call_id: &str, sdp_text: &str, from: Side, to_webrtc: bool) -> Result<OfferResult, Error> {
        let offer = self.parse_sdp(sdp_text)?;
        let offered: Vec<Payload> = offer.payloads.iter().filter(|p| p.is_audio()).cloned().collect();
        if offered.is_empty() {
            return Err(Error::UnsupportedCodec(format!("leg {} offered no audio format", side_name(from))));
        }
        let events: Vec<Payload> = offer.payloads.iter().filter(|p| p.is_telephone_event()).cloned().collect();
        let mut taken = offer.payloads.clone();
        let mut b_offer: Vec<Payload> = offer.payloads.iter().filter(|p| !p.is_telephone_event()).cloned().collect();
        if offered.iter().any(|p| p.codec.is_some()) {
            for codec in Codec::ALL {
                if b_offer.iter().any(|p| p.codec == Some(codec)) {
                    continue;
                }
                let pt = codec
                    .static_payload_type()
                    .filter(|pt| !taken.iter().any(|p| p.pt == *pt))
                    .or_else(|| sdp::free_dynamic_pt(&taken));
                if let Some(pt) = pt {
                    let payload = Payload::for_codec(codec, pt);
                    taken.push(payload.clone());
                    b_offer.push(payload);
                }
            }
        }
        let mut clocks: Vec<u32> = Vec::new();
        for p in b_offer.iter().filter(|p| p.is_audio()) {
            if !clocks.contains(&p.clock_rate) {
                clocks.push(p.clock_rate);
            }
        }
        for clock in clocks {
            let ours = b_offer.iter().any(|p| p.codec.is_some() && p.clock_rate == clock);
            let event = match offer.telephone_event(clock) {
                Some(p) => Some(p.clone()),
                None if ours => sdp::free_dynamic_pt(&taken).map(|pt| Payload::telephone_event(pt, clock)),
                None => None,
            };
            if let Some(event) = event {
                taken.push(event.clone());
                b_offer.push(event);
            }
        }

        let mut state = self.state.lock().unwrap();
        let (call, created) = self.get_or_start_call(&mut state, call_id)?;
        if let Some(addr) = offer.addr {
            call.slot(from).handle.shared.set_remote(addr);
        }
        let to = from.other();
        if let Some(w) = &offer.webrtc {
            self.setup_webrtc(call.slot_mut(from), Some(remote_of(w)), offer.mid.clone());
        }
        if to_webrtc || call.slot(to).webrtc.is_some() {
            self.setup_webrtc(call.slot_mut(to), None, None);
        }
        let port = call.slot(to).handle.shared.port;
        let neg = &mut call.negotiation;
        neg.offerer = from;
        neg.offer = offered;
        neg.events = events;
        let version = neg.next_version();
        let session_id = neg.sdp_session_id;
        let direction = offer.direction;
        neg.offer_sdp = Some(offer);
        let d = Description {
            ip: self.advertise_ip,
            port,
            payloads: &b_offer,
            direction,
            session_id,
            version,
            webrtc: None,
            answering: None,
        };
        let sdp = self.describe(call.slot(to), d, Setup::ActPass);
        Ok(OfferResult { to, sdp, call: self.call_info(call_id, call, created) })
    }

    /// The SDP answer from the leg that didn't make the latest offer (`from` may name it, as
    /// a check). The answer's first audio format is the answering leg's codec. The offering
    /// leg uses the same one if it offered it (passthrough, even for codecs we can't decode),
    /// otherwise its first codec we can decode (transcoding). Returns the answer for the
    /// offering leg.
    pub fn answer(&self, call_id: &str, sdp_text: &str, from: Option<Side>) -> Result<AnswerResult, Error> {
        let answer = self.parse_sdp(sdp_text)?;
        let mut state = self.state.lock().unwrap();
        let call = state.calls.get_mut(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
        let neg = &mut call.negotiation;
        if neg.offer.is_empty() {
            return Err(Error::InvalidState("answer before offer".into()));
        }
        let (offerer, answerer) = (neg.offerer, neg.offerer.other());
        if let Some(from) = from
            && from != answerer
        {
            return Err(Error::InvalidState(format!(
                "the offer came from leg {}, so the answer comes from leg {}",
                side_name(offerer),
                side_name(answerer)
            )));
        }
        let answered = answer.payloads.iter().find(|p| p.is_audio()).cloned().ok_or_else(|| {
            Error::UnsupportedCodec(format!("leg {} answered no audio format", side_name(answerer)))
        })?;
        let same = |p: &&Payload| p.name.eq_ignore_ascii_case(&answered.name) && p.clock_rate == answered.clock_rate;
        let offered = match neg.offer.iter().find(same) {
            Some(p) => p.clone(),
            None if answered.codec.is_none() => {
                return Err(Error::UnsupportedCodec(format!(
                    "leg {} answered {}, which leg {} didn't offer and can't be transcoded",
                    side_name(answerer),
                    answered.name,
                    side_name(offerer)
                )));
            }
            None => neg.offer.iter().find(|p| p.codec.is_some()).cloned().ok_or_else(|| {
                Error::UnsupportedCodec(format!("leg {} offered nothing that can be transcoded", side_name(offerer)))
            })?,
        };
        let offered_event = neg.events.iter().find(|p| p.clock_rate == offered.clock_rate).cloned();
        let answered_event = answer.telephone_event(answered.clock_rate).map(|p| p.pt);
        let offerer_codec = LegCodec::from_payload(&offered, offered_event.as_ref().map(|p| p.pt));
        let answerer_codec = LegCodec::from_payload(&answered, answered_event);
        let version = neg.next_version();
        let session_id = neg.sdp_session_id;
        let back: Vec<Payload> = std::iter::once(offered).chain(offered_event).collect();

        match (&answer.webrtc, call.slot(answerer).webrtc.is_some()) {
            (Some(w), _) => self.setup_webrtc(call.slot_mut(answerer), Some(remote_of(w)), answer.mid.clone()),
            (None, true) => {
                return Err(Error::InvalidState(format!(
                    "leg {} answered a WebRTC offer with {}",
                    side_name(answerer),
                    answer.proto
                )));
            }
            (None, false) => {}
        }
        if let Some(addr) = answer.addr {
            call.slot(answerer).handle.shared.set_remote(addr);
        }
        call.slot_mut(offerer).codec = Some(offerer_codec);
        call.slot_mut(answerer).codec = Some(answerer_codec);
        Self::configure_call(call_id, call);
        let mode = call.mode();
        let codecs = Codecs { a: call.a.codec.clone(), b: call.b.codec.clone() };
        let port = call.slot(offerer).handle.shared.port;
        let d = Description {
            ip: self.advertise_ip,
            port,
            payloads: &back,
            direction: answer.direction,
            session_id,
            version,
            webrtc: None,
            answering: call.negotiation.offer_sdp.as_ref(),
        };
        let sdp = self.describe(call.slot(offerer), d, answer_setup(call.slot(offerer)));
        tracing::info!(call_id, ?mode, offerer = side_name(offerer), "media negotiated");
        Ok(AnswerResult { to: offerer, sdp, mode, codecs })
    }

    pub fn query_call(&self, call_id: &str) -> Result<CallStats, Error> {
        let state = self.state.lock().unwrap();
        let call = state.calls.get(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
        Ok(call_stats(call_id, call))
    }

    /// Ends a call: stops both legs and finishes their recordings.
    pub fn delete_call(&self, call_id: &str) -> Result<(CallStats, Vec<Ended>), Error> {
        let mut state = self.state.lock().unwrap();
        let call = state.calls.remove(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
        let mut stats = call_stats(call_id, &call);
        let place = |side| Place::Call { call_id: call_id.to_string(), side };
        let a = self.end_slot(&mut state, place(Side::A), "a".into(), call.a);
        let b = self.end_slot(&mut state, place(Side::B), "b".into(), call.b);
        stats.a.recording = a.recording.clone();
        stats.b.recording = b.recording.clone();
        Ok((stats, vec![a, b]))
    }

    // ---- conferences ----

    /// Adds a participant from its SDP offer; creates the conference if needed. Returns the
    /// answer for the participant. Conferences mix audio, so the participant needs a codec
    /// this server decodes.
    /// A participant joins with its SDP offer. `muted`: it joins not heard, e.g. a
    /// supervisor listening in.
    pub fn join(&self, conference_id: &str, participant_id: &str, sdp_text: &str, muted: bool) -> Result<JoinResult, Error> {
        let offer = self.parse_sdp(sdp_text)?;
        let chosen = offer.supported().next().cloned().ok_or_else(|| {
            let names: Vec<&str> = offer.payloads.iter().map(|p| p.name.as_str()).collect();
            Error::UnsupportedCodec(format!("conferences mix audio; {names:?} has no PCMU, PCMA or Opus"))
        })?;
        let event = offer.telephone_event(chosen.clock_rate).cloned();
        let codec = LegCodec::from_payload(&chosen, event.as_ref().map(|p| p.pt));

        let mut state = self.state.lock().unwrap();
        if state.conferences.get(conference_id).is_some_and(|c| c.participants.contains_key(participant_id)) {
            return Err(Error::InvalidState(format!("{participant_id} is already in conference {conference_id}")));
        }
        let place = Place::Conference { conference_id: conference_id.into(), participant_id: participant_id.into() };
        let mut slot = self.new_slot(&mut state, place, offer.addr)?;
        slot.codec = Some(codec.clone());
        slot.muted = muted;
        if let Some(w) = &offer.webrtc {
            self.setup_webrtc(&mut slot, Some(remote_of(w)), offer.mid.clone());
        }
        let port = slot.handle.shared.port;
        let conference_created = !state.conferences.contains_key(conference_id);
        let conf = state
            .conferences
            .entry(conference_id.to_string())
            .or_insert_with(Conference::new);
        conf.participants.insert(participant_id.to_string(), slot);
        Self::configure_conference(conference_id, conf);

        let slot = conf.participants.get_mut(participant_id).expect("just inserted");
        slot.sdp_origin.1 += 1;
        let (session_id, version) = slot.sdp_origin;
        let payloads: Vec<Payload> = std::iter::once(chosen).chain(event).collect();
        let d = Description {
            ip: self.advertise_ip,
            port,
            payloads: &payloads,
            direction: offer.direction.answer(),
            session_id,
            version,
            webrtc: None,
            answering: Some(&offer),
        };
        let sdp = self.describe(slot, d, answer_setup(slot));
        tracing::info!(conference_id, participant_id, codec = %codec.codec, muted, webrtc = offer.is_webrtc(), "participant joined");
        Ok(JoinResult {
            sdp,
            conference_id: conference_id.into(),
            participant_id: participant_id.into(),
            ip: self.advertise_ip,
            rtp_port: port,
            rtcp_port: port + 1,
            codec,
            conference_created,
        })
    }

    /// A participant's new SDP offer (a re-INVITE: hold, a new address, another codec). It
    /// keeps its ports, recording and mode; returns the answer for it.
    pub fn reoffer(&self, conference_id: &str, participant_id: &str, sdp_text: &str) -> Result<ReofferResult, Error> {
        let offer = self.parse_sdp(sdp_text)?;
        let chosen = offer.supported().next().cloned().ok_or_else(|| {
            let names: Vec<&str> = offer.payloads.iter().map(|p| p.name.as_str()).collect();
            Error::UnsupportedCodec(format!("conferences mix audio; {names:?} has no PCMU, PCMA or Opus"))
        })?;
        let event = offer.telephone_event(chosen.clock_rate).cloned();
        let codec = LegCodec::from_payload(&chosen, event.as_ref().map(|p| p.pt));

        let mut state = self.state.lock().unwrap();
        let conf = state.conferences.get_mut(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
        let slot = conf
            .participants
            .get_mut(participant_id)
            .ok_or_else(|| Error::NotFound(format!("participant {participant_id} in conference {conference_id}")))?;
        if let Some(addr) = offer.addr {
            slot.handle.shared.set_remote(addr);
        }
        if let Some(w) = &offer.webrtc {
            self.setup_webrtc(slot, Some(remote_of(w)), offer.mid.clone());
        }
        slot.codec = Some(codec.clone());
        slot.sdp_origin.1 += 1;
        let (session_id, version) = slot.sdp_origin;
        let port = slot.handle.shared.port;
        Self::configure_conference(conference_id, conf);
        let slot = conf.participants.get(participant_id).expect("still there");
        let payloads: Vec<Payload> = std::iter::once(chosen).chain(event).collect();
        let d = Description {
            ip: self.advertise_ip,
            port,
            payloads: &payloads,
            direction: offer.direction.answer(),
            session_id,
            version,
            webrtc: None,
            answering: Some(&offer),
        };
        let sdp = self.describe(slot, d, answer_setup(slot));
        tracing::info!(conference_id, participant_id, codec = %codec.codec, "participant re-negotiated");
        Ok(ReofferResult { sdp, codec })
    }

    /// Removes a participant. The conference ends with its last participant; until then a
    /// participant may well be alone in it.
    pub fn leave(&self, conference_id: &str, participant_id: &str) -> Result<Left, Error> {
        let mut state = self.state.lock().unwrap();
        let conf = state.conferences.get_mut(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
        let slot = conf
            .participants
            .remove(participant_id)
            .ok_or_else(|| Error::NotFound(format!("participant {participant_id} in conference {conference_id}")))?;
        let conference_ended = conf.participants.is_empty();
        if conference_ended {
            state.conferences.remove(conference_id);
        } else {
            Self::configure_conference(conference_id, conf);
        }
        let place = Place::Conference { conference_id: conference_id.into(), participant_id: participant_id.into() };
        let ended = self.end_slot(&mut state, place, participant_id.into(), slot);
        Ok(Left { ended, conference_ended })
    }

    pub fn delete_conference(&self, conference_id: &str) -> Result<Vec<Ended>, Error> {
        let mut state = self.state.lock().unwrap();
        let conf = state.conferences.remove(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
        Ok(conf
            .participants
            .into_iter()
            .map(|(pid, slot)| {
                let place = Place::Conference { conference_id: conference_id.into(), participant_id: pid.clone() };
                self.end_slot(&mut state, place, pid, slot)
            })
            .collect())
    }

    pub fn query_conference(&self, conference_id: &str) -> Result<ConferenceStats, Error> {
        let state = self.state.lock().unwrap();
        let conf = state.conferences.get(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
        Ok(conference_stats(conference_id, conf))
    }

    /// Moves every participant of `from` into `into`, and ends `from`. Participants keep
    /// their ports, streams and recordings. Returns the ids that moved.
    pub fn merge(&self, from: &str, into: &str) -> Result<Vec<String>, Error> {
        if from == into {
            return Err(Error::InvalidState("can't merge a conference into itself".into()));
        }
        let mut state = self.state.lock().unwrap();
        let source = state.conferences.get(from).ok_or_else(|| Error::NotFound(format!("conference {from}")))?;
        let target = state.conferences.get(into).ok_or_else(|| Error::NotFound(format!("conference {into}")))?;
        if let Some(clash) = source.participants.keys().find(|p| target.participants.contains_key(*p)) {
            return Err(Error::InvalidState(format!("participant {clash} is in both conferences")));
        }
        let source = state.conferences.remove(from).expect("checked");
        let moved: Vec<String> = source.participants.keys().cloned().collect();
        let target = state.conferences.get_mut(into).expect("checked");
        target.participants.extend(source.participants);
        Self::configure_conference(into, target);
        tracing::info!(from, into, participants = moved.len(), "conferences merged");
        Ok(moved)
    }

    /// Moves both legs of a call into a conference (created if needed) as participants
    /// `a_id` and `b_id`. They keep their ports, streams and recordings; the call is gone.
    pub fn move_call(&self, call_id: &str, conference_id: &str, a_id: &str, b_id: &str) -> Result<bool, Error> {
        if a_id == b_id {
            return Err(Error::InvalidState("the two legs need different participant ids".into()));
        }
        let mut state = self.state.lock().unwrap();
        let call = state.calls.get(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
        for slot in [&call.a, &call.b] {
            if slot.codec.as_ref().and_then(|c| c.supported).is_none() {
                return Err(Error::InvalidState(
                    "both legs need a negotiated codec this server decodes (PCMU, PCMA or Opus) to join a conference".into(),
                ));
            }
        }
        if let Some(conf) = state.conferences.get(conference_id)
            && let Some(clash) = [a_id, b_id].into_iter().find(|id| conf.participants.contains_key(*id))
        {
            return Err(Error::InvalidState(format!("participant {clash} is already in conference {conference_id}")));
        }
        let call = state.calls.remove(call_id).expect("checked");
        // The legs' next SDP goes on from the call's.
        let origin = (call.negotiation.sdp_session_id, call.negotiation.sdp_version);
        let created = !state.conferences.contains_key(conference_id);
        let conf = state
            .conferences
            .entry(conference_id.to_string())
            .or_insert_with(Conference::new);
        for (id, mut slot) in [(a_id, call.a), (b_id, call.b)] {
            slot.injected = false;
            slot.sdp_origin = origin;
            conf.participants.insert(id.to_string(), slot);
        }
        Self::configure_conference(conference_id, conf);
        tracing::info!(call_id, conference_id, "call moved into conference");
        Ok(created)
    }

    // ---- legs, by place ----

    fn with_slot<R>(&self, place: &Place, f: impl FnOnce(&mut Slot) -> Result<R, Error>) -> Result<R, Error> {
        let mut state = self.state.lock().unwrap();
        let slot = match place {
            Place::Call { call_id, side } => state
                .calls
                .get_mut(call_id)
                .ok_or_else(|| Error::NotFound(format!("call {call_id}")))?
                .slot_mut(*side),
            Place::Conference { conference_id, participant_id } => state
                .conferences
                .get_mut(conference_id)
                .ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?
                .participants
                .get_mut(participant_id)
                .ok_or_else(|| Error::NotFound(format!("participant {participant_id} in conference {conference_id}")))?,
        };
        f(slot)
    }

    /// Changes a leg, then reconfigures its call or conference.
    fn change_leg<R>(&self, place: &Place, f: impl FnOnce(&mut Slot) -> Result<R, Error>) -> Result<R, Error> {
        let mut state = self.state.lock().unwrap();
        match place {
            Place::Call { call_id, side } => {
                let call = state.calls.get_mut(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
                let r = f(call.slot_mut(*side))?;
                Self::configure_call(call_id, call);
                Ok(r)
            }
            Place::Conference { conference_id, participant_id } => {
                let conf = state
                    .conferences
                    .get_mut(conference_id)
                    .ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
                let slot = conf
                    .participants
                    .get_mut(participant_id)
                    .ok_or_else(|| Error::NotFound(format!("participant {participant_id} in conference {conference_id}")))?;
                let r = f(slot)?;
                Self::configure_conference(conference_id, conf);
                Ok(r)
            }
        }
    }

    /// Soft-mutes or unmutes a leg (ending any whisper). Returns whether that changed
    /// anything.
    pub fn set_muted(&self, place: &Place, muted: bool) -> Result<bool, Error> {
        self.change_leg(place, |slot| {
            let changed = slot.muted != muted || slot.whisper_to.is_some();
            slot.muted = muted;
            slot.whisper_to = None;
            Ok(changed)
        })
    }

    /// Sets a participant's supervisor mode. `whisper_to` names the one participant who
    /// hears a whisper.
    pub fn set_mode(&self, conference_id: &str, participant_id: &str, mode: ParticipantMode, whisper_to: Option<&str>) -> Result<(), Error> {
        let place = Place::Conference { conference_id: conference_id.into(), participant_id: participant_id.into() };
        if let Some(to) = whisper_to {
            if to == participant_id {
                return Err(Error::InvalidState("a participant can't whisper to itself".into()));
            }
            let state = self.state.lock().unwrap();
            let conf = state.conferences.get(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
            if !conf.participants.contains_key(to) {
                return Err(Error::NotFound(format!("participant {to} in conference {conference_id}")));
            }
        }
        self.change_leg(&place, |slot| {
            (slot.muted, slot.whisper_to) = match mode {
                ParticipantMode::Listen => (true, None),
                ParticipantMode::Whisper => (false, whisper_to.map(str::to_string)),
                ParticipantMode::Barge => (false, None),
            };
            Ok(())
        })
    }

    /// Plays a file from the prompt directory into a leg. The file is read here, on the
    /// main thread; the leg's thread gets it decoded.
    pub fn play(&self, place: &Place, file: &str) -> Result<PlayStatus, Error> {
        let prompt = Arc::new(prompt::load(&self.prompt_dir, file)?);
        let status = PlayStatus { file: prompt.name.clone(), position_ms: 0, duration_ms: prompt.duration_ms() };
        let shared = self.change_leg(place, |slot| {
            check_can_generate(slot, "play audio")?;
            slot.injected = true;
            Ok(slot.handle.shared.clone())
        })?;
        shared.post(LegCmd::Play(prompt));
        Ok(status)
    }

    /// Stops a file playing into a leg. Returns what was playing, if anything.
    pub fn stop_play(&self, place: &Place) -> Result<Option<PlayStatus>, Error> {
        self.with_slot(place, |slot| {
            let playing = slot.handle.shared.playing();
            slot.handle.shared.post(LegCmd::StopPlay);
            Ok(playing)
        })
    }

    pub fn start_recording(&self, place: &Place) -> Result<RecordingStatus, Error> {
        let name = format!("{}-{}", place.label(), unix_ms());
        self.with_slot(place, |slot| {
            if slot.recording {
                return Err(Error::InvalidState("already recording".into()));
            }
            let recording = self.io.create(&self.recording_dir, &name, &place.key_label()).map_err(Error::Recording)?;
            let status = recording.status();
            slot.handle.shared.post(LegCmd::StartRecording(recording));
            slot.recording = true;
            Ok(status)
        })
    }

    pub fn stop_recording(&self, place: &Place) -> Result<RecordingStatus, Error> {
        self.with_slot(place, |slot| {
            if !slot.recording {
                return Err(Error::InvalidState("not recording".into()));
            }
            let (reply, rx) = mpsc::channel();
            slot.handle.shared.post(LegCmd::StopRecording(reply));
            slot.recording = false;
            let recording = rx
                .recv_timeout(Duration::from_secs(2))
                .ok()
                .flatten()
                .ok_or_else(|| Error::InvalidState("the leg had no recording".into()))?;
            recording.close().recv().map_err(|_| Error::Recording(io::Error::other("io thread stopped")))?.map_err(Error::Recording)
        })
    }

    pub fn is_recording(&self, place: &Place) -> bool {
        self.with_slot(place, |slot| Ok(slot.recording)).unwrap_or(false)
    }

    /// Queues digits for a leg: RFC 4733 events if it negotiated them, tones otherwise.
    pub fn send_dtmf(&self, place: &Place, digits: &str, duration: Duration, pause: Duration) -> Result<(usize, Source), Error> {
        let events: Vec<u8> = digits
            .chars()
            .map(char_to_event)
            .collect::<Option<_>>()
            .ok_or_else(|| Error::InvalidState(format!("not DTMF digits: {digits:?}")))?;
        let mut state = self.state.lock().unwrap();
        let slot = match place {
            Place::Call { call_id, side } => {
                let call = state.calls.get_mut(call_id).ok_or_else(|| Error::NotFound(format!("call {call_id}")))?;
                let slot = call.slot_mut(*side);
                check_can_generate(slot, "generate DTMF")?;
                slot.injected = true;
                Self::configure_call(call_id, call);
                call.slot(*side)
            }
            Place::Conference { conference_id, participant_id } => {
                let conf = state.conferences.get(conference_id).ok_or_else(|| Error::NotFound(format!("conference {conference_id}")))?;
                let slot = conf
                    .participants
                    .get(participant_id)
                    .ok_or_else(|| Error::NotFound(format!("participant {participant_id} in conference {conference_id}")))?;
                check_can_generate(slot, "generate DTMF")?;
                slot
            }
        };
        let method = if slot.codec.as_ref().is_some_and(|c| c.dtmf.is_some()) { Source::Rfc4733 } else { Source::Inband };
        slot.handle.shared.post(LegCmd::SendDtmf(events.iter().map(|&event| Injected { event, duration, pause }).collect()));
        Ok((events.len(), method))
    }

    // ---- everything ----

    pub fn calls(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.state.lock().unwrap().calls.keys().cloned().collect();
        ids.sort();
        ids
    }

    pub fn conferences(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.state.lock().unwrap().conferences.keys().cloned().collect();
        ids.sort();
        ids
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        let s = self.state.lock().unwrap();
        (s.calls.len(), s.conferences.len(), s.legs)
    }

    pub fn ports_available(&self) -> usize {
        self.pool.available()
    }

    pub fn ports_total(&self) -> usize {
        self.pool.total()
    }

    pub fn max_legs(&self) -> usize {
        self.max_legs
    }

    /// Calls where neither leg has received anything for `timeout`, and conference
    /// participants that haven't.
    pub fn idle(&self, timeout: Duration) -> (Vec<String>, Vec<(String, String)>) {
        let state = self.state.lock().unwrap();
        let idle = |s: &Slot| s.handle.shared.activity.idle() >= timeout;
        let calls = state.calls.iter().filter(|(_, c)| idle(&c.a) && idle(&c.b)).map(|(id, _)| id.clone()).collect();
        let participants = state
            .conferences
            .iter()
            .flat_map(|(cid, c)| c.participants.iter().filter(|(_, s)| idle(s)).map(move |(pid, _)| (cid.clone(), pid.clone())))
            .collect();
        (calls, participants)
    }
}

/// The peer's ICE and DTLS details from its SDP. Our DTLS role: client if it waits
/// (`passive`), server otherwise (`active`, or `actpass` in an offer we answer `passive`).
fn remote_of(w: &sdp::WebRtc) -> webrtc::Remote {
    let role = if w.setup == Setup::Passive { Role::Client } else { Role::Server };
    webrtc::Remote { ufrag: w.ufrag.clone(), fingerprint: w.fingerprint.clone(), role }
}

/// Our `a=setup` when answering a WebRTC leg's offer: the opposite of our role's peer.
fn answer_setup(slot: &Slot) -> Setup {
    match slot.webrtc.as_ref().and_then(|w| w.remote.as_ref()).map(|r| r.role) {
        Some(Role::Client) => Setup::Active,
        _ => Setup::Passive,
    }
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::A => "A",
        Side::B => "B",
    }
}

/// The leg's stream can be made here: its codec is one we encode.
fn check_can_generate(slot: &Slot, what: &str) -> Result<(), Error> {
    match &slot.codec {
        Some(c) if c.supported.is_some() => Ok(()),
        Some(c) => Err(Error::InvalidState(format!("can't {what} in {}, which is only passed through", c.codec))),
        None => Err(Error::InvalidState(format!("to {what}, the leg needs codecs negotiated with offer/answer"))),
    }
}

/// Whether `listener` hears `speaker`: not muted, and not whispering to someone else.
fn heard_by(speaker: &Slot, listener: &str) -> bool {
    !speaker.muted && speaker.whisper_to.as_deref().is_none_or(|to| to == listener)
}

/// Whether `from`'s packets can go to `to` as they are.
fn can_forward(from: &Slot, to: &Slot) -> bool {
    if to.injected {
        return false;
    }
    match (&from.codec, &to.codec) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            // A DTMF mismatch needs conversion, which needs a codec we can generate; with one
            // we can't, the events are dropped instead.
            x.same_codec(y) && (x.dtmf.is_some() == y.dtmf.is_some() || x.supported.is_none())
        }
        _ => false,
    }
}

fn set_remotes(call: &Call, a: Option<SocketAddr>, b: Option<SocketAddr>) {
    if let Some(addr) = a {
        call.a.handle.shared.set_remote(addr);
    }
    if let Some(addr) = b {
        call.b.handle.shared.set_remote(addr);
    }
}

fn call_stats(call_id: &str, call: &Call) -> CallStats {
    let (a, b) = (&call.a.handle.shared, &call.b.handle.shared);
    let age = a.activity.age();
    CallStats {
        call_id: call_id.to_string(),
        created_at_ms: call.created_at_ms,
        age_ms: age.as_millis() as u64,
        idle_ms: a.activity.idle().min(b.activity.idle()).as_millis() as u64,
        mode: call.mode(),
        codecs: Codecs { a: call.a.codec.clone(), b: call.b.codec.clone() },
        a: leg_stats(&call.a),
        b: leg_stats(&call.b),
    }
}

fn leg_stats(slot: &Slot) -> LegStats {
    let mut stats = slot.handle.shared.stats();
    stats.muted = slot.muted;
    stats
}

fn participant_stats(participant_id: String, slot: &Slot) -> ParticipantStats {
    ParticipantStats {
        participant_id,
        joined_at_ms: slot.created_at_ms,
        idle_ms: slot.handle.shared.activity.idle().as_millis() as u64,
        codec: slot.codec.clone(),
        whisper_to: slot.whisper_to.clone(),
        leg: leg_stats(slot),
    }
}

fn conference_stats(conference_id: &str, conf: &Conference) -> ConferenceStats {
    ConferenceStats {
        conference_id: conference_id.to_string(),
        created_at_ms: conf.created_at_ms,
        participants: conf.participants.iter().map(|(pid, s)| participant_stats(pid.clone(), s)).collect(),
    }
}

impl Engine {
    /// Ends everything (on shutdown).
    pub fn end_all(&self) -> Vec<Ended> {
        let mut state = self.state.lock().unwrap();
        let calls: Vec<(String, Call)> = state.calls.drain().collect();
        let conferences: Vec<(String, Conference)> = state.conferences.drain().collect();
        let mut ended = Vec::new();
        for (call_id, call) in calls {
            for (side, slot) in [(Side::A, call.a), (Side::B, call.b)] {
                let place = Place::Call { call_id: call_id.clone(), side };
                ended.push(self.end_slot(&mut state, place, place_label_side(side), slot));
            }
        }
        for (conference_id, conf) in conferences {
            for (pid, slot) in conf.participants {
                let place = Place::Conference { conference_id: conference_id.clone(), participant_id: pid.clone() };
                ended.push(self.end_slot(&mut state, place, pid, slot));
            }
        }
        ended
    }
}

fn place_label_side(side: Side) -> String {
    if side == Side::A { "a".into() } else { "b".into() }
}

fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}
