//! The command protocol, served on the TCP control channel. The service's own API, still to
//! be described, will carry the same commands and events.
//!
//! A request is one JSON object with a `cmd` field. The optional `id` is echoed back in the
//! response, so clients can pipeline requests and match the answers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dtmf::char_to_event;
use crate::engine::{Ended, Engine, Error, ParticipantMode, unix_ms};
use crate::media::prompt::PromptError;
use crate::events::Events;
use crate::leg::Place;
use crate::media::Side;
use crate::recording::RecordingStatus;

pub const MAX_ID_LEN: usize = 256;
pub const MAX_DIGITS: usize = 64;

#[derive(Debug, Deserialize)]
pub struct Request {
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(flatten)]
    pub command: Command,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Ping,
    Stats,
    List,

    // Two-party calls.
    /// A leg's SDP offer: leg A's by default, or `leg: "b"` (e.g. a re-INVITE from B).
    /// Creates the call if needed; returns the SDP offer for the other leg.
    Offer {
        call_id: String,
        sdp: String,
        #[serde(default)]
        leg: Option<Side>,
        /// Start recording the offering leg as well.
        #[serde(default)]
        record: bool,
        /// The other leg (the one the offer is for) is a WebRTC endpoint: the offer to it is
        /// `UDP/TLS/RTP/SAVPF` with ICE-lite and DTLS.
        #[serde(default)]
        webrtc: bool,
    },
    /// The other leg's SDP answer. Picks passthrough or transcoding; returns the answer for
    /// the offering leg. `leg`, if given, must name the answering leg.
    Answer {
        call_id: String,
        sdp: String,
        #[serde(default)]
        leg: Option<Side>,
    },
    /// A call without SDP: both legs pass media through as it comes. Idempotent.
    Create {
        call_id: String,
        #[serde(default)]
        a: LegParams,
        #[serde(default)]
        b: LegParams,
    },
    /// Changes the remote address signalled for a leg (e.g. after a re-INVITE).
    Update {
        call_id: String,
        #[serde(default)]
        a: LegParams,
        #[serde(default)]
        b: LegParams,
    },
    Delete { call_id: String },
    Query { call_id: String },
    /// Moves both legs of a call into a conference, as participants `a_participant_id`
    /// (default `<call_id>-a`) and `b_participant_id` (default `<call_id>-b`).
    MoveToConference {
        call_id: String,
        conference_id: String,
        #[serde(default)]
        a_participant_id: Option<String>,
        #[serde(default)]
        b_participant_id: Option<String>,
    },

    // Conferences.
    /// A participant joins with its SDP offer (the conference is created if needed);
    /// returns the SDP answer for it.
    Join {
        conference_id: String,
        participant_id: String,
        sdp: String,
        #[serde(default)]
        record: bool,
        /// Joins not heard by anyone, e.g. a supervisor listening in.
        #[serde(default)]
        muted: bool,
    },
    Leave { conference_id: String, participant_id: String },
    /// A participant's new SDP offer (re-INVITE); returns the answer for it.
    Reoffer { conference_id: String, participant_id: String, sdp: String },
    /// A participant's supervisor mode: `listen` (muted), `whisper` (heard only by
    /// `whisper_to`) or `barge` (heard by everyone).
    SetMode {
        conference_id: String,
        participant_id: String,
        mode: ParticipantMode,
        #[serde(default)]
        whisper_to: Option<String>,
    },
    ConferenceQuery { conference_id: String },
    ConferenceDelete { conference_id: String },
    /// Moves every participant of `from` into `into`, and ends `from`.
    ConferenceMerge { from: String, into: String },

    // Any leg: `call_id` + `leg`, or `conference_id` + `participant_id`.
    StartRecording {
        #[serde(flatten)]
        target: Target,
    },
    StopRecording {
        #[serde(flatten)]
        target: Target,
    },
    /// Sends digits to a leg: RFC 4733 events if it negotiated them, tones otherwise.
    SendDtmf {
        #[serde(flatten)]
        target: Target,
        digits: String,
        #[serde(default = "default_digit_ms")]
        duration_ms: u64,
        #[serde(default = "default_pause_ms")]
        pause_ms: u64,
    },
    /// Soft mute: the leg's audio is replaced by silence for everyone else, packet by
    /// packet. It still hears them.
    Mute {
        #[serde(flatten)]
        target: Target,
    },
    Unmute {
        #[serde(flatten)]
        target: Target,
    },
    /// Plays a file from the prompt directory into a leg, mixed into what it hears (and
    /// so into its recording, frame by frame). Replaces a file still playing.
    Play {
        #[serde(flatten)]
        target: Target,
        file: String,
    },
    StopPlay {
        #[serde(flatten)]
        target: Target,
    },
}

/// Which leg a command is about.
#[derive(Debug, Default, Deserialize)]
pub struct Target {
    #[serde(default)]
    pub call_id: Option<String>,
    /// For calls; recording defaults to leg A.
    #[serde(default)]
    pub leg: Option<Side>,
    #[serde(default)]
    pub conference_id: Option<String>,
    #[serde(default)]
    pub participant_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegParams {
    /// RTP address from the leg's SDP. RTCP is assumed on the next port. Both are replaced by
    /// the actual source address once the first packet arrives.
    #[serde(default)]
    pub remote: Option<SocketAddr>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub node: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    NotFound,
    Full,
    NoPorts,
    UnsupportedCodec,
    InvalidState,
    Unavailable,
    Internal,
}

impl From<&Error> for ErrorCode {
    fn from(e: &Error) -> Self {
        match e {
            Error::NotFound(_) => ErrorCode::NotFound,
            Error::Full(_) => ErrorCode::Full,
            Error::NoPorts => ErrorCode::NoPorts,
            Error::Sdp(_) => ErrorCode::BadRequest,
            Error::UnsupportedCodec(_) => ErrorCode::UnsupportedCodec,
            Error::InvalidState(_) => ErrorCode::InvalidState,
            Error::Recording(_) | Error::Io(_) => ErrorCode::Internal,
            Error::Prompt(PromptError::BadName(_) | PromptError::Format { .. }) => ErrorCode::BadRequest,
            Error::Prompt(PromptError::NotFound(_)) => ErrorCode::NotFound,
            Error::Prompt(PromptError::Io { .. }) => ErrorCode::Internal,
        }
    }
}

/// Why something ended, as reported in events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    Deleted,
    Left,
    IdleTimeout,
    Shutdown,
}

type Failure = (ErrorCode, String);

pub struct Api {
    engine: Arc<Engine>,
    events: Arc<Events>,
    node_id: String,
    started: Instant,
    shutting_down: AtomicBool,
}

impl Api {
    pub fn new(engine: Arc<Engine>, events: Arc<Events>, node_id: String) -> Api {
        Api { engine, events, node_id, started: Instant::now(), shutting_down: AtomicBool::new(false) }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// New calls and participants are refused from now on; existing ones keep running.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Parses and runs one request.
    pub async fn handle_bytes(&self, bytes: &[u8]) -> Response {
        let value: Value = match serde_json::from_slice(bytes) {
            Ok(v) => v,
            Err(e) => return self.error(None, ErrorCode::BadRequest, format!("invalid JSON: {e}")),
        };
        let id = value.get("id").cloned();
        match serde_json::from_value::<Request>(value) {
            Ok(req) => self.handle(req).await,
            Err(e) => self.error(id, ErrorCode::BadRequest, e.to_string()),
        }
    }

    pub async fn handle(&self, req: Request) -> Response {
        let id = req.id;
        match self.run(req.command) {
            Ok(result) => Response { id, node: self.node_id.clone(), ok: true, result: Some(result), error: None },
            Err((code, message)) => self.error(id, code, message),
        }
    }

    fn run(&self, command: Command) -> Result<Value, Failure> {
        let engine = &self.engine;
        match command {
            Command::Ping => Ok(json!({ "pong": true })),
            Command::Stats => {
                let (calls, conferences, legs) = engine.counts();
                Ok(json!({
                    "node": self.node_id,
                    "uptime_ms": self.started.elapsed().as_millis() as u64,
                    "calls": calls,
                    "conferences": conferences,
                    "legs": legs,
                    "max_legs": engine.max_legs(),
                    "port_pairs_available": engine.ports_available(),
                    "port_pairs_total": engine.ports_total(),
                    "threads": engine.thread_stats(),
                    "shutting_down": self.shutting_down.load(Ordering::SeqCst),
                }))
            }
            Command::List => Ok(json!({ "calls": engine.calls(), "conferences": engine.conferences() })),

            Command::Offer { call_id, sdp, leg, record, webrtc } => {
                check_id("call_id", &call_id)?;
                self.refuse_if_shutting_down()?;
                let from = leg.unwrap_or(Side::A);
                let offer = engine.offer(&call_id, &sdp, from, webrtc).map_err(fail)?;
                if offer.call.created {
                    self.publish(&call_id, "call.created", json!({ "call": offer.call }));
                }
                let offering = Place::Call { call_id, side: from };
                if record && !engine.is_recording(&offering) {
                    self.start_recording(&offering)?;
                }
                Ok(to_value(&offer))
            }
            Command::Answer { call_id, sdp, leg } => {
                check_id("call_id", &call_id)?;
                engine.answer(&call_id, &sdp, leg).map(|a| to_value(&a)).map_err(fail)
            }
            Command::Reoffer { conference_id, participant_id, sdp } => {
                check_id("conference_id", &conference_id)?;
                check_id("participant_id", &participant_id)?;
                engine.reoffer(&conference_id, &participant_id, &sdp).map(|r| to_value(&r)).map_err(fail)
            }
            Command::Create { call_id, a, b } => {
                check_id("call_id", &call_id)?;
                self.refuse_if_shutting_down()?;
                let info = engine.create(&call_id, a.remote, b.remote).map_err(fail)?;
                if info.created {
                    self.publish(&call_id, "call.created", json!({ "call": info }));
                }
                Ok(to_value(&info))
            }
            Command::Update { call_id, a, b } => {
                check_id("call_id", &call_id)?;
                engine.update(&call_id, a.remote, b.remote).map(|s| to_value(&s)).map_err(fail)
            }
            Command::Delete { call_id } => {
                check_id("call_id", &call_id)?;
                self.end_call(&call_id, EndReason::Deleted).map_err(fail)
            }
            Command::Query { call_id } => {
                check_id("call_id", &call_id)?;
                engine.query_call(&call_id).map(|s| to_value(&s)).map_err(fail)
            }
            Command::MoveToConference { call_id, conference_id, a_participant_id, b_participant_id } => {
                check_id("call_id", &call_id)?;
                check_id("conference_id", &conference_id)?;
                let a_id = a_participant_id.unwrap_or_else(|| format!("{call_id}-a"));
                let b_id = b_participant_id.unwrap_or_else(|| format!("{call_id}-b"));
                check_id("a_participant_id", &a_id)?;
                check_id("b_participant_id", &b_id)?;
                let created = engine.move_call(&call_id, &conference_id, &a_id, &b_id).map_err(fail)?;
                if created {
                    self.publish(&conference_id, "conference.created", json!({ "conference_id": conference_id }));
                }
                let body = json!({ "call_id": call_id, "conference_id": conference_id, "participants": [a_id, b_id] });
                self.publish(&call_id, "call.moved", body.clone());
                Ok(body)
            }

            Command::Join { conference_id, participant_id, sdp, record, muted } => {
                check_id("conference_id", &conference_id)?;
                check_id("participant_id", &participant_id)?;
                self.refuse_if_shutting_down()?;
                let joined = engine.join(&conference_id, &participant_id, &sdp, muted).map_err(fail)?;
                if joined.conference_created {
                    self.publish(&conference_id, "conference.created", json!({ "conference_id": conference_id }));
                }
                let body = json!({ "conference_id": conference_id, "participant_id": participant_id, "codec": joined.codec, "muted": muted });
                self.publish(&conference_id, "participant.joined", body);
                if record {
                    self.start_recording(&Place::Conference { conference_id, participant_id })?;
                }
                Ok(to_value(&joined))
            }
            Command::Leave { conference_id, participant_id } => {
                check_id("conference_id", &conference_id)?;
                check_id("participant_id", &participant_id)?;
                let left = engine.leave(&conference_id, &participant_id).map_err(fail)?;
                self.announce_ended(&left.ended, EndReason::Left);
                if left.conference_ended {
                    let body = json!({ "conference_id": conference_id, "reason": "empty" });
                    self.publish(&conference_id, "conference.ended", body);
                }
                Ok(to_value(&left.ended.stats))
            }
            Command::ConferenceQuery { conference_id } => {
                check_id("conference_id", &conference_id)?;
                engine.query_conference(&conference_id).map(|s| to_value(&s)).map_err(fail)
            }
            Command::ConferenceDelete { conference_id } => {
                check_id("conference_id", &conference_id)?;
                let ended = engine.delete_conference(&conference_id).map_err(fail)?;
                for e in &ended {
                    self.announce_ended(e, EndReason::Deleted);
                }
                self.publish(&conference_id, "conference.ended", json!({ "conference_id": conference_id, "reason": "deleted" }));
                let stats: Vec<Value> = ended.iter().map(|e| to_value(&e.stats)).collect();
                Ok(json!({ "conference_id": conference_id, "participants": stats }))
            }
            Command::ConferenceMerge { from, into } => {
                check_id("from", &from)?;
                check_id("into", &into)?;
                let moved = engine.merge(&from, &into).map_err(fail)?;
                let body = json!({ "from": from, "into": into, "participants": moved });
                self.publish(&into, "conference.merged", body.clone());
                self.publish(&from, "conference.ended", json!({ "conference_id": from, "reason": "merged", "into": into }));
                Ok(body)
            }

            Command::StartRecording { target } => {
                let place = target.place(Some(Side::A))?;
                self.start_recording(&place).map(|r| to_value(&r))
            }
            Command::StopRecording { target } => {
                let place = target.place(Some(Side::A))?;
                let status = engine.stop_recording(&place).map_err(fail)?;
                self.publish_recording_stopped(&place, &status, "requested");
                Ok(to_value(&status))
            }
            Command::SetMode { conference_id, participant_id, mode, whisper_to } => {
                check_id("conference_id", &conference_id)?;
                check_id("participant_id", &participant_id)?;
                match (mode, &whisper_to) {
                    (ParticipantMode::Whisper, None) => return Err((ErrorCode::BadRequest, "whisper needs whisper_to".into())),
                    (ParticipantMode::Listen | ParticipantMode::Barge, Some(_)) => {
                        return Err((ErrorCode::BadRequest, "whisper_to is only for whisper".into()));
                    }
                    _ => {}
                }
                engine.set_mode(&conference_id, &participant_id, mode, whisper_to.as_deref()).map_err(fail)?;
                let mut body = json!({ "conference_id": conference_id, "participant_id": participant_id, "mode": mode });
                if let Some(to) = &whisper_to {
                    body["whisper_to"] = json!(to);
                }
                self.publish(&conference_id, "participant.mode_changed", body.clone());
                Ok(body)
            }
            Command::Mute { target } => self.set_muted(target, true),
            Command::Unmute { target } => self.set_muted(target, false),
            Command::Play { target, file } => {
                let place = target.place(None)?;
                let status = engine.play(&place, &file).map_err(fail)?;
                let mut body = place.fields();
                body["file"] = json!(status.file);
                body["duration_ms"] = json!(status.duration_ms);
                self.publish(place.key(), "play.started", body.clone());
                Ok(body)
            }
            Command::StopPlay { target } => {
                let place = target.place(None)?;
                let playing = engine.stop_play(&place).map_err(fail)?;
                Ok(json!({ "stopped": playing.is_some(), "file": playing.map(|p| p.file) }))
            }
            Command::SendDtmf { target, digits, duration_ms, pause_ms } => {
                let place = target.place(None)?;
                check_digits(&digits, duration_ms, pause_ms)?;
                let (duration, pause) = (Duration::from_millis(duration_ms), Duration::from_millis(pause_ms));
                let (queued, method) = engine.send_dtmf(&place, &digits, duration, pause).map_err(fail)?;
                Ok(json!({ "queued": queued, "method": method }))
            }
        }
    }

    fn set_muted(&self, target: Target, muted: bool) -> Result<Value, Failure> {
        let place = target.place(None)?;
        let changed = self.engine.set_muted(&place, muted).map_err(fail)?;
        if changed {
            self.publish(place.key(), if muted { "leg.muted" } else { "leg.unmuted" }, place.fields());
        }
        let mut body = place.fields();
        body["muted"] = json!(muted);
        body["changed"] = json!(changed);
        Ok(body)
    }

    fn refuse_if_shutting_down(&self) -> Result<(), Failure> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err((ErrorCode::Unavailable, "shutting down".into()));
        }
        Ok(())
    }

    fn start_recording(&self, place: &Place) -> Result<RecordingStatus, Failure> {
        let status = self.engine.start_recording(place).map_err(fail)?;
        let mut body = place.fields();
        body["recording"] = to_value(&status);
        self.publish(place.key(), "recording.started", body);
        Ok(status)
    }

    fn publish_recording_stopped(&self, place: &Place, status: &RecordingStatus, reason: &str) {
        let mut body = place.fields();
        body["reason"] = reason.into();
        body["recording"] = to_value(status);
        self.publish(place.key(), "recording.stopped", body);
    }

    /// Ends a call, whatever the reason, and publishes what happened.
    pub fn end_call(&self, call_id: &str, reason: EndReason) -> Result<Value, Error> {
        let (stats, ended) = self.engine.delete_call(call_id)?;
        tracing::info!(call_id, ?reason, age_ms = stats.age_ms, "call ended");
        for e in &ended {
            if let Some(status) = &e.recording {
                self.publish_recording_stopped(&e.place, status, "ended");
            }
        }
        let stats = to_value(&stats);
        self.publish(call_id, "call.ended", json!({ "call_id": call_id, "reason": reason, "call": stats }));
        Ok(stats)
    }

    /// Removes a conference participant (e.g. idle) and publishes what happened.
    pub fn end_participant(&self, conference_id: &str, participant_id: &str, reason: EndReason) {
        if let Ok(left) = self.engine.leave(conference_id, participant_id) {
            self.announce_ended(&left.ended, reason);
            if left.conference_ended {
                self.publish(conference_id, "conference.ended", json!({ "conference_id": conference_id, "reason": "empty" }));
            }
        }
    }

    /// Publishes the end of a leg: its recording, and for conferences `participant.left`.
    fn announce_ended(&self, ended: &Ended, reason: EndReason) {
        if let Some(status) = &ended.recording {
            self.publish_recording_stopped(&ended.place, status, "ended");
        }
        if let Place::Conference { .. } = &ended.place {
            let mut body = ended.place.fields();
            body["reason"] = to_value(&reason);
            body["participant"] = to_value(&ended.stats);
            self.publish(ended.place.key(), "participant.left", body);
        }
    }

    /// Ends everything at shutdown, publishing the usual events.
    pub fn end_all(&self) {
        let ended = self.engine.end_all();
        let mut calls: Vec<String> = Vec::new();
        let mut conferences: Vec<String> = Vec::new();
        for e in &ended {
            self.announce_ended(e, EndReason::Shutdown);
            match &e.place {
                Place::Call { call_id, .. } if !calls.contains(call_id) => calls.push(call_id.clone()),
                Place::Conference { conference_id, .. } if !conferences.contains(conference_id) => {
                    conferences.push(conference_id.clone())
                }
                _ => {}
            }
        }
        for id in calls {
            self.publish(&id, "call.ended", json!({ "call_id": id, "reason": EndReason::Shutdown }));
        }
        for id in conferences {
            self.publish(&id, "conference.ended", json!({ "conference_id": id, "reason": "shutdown" }));
        }
    }

    fn publish(&self, key: &str, kind: &str, body: Value) {
        self.events.publish(key, &event(&self.node_id, kind, body));
    }

    pub fn error(&self, id: Option<Value>, code: ErrorCode, message: String) -> Response {
        Response { id, node: self.node_id.clone(), ok: false, result: None, error: Some(ApiError { code, message }) }
    }
}

impl Target {
    /// The leg this names. `default_leg` is used for calls when `leg` is missing.
    fn place(self, default_leg: Option<Side>) -> Result<Place, Failure> {
        let bad = |m: &str| Err((ErrorCode::BadRequest, m.to_string()));
        match (self.call_id, self.conference_id, self.participant_id) {
            (Some(call_id), None, None) => {
                check_id("call_id", &call_id)?;
                match self.leg.or(default_leg) {
                    Some(side) => Ok(Place::Call { call_id, side }),
                    None => bad("leg (\"a\" or \"b\") is required"),
                }
            }
            (None, Some(conference_id), Some(participant_id)) if self.leg.is_none() => {
                check_id("conference_id", &conference_id)?;
                check_id("participant_id", &participant_id)?;
                Ok(Place::Conference { conference_id, participant_id })
            }
            _ => bad("name the leg with call_id (and leg), or with conference_id and participant_id"),
        }
    }
}

/// An event as published on the events topic: `body` plus `type`, `node` and `at_ms`.
pub fn event(node_id: &str, kind: &str, mut body: Value) -> Value {
    let obj = body.as_object_mut().expect("event body is an object");
    obj.insert("type".into(), kind.into());
    obj.insert("node".into(), node_id.into());
    obj.insert("at_ms".into(), unix_ms().into());
    body
}

fn default_digit_ms() -> u64 {
    100
}

fn default_pause_ms() -> u64 {
    70
}

fn check_digits(digits: &str, duration_ms: u64, pause_ms: u64) -> Result<(), Failure> {
    let bad = |m: String| Err((ErrorCode::BadRequest, m));
    if digits.is_empty() || digits.chars().count() > MAX_DIGITS {
        return bad(format!("digits must be 1..={MAX_DIGITS} characters"));
    }
    if let Some(c) = digits.chars().find(|&c| char_to_event(c).is_none()) {
        return bad(format!("{c:?} is not a DTMF digit (0-9, *, #, A-D)"));
    }
    if !(40..=2000).contains(&duration_ms) {
        return bad("duration_ms must be 40..=2000".into());
    }
    if !(20..=2000).contains(&pause_ms) {
        return bad("pause_ms must be 20..=2000".into());
    }
    Ok(())
}

fn check_id(field: &str, id: &str) -> Result<(), Failure> {
    if id.is_empty() || id.len() > MAX_ID_LEN || id.chars().any(char::is_control) {
        return Err((ErrorCode::BadRequest, format!("{field} must be 1..={MAX_ID_LEN} bytes without control characters")));
    }
    Ok(())
}

fn fail(e: Error) -> Failure {
    (ErrorCode::from(&e), e.to_string())
}

fn to_value<T: Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("response types serialize")
}
