//! Recordings, and the IO thread that writes them.
//!
//! A recording belongs to a leg (a call leg or a conference participant) and follows it
//! when it moves, e.g. into a conference or through a merge.
//! - **Channels:** left is what the leg sends (the participant's voice), right is what it
//!   receives (what the participant hears).
//! - **Audio:** Sun `.au`, 16-bit big-endian linear PCM, 8 kHz, stereo.
//!
//! Recordings are timestamped:
//! - **In the `.au` header:** the annotation field holds the wall-clock start time
//!   (RFC 3339 and Unix milliseconds), the format and the leg. So the audio file alone says
//!   when it began.
//! - **In a timeline next to it** (`<name>.json`, written when the recording ends):
//!   - wall-clock anchors every 5 s, linking frame numbers (20 ms each) to real time;
//!   - events with their offset and time: moves into a conference or a merge, DTMF digits,
//!     and `silence` where frames the RTP thread missed were filled in.
//!
//! The leg's RTP thread hands both channels to the IO thread 20 ms at a time, stamped with
//! their wall-clock time, so the channels are aligned by construction. The IO thread does
//! all the file writing. It handles messages as they come and runs a cycle of its own
//! (`--cycle-io-ms`), which the watchdog checks.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};

use crate::clock::{rfc3339, unix_ms};
use crate::codec::{FRAME_SAMPLES, PCM_RATE};
use crate::cycle::{CycleMeter, Registry, ThreadStats};
use crate::sched::Policy;

pub type Frame = [i16; FRAME_SAMPLES];

const CHANNELS: u32 = 2;
const BYTES_PER_FRAME: u64 = FRAME_SAMPLES as u64 * CHANNELS as u64 * 2;
const FRAME_MS: u64 = FRAME_SAMPLES as u64 * 1000 / PCM_RATE as u64;
/// A wall-clock anchor every 250 frames (5 s).
const ANCHOR_FRAMES: u64 = 250;
const AU_MAGIC: u32 = 0x2e73_6e64; // ".snd"
const AU_MIN_HEADER: usize = 24;
const AU_LINEAR_16: u32 = 3;
const AU_UNKNOWN_SIZE: u32 = 0xFFFF_FFFF;

/// Sun/NeXT audio header with an annotation. Fields are big-endian. The annotation is
/// NUL-terminated and padded so the audio starts on an 8-byte boundary.
pub fn au_header(data_size: u32, annotation: &str) -> Vec<u8> {
    let text = annotation.as_bytes();
    let len = (AU_MIN_HEADER + text.len() + 1).div_ceil(8) * 8;
    let mut h = Vec::with_capacity(len);
    for v in [AU_MAGIC, len as u32, data_size, AU_LINEAR_16, PCM_RATE, CHANNELS] {
        h.extend_from_slice(&v.to_be_bytes());
    }
    h.extend_from_slice(text);
    h.resize(len, 0);
    h
}

/// The annotation of an `.au` file, if it has one.
pub fn au_annotation(file: &[u8]) -> Option<String> {
    let offset = u32::from_be_bytes(file.get(4..8)?.try_into().ok()?) as usize;
    let text = file.get(AU_MIN_HEADER..offset)?;
    let end = text.iter().position(|&b| b == 0).unwrap_or(text.len());
    String::from_utf8(text[..end].to_vec()).ok()
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RecordingStatus {
    pub path: String,
    /// The timeline file (`.json` next to the audio), written when the recording ends.
    pub timeline: String,
    pub active: bool,
    pub started_at: String,
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub bytes: u64,
}

enum IoMsg {
    Open { id: u64, file: File, path: PathBuf, label: String, started_at_ms: u64 },
    Frame { id: u64, at_ms: u64, left: Box<Frame>, right: Box<Frame> },
    /// `frames` frames of silence on both channels, the last of them ending at `at_ms`.
    Silence { id: u64, frames: u32, at_ms: u64, reason: &'static str },
    Event { id: u64, frame: u64, at_ms: u64, kind: String, data: Value },
    Close { id: u64, reply: Option<Sender<io::Result<RecordingStatus>>> },
    Shutdown,
}

/// The IO thread.
pub struct IoThread {
    tx: Sender<IoMsg>,
    handle: Option<JoinHandle<()>>,
}

impl IoThread {
    pub fn start(policy: Policy, cycle: Duration, registry: &Registry) -> io::Result<IoThread> {
        let (tx, rx) = mpsc::channel();
        let stats = registry.register("io", cycle, true);
        let handle = std::thread::Builder::new().name("io".into()).spawn(move || {
            policy.apply_or_warn("io");
            io_loop(rx, stats)
        })?;
        Ok(IoThread { tx, handle: Some(handle) })
    }

    pub fn handle(&self) -> IoHandle {
        IoHandle { tx: self.tx.clone(), next_id: Arc::new(AtomicU64::new(1)) }
    }

    /// Finishes every open file and stops the thread.
    pub fn shutdown(mut self) {
        let _ = self.tx.send(IoMsg::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Starts recordings; cheap to clone.
#[derive(Clone)]
pub struct IoHandle {
    tx: Sender<IoMsg>,
    next_id: Arc<AtomicU64>,
}

impl IoHandle {
    /// Creates `<dir>/<name>.au` with its header, here, so errors come back right away.
    /// `label` names the leg in the header and the timeline. The returned recording is then
    /// written by the leg's thread.
    pub fn create(&self, dir: &Path, name: &str, label: &str) -> io::Result<Recording> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{}.au", file_safe(name)));
        let started_at_ms = unix_ms();
        let annotation = format!(
            "start={};start_ms={started_at_ms};rate={PCM_RATE};channels={CHANNELS};left=sent;right=heard;leg={}",
            rfc3339(started_at_ms),
            label.replace(['\0', ';'], "_"),
        );
        let mut file = File::options().write(true).create_new(true).open(&path)?;
        file.write_all(&au_header(AU_UNKNOWN_SIZE, &annotation))?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let open = IoMsg::Open { id, file, path: path.clone(), label: label.to_string(), started_at_ms };
        self.tx.send(open).map_err(|_| io::Error::other("io thread stopped"))?;
        tracing::info!(path = %path.display(), "recording started");
        Ok(Recording { id, path, tx: self.tx.clone(), frames: 0, started_at_ms, closed: false })
    }
}

/// An open recording, owned by the leg's RTP thread.
pub struct Recording {
    id: u64,
    path: PathBuf,
    tx: Sender<IoMsg>,
    frames: u64,
    started_at_ms: u64,
    closed: bool,
}

impl Recording {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 20 ms of both channels, captured at `at_ms` (Unix milliseconds).
    pub fn write(&mut self, left: &Frame, right: &Frame, at_ms: u64) {
        let msg = IoMsg::Frame { id: self.id, at_ms, left: Box::new(*left), right: Box::new(*right) };
        if self.tx.send(msg).is_ok() {
            self.frames += 1;
        }
    }

    /// Private API: adds `frames` frames of silence on both channels, for frames that were
    /// never captured, the last ending at `at_ms`. The timeline gets a `silence` event
    /// with `reason`, so the recording keeps to wall-clock time and says where it was filled.
    pub(crate) fn add_silence(&mut self, frames: u32, at_ms: u64, reason: &'static str) {
        if frames == 0 {
            return;
        }
        if self.tx.send(IoMsg::Silence { id: self.id, frames, at_ms, reason }).is_ok() {
            self.frames += frames as u64;
        }
    }

    /// Marks something that happened now in the timeline (e.g. `moved`, `dtmf`).
    pub fn event(&mut self, kind: &str, data: Value) {
        let msg = IoMsg::Event { id: self.id, frame: self.frames, at_ms: unix_ms(), kind: kind.to_string(), data };
        let _ = self.tx.send(msg);
    }

    pub fn status(&self) -> RecordingStatus {
        RecordingStatus {
            path: self.path.display().to_string(),
            timeline: timeline_path(&self.path).display().to_string(),
            active: !self.closed,
            started_at: rfc3339(self.started_at_ms),
            started_at_ms: self.started_at_ms,
            duration_ms: self.frames * FRAME_MS,
            bytes: self.frames * BYTES_PER_FRAME,
        }
    }

    /// Asks the IO thread to finish the file; the final status arrives on the receiver.
    pub fn close(mut self) -> Receiver<io::Result<RecordingStatus>> {
        let (reply, rx) = mpsc::channel();
        self.closed = true;
        if self.tx.send(IoMsg::Close { id: self.id, reply: Some(reply.clone()) }).is_err() {
            let _ = reply.send(Err(io::Error::other("io thread stopped")));
        }
        rx
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.tx.send(IoMsg::Close { id: self.id, reply: None });
        }
    }
}

fn timeline_path(audio: &Path) -> PathBuf {
    audio.with_extension("json")
}

struct Writer {
    out: BufWriter<File>,
    path: PathBuf,
    label: String,
    started_at_ms: u64,
    frames: u64,
    anchors: Vec<Value>,
    events: Vec<Value>,
    error: Option<io::Error>,
}

impl Writer {
    fn bytes(&self) -> u64 {
        self.frames * BYTES_PER_FRAME
    }

    fn finish(mut self) -> io::Result<RecordingStatus> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        self.out.flush()?;
        let mut file = self.out.into_inner().map_err(|e| e.into_error())?;
        let size = (self.frames * BYTES_PER_FRAME).min(AU_UNKNOWN_SIZE as u64 - 1) as u32;
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&size.to_be_bytes())?;
        file.sync_all()?;

        let stopped_at_ms = unix_ms();
        let duration_ms = self.frames * FRAME_MS;
        let timeline = json!({
            "recording": self.path.file_name().map(|n| n.to_string_lossy().into_owned()),
            "leg": self.label,
            "started_at": rfc3339(self.started_at_ms),
            "started_at_ms": self.started_at_ms,
            "stopped_at": rfc3339(stopped_at_ms),
            "stopped_at_ms": stopped_at_ms,
            "sample_rate": PCM_RATE,
            "channels": CHANNELS,
            "left": "sent",
            "right": "heard",
            "frame_ms": FRAME_MS,
            "frames": self.frames,
            "duration_ms": duration_ms,
            "anchors": self.anchors,
            "events": self.events,
        });
        let timeline_file = timeline_path(&self.path);
        std::fs::write(&timeline_file, serde_json::to_vec_pretty(&timeline).map_err(io::Error::other)?)?;

        let status = RecordingStatus {
            path: self.path.display().to_string(),
            timeline: timeline_file.display().to_string(),
            active: false,
            started_at: rfc3339(self.started_at_ms),
            started_at_ms: self.started_at_ms,
            duration_ms,
            bytes: self.frames * BYTES_PER_FRAME,
        };
        tracing::info!(path = %status.path, duration_ms, "recording stopped");
        Ok(status)
    }
}

impl Writer {
    /// One frame of interleaved big-endian samples, captured at `at_ms`.
    fn write_frame(&mut self, bytes: &[u8], at_ms: u64) {
        if self.error.is_some() {
            return;
        }
        if self.frames.is_multiple_of(ANCHOR_FRAMES) {
            self.anchors.push(json!({
                "frame": self.frames,
                "offset_ms": self.frames * FRAME_MS,
                "at_ms": at_ms,
                "at": rfc3339(at_ms),
            }));
        }
        match self.out.write_all(bytes) {
            Ok(()) => self.frames += 1,
            Err(e) => {
                tracing::error!(path = %self.path.display(), error = %e, "recording write failed");
                self.error = Some(e);
            }
        }
        debug_assert_eq!(self.bytes() % BYTES_PER_FRAME, 0);
    }

    fn event(&mut self, frame: u64, at_ms: u64, kind: &str, data: Value) {
        let mut event = json!({ "type": kind, "offset_ms": frame * FRAME_MS, "at_ms": at_ms, "at": rfc3339(at_ms) });
        if let (Some(e), Value::Object(extra)) = (event.as_object_mut(), data) {
            e.extend(extra);
        }
        self.events.push(event);
    }
}

fn io_loop(rx: Receiver<IoMsg>, stats: Arc<ThreadStats>) {
    let mut meter = CycleMeter::new(stats, Instant::now(), 0);
    let mut files: HashMap<u64, Writer> = HashMap::new();
    let mut bytes = Vec::with_capacity(BYTES_PER_FRAME as usize);
    loop {
        let now = Instant::now();
        meter.sleep(now);
        let msg = rx.recv_timeout(meter.deadline().saturating_duration_since(now));
        meter.wake(Instant::now());
        match msg {
            Ok(IoMsg::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
            Ok(msg) => handle(msg, &mut files, &mut bytes),
            Err(RecvTimeoutError::Timeout) => {}
        }
        let now = Instant::now();
        if meter.is_due(now) {
            meter.start_cycle(now);
            meter.load_changed();
        }
    }
    for (_, w) in files.drain() {
        if let Err(e) = w.finish() {
            tracing::warn!(error = %e, "recording did not finish cleanly");
        }
    }
}

fn handle(msg: IoMsg, files: &mut HashMap<u64, Writer>, bytes: &mut Vec<u8>) {
    match msg {
        IoMsg::Open { id, file, path, label, started_at_ms } => {
            let w = Writer {
                out: BufWriter::new(file),
                path,
                label,
                started_at_ms,
                frames: 0,
                anchors: Vec::new(),
                events: Vec::new(),
                error: None,
            };
            files.insert(id, w);
        }
        IoMsg::Frame { id, at_ms, left, right } => {
            let Some(w) = files.get_mut(&id) else { return };
            bytes.clear();
            for (l, r) in left.iter().zip(right.iter()) {
                bytes.extend_from_slice(&l.to_be_bytes());
                bytes.extend_from_slice(&r.to_be_bytes());
            }
            w.write_frame(bytes, at_ms);
        }
        IoMsg::Silence { id, frames, at_ms, reason } => {
            let Some(w) = files.get_mut(&id) else { return };
            let first_ms = at_ms.saturating_sub((frames as u64 - 1) * FRAME_MS);
            let data = json!({ "frames": frames, "duration_ms": frames as u64 * FRAME_MS, "reason": reason });
            w.event(w.frames, first_ms, "silence", data);
            bytes.clear();
            bytes.resize(BYTES_PER_FRAME as usize, 0);
            for i in 0..frames as u64 {
                w.write_frame(bytes, first_ms + i * FRAME_MS);
            }
        }
        IoMsg::Event { id, frame, at_ms, kind, data } => {
            if let Some(w) = files.get_mut(&id) {
                w.event(frame, at_ms, &kind, data);
            }
        }
        IoMsg::Close { id, reply } => {
            let result = match files.remove(&id) {
                Some(w) => w.finish(),
                None => Err(io::Error::other("recording not open")),
            };
            if let Some(reply) = reply {
                let _ = reply.send(result);
            } else if let Err(e) = result {
                tracing::warn!(error = %e, "recording did not finish cleanly");
            }
        }
        IoMsg::Shutdown => {}
    }
}

pub fn file_safe(name: &str) -> String {
    let safe: String = name
        .chars()
        .take(150)
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') { c } else { '_' })
        .collect();
    if safe.starts_with('.') { format!("_{safe}") } else { safe }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_carries_the_annotation_and_stays_aligned() {
        let h = au_header(320, "start=2026-10-06T09:00:00.000Z");
        assert_eq!(&h[0..4], b".snd");
        let field = |i: usize| u32::from_be_bytes(h[i * 4..i * 4 + 4].try_into().unwrap());
        assert_eq!((field(2), field(3), field(4), field(5)), (320, 3, 8000, 2));
        assert_eq!(field(1) as usize, h.len());
        assert_eq!(h.len() % 8, 0);
        assert_eq!(au_annotation(&h).as_deref(), Some("start=2026-10-06T09:00:00.000Z"));
    }

    #[test]
    fn file_names_are_safe() {
        assert_eq!(file_safe("abc-123@sip.example.com"), "abc-123_sip.example.com");
        assert_eq!(file_safe("../../etc/passwd"), "_.._.._etc_passwd");
    }

    #[test]
    fn the_io_thread_writes_stereo_timestamps_and_a_timeline() {
        let dir = std::env::temp_dir().join(format!("media-server-io-{}", std::process::id()));
        let io = IoThread::start(Policy::Inherit, Duration::from_millis(100), &Registry::new()).unwrap();
        let mut rec = io.handle().create(&dir, "unit-1", "call-1/a").unwrap();
        let t0 = 1_791_274_833_000;
        for i in 0..3 {
            rec.write(&[1000 + i; FRAME_SAMPLES], &[-2000; FRAME_SAMPLES], t0 + i as u64 * 20);
        }
        rec.event("dtmf", json!({ "digit": "5" }));
        assert_eq!(rec.status().duration_ms, 60);
        let path = rec.path().to_path_buf();
        let status = rec.close().recv().unwrap().unwrap();
        assert_eq!((status.active, status.bytes, status.duration_ms), (false, 3 * BYTES_PER_FRAME, 60));
        assert!(status.started_at.ends_with('Z'));

        let data = std::fs::read(&path).unwrap();
        let offset = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        assert_eq!(u32::from_be_bytes(data[8..12].try_into().unwrap()) as u64, status.bytes);
        assert_eq!(data.len(), offset + status.bytes as usize);
        let annotation = au_annotation(&data).unwrap();
        assert!(annotation.starts_with(&format!("start={};start_ms={}", status.started_at, status.started_at_ms)), "{annotation}");
        assert!(annotation.ends_with("left=sent;right=heard;leg=call-1/a"), "{annotation}");
        let samples: Vec<i16> = data[offset..].chunks_exact(2).map(|b| i16::from_be_bytes([b[0], b[1]])).collect();
        assert_eq!(&samples[..4], &[1000, -2000, 1000, -2000], "left, right, left, right");

        let timeline: Value = serde_json::from_slice(&std::fs::read(&status.timeline).unwrap()).unwrap();
        assert_eq!(timeline["leg"], "call-1/a");
        assert_eq!(timeline["frames"], 3);
        assert_eq!(timeline["anchors"][0]["at_ms"], t0, "frame 0 is anchored to its capture time");
        assert_eq!(timeline["events"][0]["type"], "dtmf");
        assert_eq!(timeline["events"][0]["offset_ms"], 60);
        assert_eq!(timeline["events"][0]["digit"], "5");

        // Missed frames are filled with silence: the audio keeps to wall-clock time, the
        // timeline says where.
        let mut gap = io.handle().create(&dir, "unit-gap", "call-2/a").unwrap();
        gap.write(&[7; FRAME_SAMPLES], &[7; FRAME_SAMPLES], t0);
        gap.add_silence(250, t0 + 250 * 20, "rtp thread behind");
        gap.write(&[9; FRAME_SAMPLES], &[9; FRAME_SAMPLES], t0 + 251 * 20);
        assert_eq!(gap.status().duration_ms, 252 * 20);
        let gap_path = gap.path().to_path_buf();
        let status = gap.close().recv().unwrap().unwrap();
        let data = std::fs::read(&gap_path).unwrap();
        let offset = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        let samples: Vec<i16> = data[offset..].chunks_exact(2).map(|b| i16::from_be_bytes([b[0], b[1]])).collect();
        let per_frame = 2 * FRAME_SAMPLES;
        assert_eq!(samples.len(), 252 * per_frame);
        assert!(samples[per_frame..251 * per_frame].iter().all(|&s| s == 0), "the gap is silence");
        assert_eq!(samples[251 * per_frame], 9);
        let timeline: Value = serde_json::from_slice(&std::fs::read(&status.timeline).unwrap()).unwrap();
        let silence = &timeline["events"][0];
        assert_eq!((silence["type"].as_str(), silence["frames"].as_u64()), (Some("silence"), Some(250)));
        assert_eq!((silence["offset_ms"].as_u64(), silence["at_ms"].as_u64()), (Some(20), Some(t0 + 20)));
        assert_eq!(timeline["anchors"][1]["frame"], 250, "anchors count silent frames too");
        assert_eq!(timeline["anchors"][1]["at_ms"], t0 + 250 * 20);

        // A recording still open at shutdown is finished too.
        let mut open = io.handle().create(&dir, "unit-2", "x").unwrap();
        open.write(&[1; FRAME_SAMPLES], &[2; FRAME_SAMPLES], t0);
        let open_path = open.path().to_path_buf();
        std::mem::forget(open);
        io.shutdown();
        let data = std::fs::read(&open_path).unwrap();
        assert_eq!(u32::from_be_bytes(data[8..12].try_into().unwrap()) as u64, BYTES_PER_FRAME);
        assert!(timeline_path(&open_path).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
