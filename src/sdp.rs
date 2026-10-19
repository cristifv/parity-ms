//! Just enough SDP (RFC 4566 / 3264) to negotiate one audio stream: the connection address,
//! the first `m=audio` line, its payload types, and the direction.
//!
//! WebRTC (`UDP/TLS/RTP/SAVPF`) adds the ICE credentials, the DTLS fingerprint and setup
//! role, `rtcp-mux` and `mid` (RFC 8829, 8842). Every `m=` line is kept, so an answer can
//! reject the ones we don't take (video, data channels) in their place.

use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};

use serde::Serialize;

use crate::codec::Codec;

pub const TELEPHONE_EVENT: &str = "telephone-event";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Payload {
    pub pt: u8,
    pub name: String,
    pub clock_rate: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fmtp: Option<String>,
    /// The codec, when this server can decode and encode it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<Codec>,
}

impl Payload {
    /// A payload for one of our codecs, with its usual format parameters.
    pub fn for_codec(codec: Codec, pt: u8) -> Payload {
        Payload {
            pt,
            name: codec.sdp_name().to_string(),
            clock_rate: codec.clock_rate(),
            channels: codec.sdp_channels(),
            fmtp: (codec == Codec::Opus).then(|| "minptime=10".to_string()),
            codec: Some(codec),
        }
    }

    /// RFC 4733 `telephone-event` at `clock_rate`, for the 16 DTMF digits.
    pub fn telephone_event(pt: u8, clock_rate: u32) -> Payload {
        Payload {
            pt,
            name: TELEPHONE_EVENT.to_string(),
            clock_rate,
            channels: None,
            fmtp: Some("0-15".to_string()),
            codec: None,
        }
    }

    pub fn is_telephone_event(&self) -> bool {
        self.name.eq_ignore_ascii_case(TELEPHONE_EVENT)
    }

    /// An audio encoding (as opposed to `telephone-event`, comfort noise, or a payload type
    /// with no name).
    pub fn is_audio(&self) -> bool {
        !self.name.is_empty() && !self.is_telephone_event() && !self.name.eq_ignore_ascii_case("CN")
    }

    fn from_static(pt: u8) -> Payload {
        // Static assignments from RFC 3551 that phones commonly send without an rtpmap.
        let (name, clock_rate) = match pt {
            0 => ("PCMU", 8000),
            3 => ("GSM", 8000),
            8 => ("PCMA", 8000),
            9 => ("G722", 8000),
            13 => ("CN", 8000),
            18 => ("G729", 8000),
            _ => ("", 0),
        };
        Payload {
            pt,
            name: name.to_string(),
            clock_rate,
            channels: None,
            fmtp: None,
            codec: Codec::from_static_payload_type(pt),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    SendRecv,
    SendOnly,
    RecvOnly,
    Inactive,
}

impl Direction {
    fn attribute(self) -> &'static str {
        match self {
            Direction::SendRecv => "sendrecv",
            Direction::SendOnly => "sendonly",
            Direction::RecvOnly => "recvonly",
            Direction::Inactive => "inactive",
        }
    }

    /// The direction to answer with: the mirror image of the offer's.
    pub fn answer(self) -> Direction {
        match self {
            Direction::SendOnly => Direction::RecvOnly,
            Direction::RecvOnly => Direction::SendOnly,
            d => d,
        }
    }

    fn parse(attr: &str) -> Option<Direction> {
        match attr {
            "sendrecv" => Some(Direction::SendRecv),
            "sendonly" => Some(Direction::SendOnly),
            "recvonly" => Some(Direction::RecvOnly),
            "inactive" => Some(Direction::Inactive),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Audio {
    /// Where the endpoint receives RTP. `None` when the stream is disabled (port 0) or on
    /// hold the old way (`c=0.0.0.0`).
    pub addr: Option<SocketAddr>,
    /// In the order of the `m=` line, which is the order of preference.
    pub payloads: Vec<Payload>,
    pub direction: Direction,
    /// The audio stream's transport: `RTP/AVP`, or `UDP/TLS/RTP/SAVPF` for WebRTC.
    pub proto: String,
    /// ICE and DTLS parameters, when the stream is WebRTC.
    pub webrtc: Option<WebRtc>,
    pub mid: Option<String>,
    /// Every `m=` line, in order; `audio` is the one negotiated.
    pub media: Vec<MediaLine>,
    pub audio: usize,
}

/// One `m=` line, as offered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaLine {
    pub kind: String,
    pub proto: String,
    /// The format list, as written.
    pub formats: String,
    pub mid: Option<String>,
}

/// A WebRTC stream's ICE credentials and DTLS parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebRtc {
    pub ufrag: String,
    pub pwd: String,
    pub fingerprint: Fingerprint,
    pub setup: Setup,
    pub rtcp_mux: bool,
}

/// A certificate fingerprint (`a=fingerprint:sha-256 AB:CD:...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    /// Lower case, e.g. `sha-256`.
    pub algorithm: String,
    pub value: Vec<u8>,
}

impl Fingerprint {
    pub fn parse(attr: &str) -> Option<Fingerprint> {
        let (algorithm, hex) = attr.trim().split_once(' ')?;
        let value = hex
            .trim()
            .split(':')
            .map(|b| u8::from_str_radix(b, 16).ok())
            .collect::<Option<Vec<u8>>>()?;
        (!value.is_empty()).then(|| Fingerprint { algorithm: algorithm.to_ascii_lowercase(), value })
    }
}

impl std::fmt::Display for Fingerprint {
    /// As written in SDP: `sha-256 AB:CD:...`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex: Vec<String> = self.value.iter().map(|b| format!("{b:02X}")).collect();
        write!(f, "{} {}", self.algorithm, hex.join(":"))
    }
}

/// Who starts the DTLS handshake (RFC 4145, 5763): `active` connects, `passive` waits,
/// `actpass` (offers) lets the answerer choose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Setup {
    ActPass,
    Active,
    Passive,
}

impl Setup {
    fn parse(v: &str) -> Option<Setup> {
        match v.trim() {
            "actpass" => Some(Setup::ActPass),
            "active" => Some(Setup::Active),
            "passive" => Some(Setup::Passive),
            _ => None,
        }
    }

    pub fn attribute(self) -> &'static str {
        match self {
            Setup::ActPass => "actpass",
            Setup::Active => "active",
            Setup::Passive => "passive",
        }
    }
}

impl Audio {
    /// The stream is WebRTC: DTLS-SRTP with ICE.
    pub fn is_webrtc(&self) -> bool {
        self.webrtc.is_some()
    }

    /// Audio payloads with a codec this server handles, in order of preference.
    pub fn supported(&self) -> impl Iterator<Item = &Payload> {
        self.payloads.iter().filter(|p| p.codec.is_some())
    }

    /// The `telephone-event` payload at `clock_rate`, if offered.
    pub fn telephone_event(&self, clock_rate: u32) -> Option<&Payload> {
        self.payloads.iter().find(|p| p.is_telephone_event() && p.clock_rate == clock_rate)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid SDP: {0}")]
pub struct SdpError(String);

fn err<T>(m: impl Into<String>) -> Result<T, SdpError> {
    Err(SdpError(m.into()))
}

fn parse_connection(value: &str) -> Result<IpAddr, SdpError> {
    // c=IN IP4 203.0.113.7 (a multicast "/ttl" suffix is ignored)
    let mut parts = value.split_whitespace();
    let (Some("IN"), Some(_family), Some(addr)) = (parts.next(), parts.next(), parts.next()) else {
        return err(format!("bad c= line: {value}"));
    };
    let addr = addr.split('/').next().unwrap_or(addr);
    addr.parse().map_err(|_| SdpError(format!("bad connection address: {addr}")))
}

/// The WebRTC transport profiles (RFC 5764 §8): DTLS-SRTP over UDP.
fn is_webrtc_proto(proto: &str) -> bool {
    proto.eq_ignore_ascii_case("UDP/TLS/RTP/SAVPF") || proto.eq_ignore_ascii_case("UDP/TLS/RTP/SAVP")
}

/// Attributes that may be at session level, with media level taking precedence.
#[derive(Default, Clone)]
struct Transport {
    ufrag: Option<String>,
    pwd: Option<String>,
    fingerprint: Option<Fingerprint>,
    setup: Option<Setup>,
}

impl Transport {
    fn attribute(&mut self, name: &str, arg: &str) -> bool {
        match name {
            "ice-ufrag" => self.ufrag = Some(arg.trim().to_string()),
            "ice-pwd" => self.pwd = Some(arg.trim().to_string()),
            "fingerprint" => self.fingerprint = Fingerprint::parse(arg),
            "setup" => self.setup = Setup::parse(arg),
            _ => return false,
        }
        true
    }

    fn or(self, session: &Transport) -> Transport {
        Transport {
            ufrag: self.ufrag.or_else(|| session.ufrag.clone()),
            pwd: self.pwd.or_else(|| session.pwd.clone()),
            fingerprint: self.fingerprint.or_else(|| session.fingerprint.clone()),
            setup: self.setup.or(session.setup),
        }
    }
}

pub fn parse(sdp: &str) -> Result<Audio, SdpError> {
    #[derive(PartialEq)]
    enum Section {
        Session,
        Audio,
        Other,
    }
    let mut section = Section::Session;
    let mut audio: Option<usize> = None;
    let mut media: Vec<MediaLine> = Vec::new();
    let (mut session_ip, mut media_ip) = (None, None);
    let (mut session_dir, mut media_dir) = (None, None);
    let mut port = 0u16;
    let mut pts: Vec<u8> = Vec::new();
    let mut rtpmaps: Vec<(u8, String, u32, Option<u8>)> = Vec::new();
    let mut fmtps: Vec<(u8, String)> = Vec::new();
    let (mut session_transport, mut audio_transport) = (Transport::default(), Transport::default());
    let mut rtcp_mux = false;

    for raw in sdp.lines() {
        let line = raw.trim_end_matches('\r');
        let Some((key, value)) = line.split_once('=') else { continue };
        match key {
            "m" => {
                let fields: Vec<&str> = value.split_whitespace().collect();
                let [kind, p, proto, formats @ ..] = fields.as_slice() else {
                    return err(format!("bad m= line: {value}"));
                };
                media.push(MediaLine {
                    kind: kind.to_string(),
                    proto: proto.to_string(),
                    formats: formats.join(" "),
                    mid: None,
                });
                if *kind == "audio" && audio.is_none() {
                    port = p.split('/').next().unwrap_or(p).parse().map_err(|_| SdpError(format!("bad port: {p}")))?;
                    pts = formats
                        .iter()
                        .map(|f| f.parse::<u8>().map_err(|_| SdpError(format!("bad payload type: {f}"))))
                        .collect::<Result<_, _>>()?;
                    section = Section::Audio;
                    audio = Some(media.len() - 1);
                } else {
                    section = Section::Other;
                }
            }
            "c" => match section {
                Section::Session => session_ip = Some(parse_connection(value)?),
                Section::Audio => media_ip = Some(parse_connection(value)?),
                Section::Other => {}
            },
            "a" => {
                let (name, arg) = value.split_once(':').unwrap_or((value, ""));
                if name == "mid"
                    && section != Section::Session
                    && let Some(m) = media.last_mut()
                {
                    m.mid = Some(arg.trim().to_string());
                    continue;
                }
                match section {
                    Section::Session => {
                        if !session_transport.attribute(name, arg) && Direction::parse(name).is_some() {
                            session_dir = Direction::parse(name);
                        }
                    }
                    Section::Audio => match name {
                        dir if Direction::parse(dir).is_some() => media_dir = Direction::parse(dir),
                        "rtcp-mux" => rtcp_mux = true,
                        "rtpmap" => {
                            // a=rtpmap:111 opus/48000/2
                            let Some((pt, enc)) = arg.split_once(' ') else { return err(format!("bad rtpmap: {arg}")) };
                            let mut enc_parts = enc.trim().split('/');
                            let enc_name = enc_parts.next().unwrap_or_default().to_string();
                            let clock = enc_parts.next().and_then(|c| c.parse().ok());
                            let channels = enc_parts.next().and_then(|c| c.parse().ok());
                            match (pt.parse::<u8>(), clock) {
                                (Ok(pt), Some(clock)) => rtpmaps.push((pt, enc_name, clock, channels)),
                                _ => return err(format!("bad rtpmap: {arg}")),
                            }
                        }
                        "fmtp" => {
                            if let Some((pt, params)) = arg.split_once(' ')
                                && let Ok(pt) = pt.parse::<u8>()
                            {
                                fmtps.push((pt, params.trim().to_string()));
                            }
                        }
                        other => {
                            audio_transport.attribute(other, arg);
                        }
                    },
                    Section::Other => {}
                }
            }
            _ => {}
        }
    }

    let Some(audio) = audio else { return err("no m=audio line") };
    let payloads = pts
        .into_iter()
        .map(|pt| {
            let mut p = match rtpmaps.iter().find(|(p, ..)| *p == pt) {
                Some((_, name, clock, channels)) => Payload {
                    pt,
                    name: name.clone(),
                    clock_rate: *clock,
                    channels: *channels,
                    fmtp: None,
                    codec: Codec::from_rtpmap(name, *clock),
                },
                None => Payload::from_static(pt),
            };
            p.fmtp = fmtps.iter().find(|(f, _)| *f == pt).map(|(_, v)| v.clone());
            p
        })
        .collect();
    let proto = media[audio].proto.clone();
    let webrtc = if is_webrtc_proto(&proto) {
        let t = audio_transport.or(&session_transport);
        let (Some(ufrag), Some(pwd)) = (t.ufrag, t.pwd) else { return err("WebRTC audio without ICE credentials") };
        let Some(fingerprint) = t.fingerprint else { return err("WebRTC audio without a DTLS fingerprint") };
        Some(WebRtc { ufrag, pwd, fingerprint, setup: t.setup.unwrap_or(Setup::ActPass), rtcp_mux })
    } else {
        None
    };
    // WebRTC endpoints are reached through ICE; the address in c= says nothing reliable.
    let addr = if port == 0 || webrtc.is_some() {
        None
    } else {
        match media_ip.or(session_ip) {
            Some(ip) if ip.is_unspecified() => None,
            Some(ip) => Some(SocketAddr::new(ip, port)),
            None => return err("no c= connection address"),
        }
    };
    let mid = media[audio].mid.clone();
    Ok(Audio {
        addr,
        payloads,
        direction: media_dir.or(session_dir).unwrap_or(Direction::SendRecv),
        proto,
        webrtc,
        mid,
        media,
        audio,
    })
}

/// Writes an SDP with one audio stream sent and received at `ip:port`.
pub fn build(ip: IpAddr, port: u16, payloads: &[Payload], direction: Direction, session_id: u64, version: u64) -> String {
    describe(&Description { ip, port, payloads, direction, session_id, version, webrtc: None, answering: None })
}

/// Our side of a WebRTC stream, for the SDP we send.
#[derive(Debug, Clone)]
pub struct LocalWebRtc<'a> {
    pub ufrag: &'a str,
    pub pwd: &'a str,
    pub fingerprint: &'a Fingerprint,
    pub setup: Setup,
    pub mid: &'a str,
}

/// An SDP to write: one audio stream, plain RTP or WebRTC.
pub struct Description<'a> {
    pub ip: IpAddr,
    pub port: u16,
    pub payloads: &'a [Payload],
    pub direction: Direction,
    pub session_id: u64,
    pub version: u64,
    pub webrtc: Option<LocalWebRtc<'a>>,
    /// The offer being answered: its other `m=` lines are rejected, in their places.
    pub answering: Option<&'a Audio>,
}

/// Writes an SDP: the audio stream (plain, or WebRTC with ICE-lite, DTLS and `rtcp-mux`),
/// and, when answering, every other offered `m=` line rejected (port 0).
pub fn describe(d: &Description<'_>) -> String {
    let (ip, port) = (d.ip, d.port);
    let family = if ip.is_ipv4() { "IP4" } else { "IP6" };
    let pts: Vec<String> = d.payloads.iter().map(|p| p.pt.to_string()).collect();
    let mut s = String::new();
    let _ = write!(s, "v=0\r\no=- {} {} IN {family} {ip}\r\ns=media-server\r\n", d.session_id, d.version);
    let _ = write!(s, "c=IN {family} {ip}\r\nt=0 0\r\n");
    if let Some(w) = &d.webrtc {
        let _ = write!(s, "a=ice-lite\r\na=group:BUNDLE {}\r\n", w.mid);
    }
    let lines: Vec<(usize, &MediaLine)> = d.answering.map(|o| o.media.iter().enumerate().collect()).unwrap_or_default();
    let audio_at = d.answering.map_or(0, |o| o.audio);
    for (_, m) in lines.iter().filter(|(i, _)| *i < audio_at) {
        reject(&mut s, m);
    }
    let proto = if d.webrtc.is_some() { "UDP/TLS/RTP/SAVPF" } else { "RTP/AVP" };
    let _ = write!(s, "m=audio {port} {proto} {}\r\n", pts.join(" "));
    if let Some(w) = &d.webrtc {
        let _ = write!(s, "a=mid:{}\r\na=ice-ufrag:{}\r\na=ice-pwd:{}\r\n", w.mid, w.ufrag, w.pwd);
        let _ = write!(s, "a=fingerprint:{}\r\na=setup:{}\r\na=rtcp-mux\r\n", w.fingerprint, w.setup.attribute());
    }
    for p in d.payloads {
        let _ = write!(s, "a=rtpmap:{} {}/{}", p.pt, p.name, p.clock_rate);
        if let Some(ch) = p.channels {
            let _ = write!(s, "/{ch}");
        }
        s.push_str("\r\n");
        if let Some(fmtp) = &p.fmtp {
            let _ = write!(s, "a=fmtp:{} {fmtp}\r\n", p.pt);
        }
    }
    let _ = write!(s, "a=ptime:20\r\na={}\r\n", d.direction.attribute());
    if d.webrtc.is_some() {
        // ICE-lite: one host candidate, our public address.
        let _ = write!(s, "a=candidate:1 1 udp 2130706431 {ip} {port} typ host\r\na=end-of-candidates\r\n");
    }
    for (_, m) in lines.iter().filter(|(i, _)| *i > audio_at) {
        reject(&mut s, m);
    }
    s
}

/// An offered `m=` line we don't take: port 0, same formats, same `mid`.
fn reject(s: &mut String, m: &MediaLine) {
    let _ = write!(s, "m={} 0 {} {}\r\n", m.kind, m.proto, m.formats);
    if let Some(mid) = &m.mid {
        let _ = write!(s, "a=mid:{mid}\r\n");
    }
    s.push_str("a=inactive\r\n");
}

/// The lowest dynamic payload type (96–127) not already taken.
pub fn free_dynamic_pt(taken: &[Payload]) -> Option<u8> {
    (96..=127).find(|pt| !taken.iter().any(|p| p.pt == *pt))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CARRIER: &str = "v=0\r\no=- 1 1 IN IP4 198.51.100.1\r\ns=-\r\nc=IN IP4 198.51.100.1\r\nt=0 0\r\n\
        m=audio 40000 RTP/AVP 18 0 8 101\r\na=rtpmap:18 G729/8000\r\na=rtpmap:101 telephone-event/8000\r\n\
        a=fmtp:101 0-16\r\na=sendrecv\r\n";

    #[test]
    fn parses_a_carrier_offer() {
        let a = parse(CARRIER).unwrap();
        assert_eq!(a.addr, Some("198.51.100.1:40000".parse().unwrap()));
        assert_eq!(a.payloads.iter().map(|p| p.pt).collect::<Vec<_>>(), [18, 0, 8, 101]);
        // PCMU and PCMA have no rtpmap: static payload types.
        assert_eq!(a.supported().map(|p| p.codec.unwrap()).collect::<Vec<_>>(), [Codec::Pcmu, Codec::Pcma]);
        assert_eq!(a.payloads[3].fmtp.as_deref(), Some("0-16"));
        assert_eq!(a.telephone_event(8000).map(|p| p.pt), Some(101));
        assert_eq!(a.telephone_event(48000), None);
        assert_eq!(a.direction, Direction::SendRecv);
    }

    #[test]
    fn media_level_connection_and_direction_win() {
        let sdp = "v=0\no=- 1 1 IN IP4 10.0.0.1\ns=-\nc=IN IP4 10.0.0.1\nt=0 0\na=sendrecv\n\
            m=audio 5004 RTP/AVP 111\nc=IN IP4 10.0.0.2\na=rtpmap:111 opus/48000/2\na=sendonly\n\
            m=video 5006 RTP/AVP 96\nc=IN IP4 10.0.0.3\n";
        let a = parse(sdp).unwrap();
        assert_eq!(a.addr, Some("10.0.0.2:5004".parse().unwrap()));
        assert_eq!(a.payloads[0].codec, Some(Codec::Opus));
        assert_eq!(a.payloads[0].channels, Some(2));
        assert_eq!(a.direction, Direction::SendOnly);
    }

    #[test]
    fn disabled_or_held_streams_have_no_address() {
        let held = CARRIER.replace("c=IN IP4 198.51.100.1", "c=IN IP4 0.0.0.0");
        assert_eq!(parse(&held).unwrap().addr, None);
        let disabled = CARRIER.replace("m=audio 40000", "m=audio 0");
        assert_eq!(parse(&disabled).unwrap().addr, None);
    }

    #[test]
    fn rejects_sdp_without_audio_or_address() {
        assert!(parse("v=0\r\nm=video 1 RTP/AVP 96\r\n").is_err());
        assert!(parse("v=0\r\nm=audio 1 RTP/AVP 0\r\n").is_err());
        assert!(parse("v=0\r\nc=IN IP4 1.2.3.4\r\nm=audio x RTP/AVP 0\r\n").is_err());
    }

    #[test]
    fn build_round_trips() {
        let payloads = vec![Payload::for_codec(Codec::Pcmu, 0), Payload::for_codec(Codec::Opus, 96)];
        let text = build("10.1.2.3".parse().unwrap(), 30000, &payloads, Direction::RecvOnly, 7, 2);
        assert!(text.contains("m=audio 30000 RTP/AVP 0 96\r\n"), "{text}");
        assert!(text.contains("a=rtpmap:96 opus/48000/2\r\n"), "{text}");
        let back = parse(&text).unwrap();
        assert_eq!(back.addr, Some("10.1.2.3:30000".parse().unwrap()));
        assert_eq!(back.payloads, payloads);
        assert_eq!(back.direction, Direction::RecvOnly);
    }

    #[test]
    fn picks_a_free_dynamic_payload_type() {
        let taken = [Payload::for_codec(Codec::Opus, 96), Payload::for_codec(Codec::Opus, 97)];
        assert_eq!(free_dynamic_pt(&taken), Some(98));
    }
}
