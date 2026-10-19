//! DTMF end to end: RFC 4733 relay, conversion between events and tones in both
//! directions, `send_dtmf`, and the `dtmf` events, over real TCP and UDP sockets.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use media_server::codec::{self, FRAME_SAMPLES};
use media_server::dtmf::inband::{Detection, Detector};
use media_server::dtmf::rfc4733::Event;
use media_server::dtmf::{char_to_event, event_to_char};
use media_server::events::Events;
use media_server::rtp;
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::time::sleep;

const TE_8K: &str = "101 telephone-event/8000";
const QUIET: Duration = Duration::from_millis(400);

struct Call {
    a: UdpSocket,
    b: UdpSocket,
    relay_a: SocketAddr,
    answer: Value,
}

async fn call(c: &mut Control, id: &str, a: (&str, &[&str]), b: (&str, &[&str])) -> Call {
    let (sa, sb) = (udp().await, udp().await);
    let offer = c
        .ok(json!({"cmd": "offer", "call_id": id, "sdp": endpoint_sdp(sa.local_addr().unwrap(), a.0, a.1)}))
        .await;
    let answer = c
        .ok(json!({"cmd": "answer", "call_id": id, "sdp": endpoint_sdp(sb.local_addr().unwrap(), b.0, b.1)}))
        .await;
    let port = |v: &Value| SocketAddr::new(LOCALHOST.parse().unwrap(), v.as_u64().unwrap() as u16);
    Call {
        a: sa,
        b: sb,
        relay_a: port(&offer["call"]["a"]["rtp_port"]),
        answer,
    }
}

fn event_packet(pt: u8, seq: u16, ts: u32, event: u8, end: bool, duration: u16) -> Vec<u8> {
    let ev = Event { event, end, volume: 10, duration };
    let mut p = Vec::new();
    rtp::write(&mut p, seq == 0, pt, seq, ts, 77, &ev.to_bytes());
    p
}

/// Sends one key press as RFC 4733 packets, paced like a phone: an update every 20 ms,
/// then three end packets.
async fn press(sock: &UdpSocket, to: SocketAddr, pt: u8, event: u8, ms: u16) {
    let steps = ms / 20;
    for i in 1..=steps {
        sock.send_to(&event_packet(pt, i, 8000, event, false, i * 160), to).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
    for i in 0..3 {
        sock.send_to(&event_packet(pt, steps + 1 + i, 8000, event, true, steps * 160), to).await.unwrap();
    }
}

/// Sends PCMU audio paced at 20 ms: `frames` of the digit's tone (or silence for `None`).
async fn send_audio(sock: &UdpSocket, to: SocketAddr, first_seq: u16, digit: Option<char>, frames: usize) {
    let mut tone_gen = digit.map(|d| media_server::dtmf::inband::ToneGen::new(char_to_event(d).unwrap()).unwrap());
    for i in 0..frames {
        let mut pcm = [0i16; FRAME_SAMPLES];
        if let Some(g) = &mut tone_gen {
            g.fill(&mut pcm);
        }
        let payload: Vec<u8> = pcm.iter().map(|&s| codec::linear_to_ulaw(s)).collect();
        let seq = first_seq + i as u16;
        sock.send_to(&rtp_packet(0, seq, seq as u32 * 160, 5, &payload), to).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

/// Digits heard in a stream of PCMU packets.
fn tones_in(packets: &[Vec<u8>]) -> String {
    let mut detector = Detector::default();
    let mut found = Vec::new();
    for p in packets {
        let h = rtp::parse(p).unwrap();
        if h.payload_type == 0 {
            let pcm: Vec<i16> = p[h.payload].iter().map(|&b| codec::ulaw_to_linear(b)).collect();
            detector.push(&pcm, &mut found);
        }
    }
    found
        .iter()
        .filter_map(|d| match d {
            Detection::Start(e) => event_to_char(*e),
            Detection::End(..) => None,
        })
        .collect()
}

/// RFC 4733 packets in a stream: (header, event).
fn events_in(packets: &[Vec<u8>], pt: u8) -> Vec<(rtp::Header, Event)> {
    packets
        .iter()
        .filter_map(|p| {
            let h = rtp::parse(p).unwrap();
            let ev = Event::parse(&p[h.payload.clone()])?;
            (h.payload_type == pt).then_some((h, ev))
        })
        .collect()
}

/// Key presses in RFC 4733 packets, each with its end packets counted.
fn presses(events: &[(rtp::Header, Event)]) -> Vec<(char, usize)> {
    let mut out: Vec<(u32, char, usize)> = Vec::new();
    for (h, ev) in events {
        match out.iter_mut().find(|(ts, ..)| *ts == h.timestamp) {
            Some(entry) => entry.2 += ev.end as usize,
            None => out.push((h.timestamp, event_to_char(ev.event).unwrap(), ev.end as usize)),
        }
    }
    out.into_iter().map(|(_, c, ends)| (c, ends)).collect()
}

fn dtmf_events(events: &Arc<Events>) -> Vec<Value> {
    events.captured().into_iter().map(|(_, e)| e).filter(|e| e["type"] == "dtmf").collect()
}

#[tokio::test]
async fn rfc4733_is_relayed_renumbered_and_reported() {
    let (server, events) = start_capturing(43500, 43519).await;
    let mut c = Control::connect(&server).await;
    let call = call(&mut c, "te", ("0 101", &[TE_8K]), ("0 96", &["96 telephone-event/8000"])).await;
    assert_eq!(call.answer["mode"], "passthrough");
    assert_eq!(call.answer["codecs"]["a"], json!({"codec": "pcmu", "pt": 0, "dtmf": 101}));
    assert_eq!(call.answer["codecs"]["b"], json!({"codec": "pcmu", "pt": 0, "dtmf": 96}));
    assert!(call.answer["sdp"].as_str().unwrap().contains("a=rtpmap:101 telephone-event/8000"));

    press(&call.a, call.relay_a, 101, 5, 60).await;
    let got = collect(&call.b, QUIET).await;
    assert_eq!(got.len(), 6);
    let evs = events_in(&got, 96);
    assert_eq!(evs.len(), 6, "every packet renumbered 101 → 96");
    assert_eq!(evs[0].1, Event { event: 5, end: false, volume: 10, duration: 160 });
    assert_eq!(presses(&evs), [('5', 3)]);

    let reported = dtmf_events(&events);
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!(reported[0]["call_id"], "te");
    assert_eq!(reported[0]["leg"], "a");
    assert_eq!(reported[0]["digit"], "5");
    assert_eq!(reported[0]["source"], "rfc4733");
    assert_eq!(reported[0]["duration_ms"], 60);
    server.shutdown().await;
}

#[tokio::test]
async fn rfc4733_toward_a_leg_without_it_becomes_a_tone() {
    let (server, events) = start_capturing(43600, 43619).await;
    let mut c = Control::connect(&server).await;
    let call = call(&mut c, "te2tone", ("0 101", &[TE_8K]), ("0", &[])).await;
    assert_eq!(call.answer["mode"], "transcode", "only one leg takes events, so B's stream is made here");
    assert!(call.answer["codecs"]["b"].get("dtmf").is_none());

    press(&call.a, call.relay_a, 101, 1, 200).await;
    let got = collect(&call.b, QUIET).await;
    assert!(got.iter().all(|p| rtp::parse(p).unwrap().payload_type == 0), "no events reach B");
    assert!(got.len() >= 8, "about 200 ms of tone, got {} packets", got.len());
    assert_eq!(tones_in(&got), "1");

    let reported = dtmf_events(&events);
    assert_eq!((reported[0]["digit"].as_str(), reported[0]["source"].as_str()), (Some("1"), Some("rfc4733")));
    server.shutdown().await;
}

#[tokio::test]
async fn a_tone_toward_a_leg_with_rfc4733_becomes_events() {
    let (server, events) = start_capturing(43700, 43719).await;
    let mut c = Control::connect(&server).await;
    let call = call(&mut c, "tone2te", ("0", &[]), ("0 101", &[TE_8K])).await;
    assert_eq!(call.answer["mode"], "transcode");

    send_audio(&call.a, call.relay_a, 0, None, 5).await;
    send_audio(&call.a, call.relay_a, 5, Some('7'), 12).await;
    send_audio(&call.a, call.relay_a, 17, None, 10).await;
    let got = collect(&call.b, QUIET).await;
    let evs = events_in(&got, 101);
    assert_eq!(presses(&evs), [('7', 3)], "one press, ended with three end packets");
    assert!(evs[0].0.marker, "the press starts with the marker bit");
    let ssrc = evs[0].0.ssrc;
    assert!(got.iter().all(|p| rtp::parse(p).unwrap().ssrc == ssrc), "events and audio share one stream");
    let end = evs.last().unwrap().1;
    // 240 ms of tone; detection adds about one debounce window (~50 ms) at each end.
    assert!((1200..=2800).contains(&end.duration), "150–350 ms at 8 kHz: {}", end.duration);

    let reported = dtmf_events(&events);
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!((reported[0]["digit"].as_str(), reported[0]["source"].as_str()), (Some("7"), Some("inband")));
    server.shutdown().await;
}

#[tokio::test]
async fn tones_are_reported_in_passthrough() {
    let (server, events) = start_capturing(43800, 43819).await;
    let mut c = Control::connect(&server).await;
    let call = call(&mut c, "pt", ("0", &[]), ("0", &[])).await;
    assert_eq!(call.answer["mode"], "passthrough");

    send_audio(&call.a, call.relay_a, 0, Some('#'), 6).await;
    send_audio(&call.a, call.relay_a, 6, None, 6).await;
    let got = collect(&call.b, QUIET).await;
    assert_eq!(got.len(), 12, "audio passes untouched");
    assert_eq!(tones_in(&got), "#");

    let reported = dtmf_events(&events);
    assert_eq!(reported.len(), 1);
    assert_eq!((reported[0]["leg"].as_str(), reported[0]["digit"].as_str()), (Some("a"), Some("#")));
    server.shutdown().await;
}

#[tokio::test]
async fn send_dtmf_as_events_and_as_tones() {
    let (server, events) = start_capturing(43900, 43919).await;
    let mut c = Control::connect(&server).await;
    let call = call(&mut c, "send", ("0 101", &[TE_8K]), ("0", &[])).await;

    // A negotiated telephone-event: RFC 4733 packets, one press per digit.
    let queued = c.ok(json!({"cmd": "send_dtmf", "call_id": "send", "leg": "a", "digits": "12", "duration_ms": 100})).await;
    assert_eq!(queued, json!({"queued": 2, "method": "rfc4733"}));
    let got = collect(&call.a, QUIET).await;
    let evs = events_in(&got, 101);
    assert_eq!(presses(&evs), [('1', 3), ('2', 3)]);
    assert_eq!(evs.iter().filter(|(_, e)| e.end).map(|(_, e)| e.duration).max(), Some(800), "100 ms");
    assert!(evs[0].0.marker);

    // B didn't: tones in the audio, even though nobody is talking.
    let queued = c.ok(json!({"cmd": "send_dtmf", "call_id": "send", "leg": "b", "digits": "9"})).await;
    assert_eq!(queued, json!({"queued": 1, "method": "inband"}));
    let got = collect(&call.b, QUIET).await;
    assert_eq!(tones_in(&got), "9");

    assert!(dtmf_events(&events).is_empty(), "only digits received from a leg are reported");
    server.shutdown().await;
}

#[tokio::test]
async fn send_dtmf_errors() {
    let (server, _) = start_capturing(44000, 44019).await;
    let mut c = Control::connect(&server).await;
    let send = |id: &str, digits: &str| json!({"cmd": "send_dtmf", "call_id": id, "leg": "a", "digits": digits});

    assert_eq!(c.call(send("nobody", "1")).await["error"]["code"], "not_found");
    c.ok(json!({"cmd": "create", "call_id": "raw"})).await;
    assert_eq!(c.call(send("raw", "1")).await["error"]["code"], "invalid_state", "no codecs negotiated");

    call(&mut c, "ok", ("0", &[]), ("0", &[])).await;
    assert_eq!(c.call(send("ok", "12x")).await["error"]["code"], "bad_request");
    assert_eq!(c.call(send("ok", "")).await["error"]["code"], "bad_request");
    let too_long = json!({"cmd": "send_dtmf", "call_id": "ok", "leg": "a", "digits": "1", "duration_ms": 5000});
    assert_eq!(c.call(too_long).await["error"]["code"], "bad_request");
    let bad_leg = json!({"cmd": "send_dtmf", "call_id": "ok", "leg": "c", "digits": "1"});
    assert_eq!(c.call(bad_leg).await["error"]["code"], "bad_request");
    assert_eq!(c.ok(send("ok", "*#ABCD0")).await["queued"], 7);
    server.shutdown().await;
}
