//! Conferences end to end: joining with SDP, N−1 mixing across codecs, a lonely
//! participant, merging conferences, moving a call in, and recordings that follow the
//! participant.

mod common;

use std::f64::consts::TAU;
use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use media_server::codec::{Codec, FRAME_SAMPLES, PCM_RATE};
use media_server::{rtp, sdp};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::time::sleep;

const WINDOW: Duration = Duration::from_millis(300);

struct Participant {
    sock: UdpSocket,
    relay: SocketAddr,
    codec: Codec,
    pt: u8,
}

fn formats(codec: Codec) -> (&'static str, &'static [&'static str]) {
    match codec {
        Codec::Pcmu => ("0", &[]),
        Codec::Pcma => ("8", &[]),
        Codec::Opus => ("111", &["111 opus/48000/2"]),
        Codec::Pulse => ("97", &["97 x-pulse/8000"]),
    }
}

async fn join(c: &mut Control, conference: &str, participant: &str, codec: Codec, record: bool) -> (Participant, Value) {
    let sock = udp().await;
    let (fmt, maps) = formats(codec);
    let sdp_offer = endpoint_sdp(sock.local_addr().unwrap(), fmt, maps);
    let joined = c
        .ok(json!({"cmd": "join", "conference_id": conference, "participant_id": participant, "sdp": sdp_offer, "record": record}))
        .await;
    let answer = sdp::parse(joined["sdp"].as_str().unwrap()).unwrap();
    let relay = answer.addr.unwrap();
    assert_eq!(relay.port() as u64, joined["rtp_port"].as_u64().unwrap());
    let pt = answer.payloads[0].pt;
    assert_eq!(answer.payloads[0].codec, Some(codec), "the server answers with the participant's codec");
    (Participant { sock, relay, codec, pt }, joined)
}

/// Sends `frames` of a tone (paced at 20 ms) in the participant's codec.
async fn speak(p: &Participant, hz: f64, frames: u32) {
    let mut enc = p.codec.encoder().unwrap();
    let ticks = p.codec.clock_rate() / 50;
    let mut payload = Vec::new();
    for i in 0..frames {
        let pcm: Vec<i16> = (0..FRAME_SAMPLES)
            .map(|n| {
                let t = (i as usize * FRAME_SAMPLES + n) as f64 / PCM_RATE as f64;
                ((t * hz * TAU).sin() * 4000.0) as i16
            })
            .collect();
        enc.encode(&pcm, &mut payload).unwrap();
        p.sock.send_to(&rtp_packet(p.pt, i as u16, i * ticks, 42, &payload), p.relay).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

/// Everything the participant received, decoded.
async fn hear(p: &Participant) -> Vec<i16> {
    let mut dec = p.codec.decoder().unwrap();
    let mut pcm = Vec::new();
    for packet in collect(&p.sock, WINDOW).await {
        let h = rtp::parse(&packet).unwrap();
        assert_eq!(h.payload_type, p.pt);
        dec.decode(&packet[h.payload], &mut pcm).unwrap();
    }
    pcm
}

/// The strongest amplitude of `hz` over 50 ms blocks.
fn level(pcm: &[i16], hz: f64) -> f64 {
    pcm.chunks(400)
        .filter(|b| b.len() == 400)
        .map(|block| {
            let (mut re, mut im) = (0.0, 0.0);
            for (n, &s) in block.iter().enumerate() {
                let w = TAU * hz * n as f64 / PCM_RATE as f64;
                re += s as f64 * w.cos();
                im += s as f64 * w.sin();
            }
            2.0 * (re * re + im * im).sqrt() / block.len() as f64
        })
        .fold(0.0, f64::max)
}

#[tokio::test]
async fn everyone_hears_the_others_but_not_themselves() {
    let server = start(44100, 44129, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (p1, joined) = join(&mut c, "room", "p1", Codec::Pcmu, false).await;
    assert_eq!(joined["conference_created"], true);
    let (p2, joined) = join(&mut c, "room", "p2", Codec::Pcma, false).await;
    assert_eq!(joined["conference_created"], false);
    let (p3, _) = join(&mut c, "room", "p3", Codec::Opus, false).await;

    tokio::join!(speak(&p1, 440.0, 25), speak(&p2, 1000.0, 25));
    let (h1, h2, h3) = tokio::join!(hear(&p1), hear(&p2), hear(&p3));

    assert!(level(&h1, 1000.0) > 2000.0, "p1 hears p2");
    assert!(level(&h1, 440.0) < 300.0, "p1 doesn't hear itself: {}", level(&h1, 440.0));
    assert!(level(&h2, 440.0) > 2000.0, "p2 hears p1");
    assert!(level(&h2, 1000.0) < 300.0, "p2 doesn't hear itself");
    assert!(level(&h3, 440.0) > 1500.0 && level(&h3, 1000.0) > 1500.0, "p3 (Opus) hears both");

    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "room"})).await;
    let ids: Vec<&str> = q["participants"].as_array().unwrap().iter().map(|p| p["participant_id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["p1", "p2", "p3"]);
    server.shutdown().await;
}

#[tokio::test]
async fn a_lonely_participant_stays_and_hears_silence() {
    let (server, events) = start_capturing(44200, 44219).await;
    let mut c = Control::connect(&server).await;
    let (p1, _) = join(&mut c, "solo", "p1", Codec::Pcmu, false).await;
    speak(&p1, 440.0, 10).await;
    let heard = hear(&p1).await;
    assert!(heard.len() >= 15 * FRAME_SAMPLES, "a steady stream even alone: {} samples", heard.len());
    assert!(heard.iter().all(|&s| s.abs() < 100), "silence");

    // Someone else joins and leaves; p1 is alone again, and the conference carries on.
    let (_p2, _) = join(&mut c, "solo", "p2", Codec::Pcmu, false).await;
    let left = c.ok(json!({"cmd": "leave", "conference_id": "solo", "participant_id": "p2"})).await;
    assert_eq!(left["participant_id"], "p2");
    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "solo"})).await;
    assert_eq!(q["participants"].as_array().unwrap().len(), 1);

    // The last one leaving ends it.
    c.ok(json!({"cmd": "leave", "conference_id": "solo", "participant_id": "p1"})).await;
    let gone = c.call(json!({"cmd": "conference_query", "conference_id": "solo"})).await;
    assert_eq!(gone["error"]["code"], "not_found");
    let kinds: Vec<String> = events.captured().iter().map(|(_, e)| e["type"].as_str().unwrap().to_string()).collect();
    assert_eq!(
        kinds,
        ["conference.created", "participant.joined", "participant.joined", "participant.left", "participant.left", "conference.ended"]
    );
    server.shutdown().await;
}

#[tokio::test]
async fn merging_moves_participants_and_their_recordings() {
    let dir = std::env::temp_dir().join(format!("media-server-conf-{}", std::process::id()));
    let server = start_recording_to(44300, 44319, Duration::from_secs(60), dir.clone()).await;
    let mut c = Control::connect(&server).await;
    let (p1, _) = join(&mut c, "first", "p1", Codec::Pcmu, true).await;
    let (p2, _) = join(&mut c, "second", "p2", Codec::Pcmu, false).await;
    // Alone in their own conferences: neither hears the other.
    tokio::join!(speak(&p1, 440.0, 10), speak(&p2, 1000.0, 10));
    assert!(level(&hear(&p1).await, 1000.0) < 300.0);

    let merged = c.ok(json!({"cmd": "conference_merge", "from": "first", "into": "second"})).await;
    assert_eq!(merged["participants"], json!(["p1"]));
    assert_eq!(c.call(json!({"cmd": "conference_query", "conference_id": "first"})).await["error"]["code"], "not_found");

    // Same ports, now in one room.
    tokio::join!(speak(&p1, 440.0, 20), speak(&p2, 1000.0, 20));
    let (h1, h2) = tokio::join!(hear(&p1), hear(&p2));
    assert!(level(&h1, 1000.0) > 2000.0, "p1 hears p2 after the merge");
    assert!(level(&h2, 440.0) > 2000.0, "p2 hears p1 after the merge");

    // p1's recording went along: one file, from before the merge to now.
    let status = c.ok(json!({"cmd": "stop_recording", "conference_id": "second", "participant_id": "p1"})).await;
    let path = status["path"].as_str().unwrap();
    assert!(path.contains("first-p1-"), "named where it started: {path}");
    assert!(status["duration_ms"].as_u64().unwrap() >= 600, "{status}");
    let file = std::fs::read(path).unwrap();
    let offset = u32::from_be_bytes(file[4..8].try_into().unwrap()) as usize;
    let right: Vec<i16> = file[offset..].chunks_exact(4).map(|f| i16::from_be_bytes([f[2], f[3]])).collect();
    let left: Vec<i16> = file[offset..].chunks_exact(4).map(|f| i16::from_be_bytes([f[0], f[1]])).collect();
    // The timeline marks the merge, at the time it happened.
    let timeline: Value = serde_json::from_slice(&std::fs::read(status["timeline"].as_str().unwrap()).unwrap()).unwrap();
    let moved = timeline["events"].as_array().unwrap().iter().find(|e| e["type"] == "moved").expect("a moved event");
    assert_eq!((moved["conference_id"].as_str(), moved["participant_id"].as_str()), (Some("second"), Some("p1")));
    assert!(moved["offset_ms"].as_u64().unwrap() >= 150, "after the first 200 ms of the recording: {moved}");
    assert!(level(&left, 440.0) > 2000.0, "what p1 says");
    assert!(level(&right, 1000.0) > 2000.0, "what p1 hears after the merge");

    // Merge errors.
    let (_p3, _) = join(&mut c, "third", "p2", Codec::Pcmu, false).await;
    let clash = c.call(json!({"cmd": "conference_merge", "from": "third", "into": "second"})).await;
    assert_eq!(clash["error"]["code"], "invalid_state", "p2 is in both");
    let itself = c.call(json!({"cmd": "conference_merge", "from": "second", "into": "second"})).await;
    assert_eq!(itself["error"]["code"], "invalid_state");
    let missing = c.call(json!({"cmd": "conference_merge", "from": "nope", "into": "second"})).await;
    assert_eq!(missing["error"]["code"], "not_found");

    server.shutdown().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_call_moves_into_a_conference_with_its_recording() {
    let dir = std::env::temp_dir().join(format!("media-server-move-{}", std::process::id()));
    let server = start_recording_to(44400, 44429, Duration::from_secs(60), dir.clone()).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);
    let offer = c
        .ok(json!({"cmd": "offer", "call_id": "call", "sdp": endpoint_sdp(a.local_addr().unwrap(), "0", &[]), "record": true}))
        .await;
    c.ok(json!({"cmd": "answer", "call_id": "call", "sdp": endpoint_sdp(b.local_addr().unwrap(), "0", &[])})).await;
    let port = |v: &Value| SocketAddr::new(LOCALHOST.parse().unwrap(), v.as_u64().unwrap() as u16);
    let pa = Participant { sock: a, relay: port(&offer["call"]["a"]["rtp_port"]), codec: Codec::Pcmu, pt: 0 };
    let pb = Participant { sock: b, relay: port(&offer["call"]["b"]["rtp_port"]), codec: Codec::Pcmu, pt: 0 };

    let moved = c.ok(json!({"cmd": "move_to_conference", "call_id": "call", "conference_id": "bridge"})).await;
    assert_eq!(moved["participants"], json!(["call-a", "call-b"]));
    assert_eq!(c.call(json!({"cmd": "query", "call_id": "call"})).await["error"]["code"], "not_found");

    let (p3, _) = join(&mut c, "bridge", "supervisor", Codec::Opus, false).await;
    tokio::join!(speak(&pa, 440.0, 20), speak(&pb, 1000.0, 20));
    let h3 = hear(&p3).await;
    assert!(level(&h3, 440.0) > 1500.0 && level(&h3, 1000.0) > 1500.0, "the supervisor hears both legs");

    let status = c.ok(json!({"cmd": "stop_recording", "conference_id": "bridge", "participant_id": "call-a"})).await;
    assert!(status["path"].as_str().unwrap().contains("call-a-"), "the call's recording carried on");
    let q = c.ok(json!({"cmd": "list"})).await;
    assert_eq!(q, json!({"calls": [], "conferences": ["bridge"]}));

    server.shutdown().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn join_errors() {
    let server = start(44500, 44519, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let addr: SocketAddr = "127.0.0.1:5004".parse().unwrap();
    let g729 = endpoint_sdp(addr, "18", &["18 G729/8000"]);
    let r = c.call(json!({"cmd": "join", "conference_id": "x", "participant_id": "p", "sdp": g729})).await;
    assert_eq!(r["error"]["code"], "unsupported_codec", "conferences have to decode to mix");

    join(&mut c, "x", "p", Codec::Pcmu, false).await;
    let pcmu = endpoint_sdp(addr, "0", &[]);
    let again = c.call(json!({"cmd": "join", "conference_id": "x", "participant_id": "p", "sdp": pcmu})).await;
    assert_eq!(again["error"]["code"], "invalid_state");
    let missing = c.call(json!({"cmd": "leave", "conference_id": "x", "participant_id": "nobody"})).await;
    assert_eq!(missing["error"]["code"], "not_found");
    let raw = c.ok(json!({"cmd": "create", "call_id": "raw"})).await;
    assert_eq!(raw["created"], true);
    let unmovable = c.call(json!({"cmd": "move_to_conference", "call_id": "raw", "conference_id": "x"})).await;
    assert_eq!(unmovable["error"]["code"], "invalid_state", "no negotiated codec to mix with");
    server.shutdown().await;
}

#[tokio::test]
async fn a_participant_reoffers_with_another_codec_then_holds() {
    let server = start(44840, 44859, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (p1, joined) = join(&mut c, "re", "p1", Codec::Pcmu, false).await;
    let (p2, _) = join(&mut c, "re", "p2", Codec::Pcmu, false).await;
    let session = |s: &str| s.lines().find(|l| l.starts_with("o=")).unwrap().split(' ').map(str::to_string).collect::<Vec<_>>();
    let first = session(joined["sdp"].as_str().unwrap());

    // p1 switches to Opus, on the same relay port.
    let (fmt, maps) = formats(Codec::Opus);
    let sdp_opus = endpoint_sdp(p1.sock.local_addr().unwrap(), fmt, maps);
    let r = c.ok(json!({"cmd": "reoffer", "conference_id": "re", "participant_id": "p1", "sdp": sdp_opus})).await;
    assert_eq!(r["codec"]["codec"], "opus");
    let answer = sdp::parse(r["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(answer.addr, Some(p1.relay), "same relay address");
    let second = session(r["sdp"].as_str().unwrap());
    assert_eq!(second[1], first[1], "same o= session id");
    assert!(second[2].parse::<u64>().unwrap() > first[2].parse::<u64>().unwrap(), "higher o= version");

    // p1 now hears p2 in Opus (after the PCMU packets already on their way).
    collect(&p1.sock, Duration::from_millis(100)).await;
    let p1_opus = Participant { sock: p1.sock, relay: p1.relay, codec: Codec::Opus, pt: answer.payloads[0].pt };
    speak(&p2, 440.0, 20).await;
    assert!(level(&hear(&p1_opus).await, 440.0) > 1500.0, "p1 hears p2, decoded from Opus");

    // On hold (sendonly), the answer says recvonly.
    let hold = endpoint_sdp(p1_opus.sock.local_addr().unwrap(), fmt, maps).replace("a=sendrecv", "a=sendonly");
    let r = c.ok(json!({"cmd": "reoffer", "conference_id": "re", "participant_id": "p1", "sdp": hold})).await;
    assert!(r["sdp"].as_str().unwrap().contains("a=recvonly"));

    let missing = c.call(json!({"cmd": "reoffer", "conference_id": "re", "participant_id": "nobody", "sdp": sdp_opus})).await;
    assert_eq!(missing["error"]["code"], "not_found");
    server.shutdown().await;
}
