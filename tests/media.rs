//! End to end: SDP offer/answer, passthrough vs transcoding, and recording, over the real
//! TCP control channel and UDP sockets.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use media_server::codec::{self, Codec, FRAME_SAMPLES};
use media_server::{rtp, sdp};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::time::sleep;

fn rms(pcm: &[i16]) -> f64 {
    (pcm.iter().map(|&s| (s as f64).powi(2)).sum::<f64>() / pcm.len().max(1) as f64).sqrt()
}

/// Leg A's socket, leg B's socket, and the relay's address for each, after offer/answer.
struct Call {
    a: UdpSocket,
    b: UdpSocket,
    relay_a: SocketAddr,
    relay_b: SocketAddr,
    answer: Value,
}

async fn negotiate(c: &mut Control, call_id: &str, a_formats: (&str, &[&str]), b_formats: (&str, &[&str]), record: bool) -> Call {
    let (a, b) = (udp().await, udp().await);
    let offer_sdp = endpoint_sdp(a.local_addr().unwrap(), a_formats.0, a_formats.1);
    let offer = c.ok(json!({"cmd": "offer", "call_id": call_id, "sdp": offer_sdp, "record": record})).await;
    let to_b = sdp::parse(offer["sdp"].as_str().unwrap()).unwrap();
    let relay_b = to_b.addr.expect("offer to B carries the relay address");
    assert_eq!(relay_b.port() as u64, offer["call"]["b"]["rtp_port"].as_u64().unwrap());

    let answer_sdp = endpoint_sdp(b.local_addr().unwrap(), b_formats.0, b_formats.1);
    let answer = c.ok(json!({"cmd": "answer", "call_id": call_id, "sdp": answer_sdp})).await;
    let to_a = sdp::parse(answer["sdp"].as_str().unwrap()).unwrap();
    let relay_a = to_a.addr.expect("answer to A carries the relay address");
    Call { a, b, relay_a, relay_b, answer }
}

/// Sends `frames` PCMU packets of a tone, paced at 20 ms.
async fn send_pcmu_tone(sock: &UdpSocket, to: SocketAddr, hz: f64, frames: u32) {
    for i in 0..frames {
        let payload: Vec<u8> = tone(i as usize, hz, 8000.0).iter().map(|&s| codec::linear_to_ulaw(s)).collect();
        sock.send_to(&rtp_packet(0, i as u16, i * 160, 7, &payload), to).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn offer_answer_with_a_common_codec_is_passthrough() {
    let server = start(43100, 43119, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);

    // The carrier offers G.729 (which we can't decode), PCMU and PCMA.
    let offer_sdp = endpoint_sdp(a.local_addr().unwrap(), "18 0 8", &["18 G729/8000"]);
    let offer = c.ok(json!({"cmd": "offer", "call_id": "pt", "sdp": offer_sdp})).await;
    assert_eq!(offer["call"]["created"], true);
    let to_b = sdp::parse(offer["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(to_b.payloads[0].name, "G729", "A's formats are offered as they are, in A's order");
    let offered: Vec<(u8, Option<Codec>)> = to_b.supported().map(|p| (p.pt, p.codec)).collect();
    assert_eq!(
        offered,
        [(0, Some(Codec::Pcmu)), (8, Some(Codec::Pcma)), (96, Some(Codec::Opus))],
        "then what we can transcode to"
    );
    // telephone-event for each clock rate, on payload types A doesn't use.
    assert_eq!(to_b.telephone_event(8000).map(|p| p.pt), Some(97));
    assert_eq!(to_b.telephone_event(48000).map(|p| p.pt), Some(98));

    let answer_sdp = endpoint_sdp(b.local_addr().unwrap(), "8", &[]);
    let answer = c.ok(json!({"cmd": "answer", "call_id": "pt", "sdp": answer_sdp})).await;
    assert_eq!(answer["mode"], "passthrough");
    assert_eq!(answer["codecs"], json!({"a": {"codec": "pcma", "pt": 8}, "b": {"codec": "pcma", "pt": 8}}));
    let to_a = sdp::parse(answer["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(to_a.payloads.iter().map(|p| p.pt).collect::<Vec<_>>(), [8]);
    let (relay_a, relay_b) = (to_a.addr.unwrap(), to_b.addr.unwrap());

    // Both remotes come from SDP, so media flows before anything latches, byte for byte.
    let from_a = rtp_packet(8, 0, 0, 1, &[0x55; 160]);
    a.send_to(&from_a, relay_a).await.unwrap();
    assert_eq!(recv(&b).await, (from_a, relay_b));
    let from_b = rtp_packet(8, 0, 0, 2, &[0xD5; 160]);
    b.send_to(&from_b, relay_b).await.unwrap();
    assert_eq!(recv(&a).await, (from_b, relay_a));

    server.shutdown().await;
}

#[tokio::test]
async fn codecs_we_cannot_decode_pass_through() {
    let server = start(43150, 43169, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let g729 = ("18 101", &["18 G729/8000", "101 telephone-event/8000"][..]);
    let call = negotiate(&mut c, "g729", g729, g729, false).await;
    assert_eq!(call.answer["mode"], "passthrough");
    assert_eq!(call.answer["codecs"]["a"], json!({"codec": "g729", "pt": 18, "dtmf": 101}));

    let packet = rtp_packet(18, 0, 0, 1, &[0x42; 20]);
    call.a.send_to(&packet, call.relay_a).await.unwrap();
    assert_eq!(recv(&call.b).await, (packet, call.relay_b));

    // Starting from something we can't transcode, there's nothing to fall back on.
    let only_g729 = endpoint_sdp("127.0.0.1:5004".parse().unwrap(), "18", &["18 G729/8000"]);
    c.ok(json!({"cmd": "offer", "call_id": "x", "sdp": only_g729})).await;
    let g722 = endpoint_sdp("127.0.0.1:5006".parse().unwrap(), "9", &["9 G722/8000"]);
    let r = c.call(json!({"cmd": "answer", "call_id": "x", "sdp": g722})).await;
    assert_eq!(r["error"]["code"], "unsupported_codec");
    server.shutdown().await;
}

#[tokio::test]
async fn pcmu_and_opus_are_transcoded_both_ways() {
    let server = start(43200, 43219, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let call = negotiate(&mut c, "tc", ("0", &[]), ("111", &["111 opus/48000/2"]), false).await;
    assert_eq!(call.answer["mode"], "transcode");
    assert_eq!(call.answer["codecs"], json!({"a": {"codec": "pcmu", "pt": 0}, "b": {"codec": "opus", "pt": 111}}));

    // A → B: PCMU in, Opus out. B's stream is made here: one packet every 20 ms.
    send_pcmu_tone(&call.a, call.relay_a, 440.0, 15).await;
    let got = collect(&call.b, Duration::from_millis(300)).await;
    let headers: Vec<rtp::Header> = got.iter().map(|p| rtp::parse(p).unwrap()).collect();
    assert!(headers.len() >= 20, "a steady stream, got {} packets", headers.len());
    assert!(headers.iter().all(|h| h.payload_type == 111));
    for w in headers.windows(2) {
        assert_eq!(w[1].sequence, w[0].sequence.wrapping_add(1));
        assert_eq!(w[1].timestamp.wrapping_sub(w[0].timestamp), 960, "20 ms on Opus's 48 kHz clock");
    }
    let mut opus = Codec::Opus.decoder().unwrap();
    let loud = got
        .iter()
        .filter(|p| {
            let h = rtp::parse(p).unwrap();
            let mut pcm = Vec::new();
            opus.decode(&p[h.payload], &mut pcm).unwrap();
            rms(&pcm) > 2000.0
        })
        .count();
    assert!(loud >= 8, "the tone made it through: {loud} loud frames");

    // B → A: Opus in, PCMU out.
    let mut enc = Codec::Opus.encoder().unwrap();
    let mut payload = Vec::new();
    for i in 0..15u32 {
        enc.encode(&tone(i as usize, 300.0, 8000.0), &mut payload).unwrap();
        call.b.send_to(&rtp_packet(111, i as u16, i * 960, 9, &payload), call.relay_b).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
    let got = collect(&call.a, Duration::from_millis(300)).await;
    let loud = got
        .iter()
        .filter(|p| {
            let h = rtp::parse(p).unwrap();
            assert_eq!((h.payload_type, h.payload.len()), (0, FRAME_SAMPLES));
            let pcm: Vec<i16> = p[h.payload].iter().map(|&b| codec::ulaw_to_linear(b)).collect();
            rms(&pcm) > 2000.0
        })
        .count();
    assert!(loud >= 8, "{loud} loud frames");

    // RTCP isn't forwarded while transcoding.
    let rtcp_a = SocketAddr::new(call.relay_a.ip(), call.relay_a.port() + 1);
    call.a.send_to(&[0x80, 200, 0, 6, 0, 0, 0, 7], rtcp_a).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "tc"})).await;
    assert_eq!(q["mode"], "transcode");
    assert_eq!(q["a"]["rtcp"]["dropped"], 1);
    assert_eq!(q["a"]["rtp"]["received"]["packets"], 15);
    assert_eq!(q["a"]["jitter"]["received"], 15, "{}", q["a"]["jitter"]);
    server.shutdown().await;
}

#[tokio::test]
async fn records_what_leg_a_says_left_and_hears_right() {
    let dir = std::env::temp_dir().join(format!("media-server-rec-e2e-{}", std::process::id()));
    let server = start_recording_to(43300, 43319, Duration::from_secs(60), dir.clone()).await;
    let mut c = Control::connect(&server).await;
    let call = negotiate(&mut c, "rec/1", ("0", &[]), ("0", &[]), true).await;

    sleep(Duration::from_millis(50)).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "rec/1"})).await;
    assert_eq!(q["a"]["recording"]["active"], true, "{q}");
    let path = q["a"]["recording"]["path"].as_str().unwrap().to_string();
    assert!(path.ends_with(".au") && path.contains("rec_1-a-"), "{path}");

    // 400 ms of audio from each side, paced like real RTP: A positive, B negative.
    let (a_payload, b_payload) = ([codec::linear_to_ulaw(6000); 160], [codec::linear_to_ulaw(-6000); 160]);
    for i in 0..20u32 {
        call.a.send_to(&rtp_packet(0, i as u16, i * 160, 1, &a_payload), call.relay_a).await.unwrap();
        call.b.send_to(&rtp_packet(0, i as u16, i * 160, 2, &b_payload), call.relay_b).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
    sleep(Duration::from_millis(100)).await;

    let status = c.ok(json!({"cmd": "stop_recording", "call_id": "rec/1"})).await;
    assert_eq!(status["active"], false);
    assert_eq!(status["path"], path.as_str());
    assert!(status["duration_ms"].as_u64().unwrap() >= 400, "{status}");
    let again = c.call(json!({"cmd": "stop_recording", "call_id": "rec/1"})).await;
    assert_eq!(again["error"]["code"], "invalid_state");

    let file = std::fs::read(&path).unwrap();
    let field = |i: usize| u32::from_be_bytes(file[i * 4..i * 4 + 4].try_into().unwrap());
    assert_eq!(&file[..4], b".snd");
    assert_eq!((field(3), field(4), field(5)), (3, 8000, 2), "16-bit PCM, 8 kHz, stereo");
    let offset = field(1) as usize;
    assert_eq!(field(2) as u64, status["bytes"].as_u64().unwrap());
    assert_eq!(file.len() as u64, offset as u64 + status["bytes"].as_u64().unwrap());

    // Timestamps: the start time in the header, and the timeline next to the file.
    let annotation = media_server::recording::au_annotation(&file).unwrap();
    let started_at = status["started_at"].as_str().unwrap();
    assert!(annotation.starts_with(&format!("start={started_at};")), "{annotation}");
    assert!(annotation.ends_with(";leg=rec/1/a"), "{annotation}");
    let timeline: Value = serde_json::from_slice(&std::fs::read(status["timeline"].as_str().unwrap()).unwrap()).unwrap();
    assert_eq!(timeline["started_at"], started_at);
    assert_eq!(timeline["duration_ms"], status["duration_ms"]);
    let anchor = timeline["anchors"][0]["at_ms"].as_u64().unwrap();
    assert!(anchor >= status["started_at_ms"].as_u64().unwrap(), "frame 0 is stamped when it was captured");

    let samples: Vec<i16> = file[offset..].chunks_exact(2).map(|b| i16::from_be_bytes([b[0], b[1]])).collect();
    let (left, right): (Vec<i16>, Vec<i16>) = samples.chunks_exact(2).map(|f| (f[0], f[1])).unzip();
    let loud = |ch: &[i16], positive: bool| ch.iter().filter(|&&s| if positive { s > 5000 } else { s < -5000 }).count();
    assert!(loud(&left, true) >= 15 * FRAME_SAMPLES, "what A says is on the left");
    assert!(loud(&right, false) >= 15 * FRAME_SAMPLES, "what A hears (B) is on the right");
    assert_eq!(loud(&left, false), 0, "no B audio on the left");
    assert_eq!(loud(&right, true), 0, "no A audio on the right");

    // A recording running when the call ends is finished and reported.
    let started = c.ok(json!({"cmd": "start_recording", "call_id": "rec/1", "leg": "b"})).await;
    assert_eq!(started["active"], true);
    assert!(started["path"].as_str().unwrap().contains("rec_1-b-"));
    sleep(Duration::from_millis(60)).await;
    let ended = c.ok(json!({"cmd": "delete", "call_id": "rec/1"})).await;
    assert_eq!(ended["b"]["recording"]["active"], false);
    assert_eq!(ended["b"]["recording"]["path"], started["path"]);
    let second = std::fs::read(started["path"].as_str().unwrap()).unwrap();
    assert_eq!(u32::from_be_bytes(second[8..12].try_into().unwrap()) as u64, ended["b"]["recording"]["bytes"].as_u64().unwrap());

    server.shutdown().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn negotiation_errors() {
    let server = start(43400, 43419, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let addr: SocketAddr = "127.0.0.1:5004".parse().unwrap();

    let events_only = endpoint_sdp(addr, "101", &["101 telephone-event/8000"]);
    let r = c.call(json!({"cmd": "offer", "call_id": "x", "sdp": events_only})).await;
    assert_eq!(r["error"]["code"], "unsupported_codec");
    assert!(c.ok(json!({"cmd": "list"})).await["calls"].as_array().unwrap().is_empty(), "nothing allocated");

    let r = c.call(json!({"cmd": "offer", "call_id": "x", "sdp": "not sdp"})).await;
    assert_eq!(r["error"]["code"], "bad_request");

    let r = c.call(json!({"cmd": "answer", "call_id": "nobody", "sdp": endpoint_sdp(addr, "0", &[])})).await;
    assert_eq!(r["error"]["code"], "not_found");

    c.ok(json!({"cmd": "create", "call_id": "raw"})).await;
    let r = c.call(json!({"cmd": "answer", "call_id": "raw", "sdp": endpoint_sdp(addr, "0", &[])})).await;
    assert_eq!(r["error"]["code"], "invalid_state", "answer before offer");

    c.ok(json!({"cmd": "offer", "call_id": "y", "sdp": endpoint_sdp(addr, "0", &[])})).await;
    let r = c.call(json!({"cmd": "answer", "call_id": "y", "sdp": endpoint_sdp(addr, "101", &["101 telephone-event/8000"])})).await;
    assert_eq!(r["error"]["code"], "unsupported_codec", "no audio in the answer");

    server.shutdown().await;
}

#[tokio::test]
async fn generated_streams_send_rtcp_sender_reports() {
    let server = start(43450, 43469, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let call = negotiate(&mut c, "sr", ("0", &[]), ("8", &[]), false).await;
    assert_eq!(call.answer["mode"], "transcode", "B's stream is generated here");
    // B's RTCP socket latches the relay's RTCP port onto itself.
    let rtcp_b = udp().await;
    let relay_rtcp_b = SocketAddr::new(call.relay_b.ip(), call.relay_b.port() + 1);
    rtcp_b.send_to(&[0x80, 201, 0, 1, 0, 0, 0, 9], relay_rtcp_b).await.unwrap();

    let started = media_server::clock::unix_ms();
    let (packet, from) = tokio::time::timeout(Duration::from_secs(3), async {
        let mut buf = [0u8; 512];
        let (n, from) = rtcp_b.recv_from(&mut buf).await.unwrap();
        (buf[..n].to_vec(), from)
    })
    .await
    .expect("a sender report within 3 s");
    assert_eq!(from, relay_rtcp_b);
    let sr = media_server::media::rtcp::parse_sender_report(&packet).expect("an SR");
    let at_ms = ((sr.ntp >> 32) - 2_208_988_800) * 1000 + (((sr.ntp & 0xFFFF_FFFF) * 1000) >> 32);
    assert!(at_ms >= started && at_ms <= media_server::clock::unix_ms() + 10, "wall-clock time of the report");
    assert!(sr.packets >= 40, "about a second of 20 ms packets: {}", sr.packets);
    assert_eq!(sr.octets, sr.packets * 160, "PCMA: 160 bytes per packet");

    // Its RTP timestamp matches the stream B receives.
    let got = collect(&call.b, Duration::from_millis(100)).await;
    let h = rtp::parse(got.last().unwrap()).unwrap();
    assert_eq!(h.ssrc, packet[4..8].iter().fold(0u32, |a, &b| (a << 8) | b as u32), "same SSRC as the audio");
    assert!(h.timestamp.wrapping_sub(sr.rtp_timestamp) < 8000, "the audio's clock, within a second");
    server.shutdown().await;
}


#[tokio::test]
async fn leg_b_reoffers_and_leg_a_answers() {
    let server = start(44800, 44819, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let call = negotiate(&mut c, "re-b", ("0 8", &[]), ("0", &[]), false).await;
    assert_eq!(call.answer["mode"], "passthrough");
    assert_eq!(call.answer["to"], "a");

    // B re-INVITEs with PCMA only. The offer goes to A, from the same relay port.
    let b_sdp = endpoint_sdp(call.b.local_addr().unwrap(), "8", &[]);
    let reoffer = c.ok(json!({"cmd": "offer", "call_id": "re-b", "leg": "b", "sdp": b_sdp})).await;
    assert_eq!(reoffer["to"], "a");
    let to_a = sdp::parse(reoffer["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(to_a.addr, Some(call.relay_a), "A keeps its relay address");
    assert_eq!(to_a.payloads[0].pt, 8, "B's codec first");
    let o_line = |s: &str| s.lines().find(|l| l.starts_with("o=")).unwrap().to_string();
    let versions = |s: &str| o_line(s).split(' ').nth(2).unwrap().parse::<u64>().unwrap();

    // The answer must come from A now.
    let wrong = c.call(json!({"cmd": "answer", "call_id": "re-b", "leg": "b", "sdp": b_sdp})).await;
    assert_eq!(wrong["error"]["code"], "invalid_state");
    let a_sdp = endpoint_sdp(call.a.local_addr().unwrap(), "8", &[]);
    let answer = c.ok(json!({"cmd": "answer", "call_id": "re-b", "leg": "a", "sdp": a_sdp})).await;
    assert_eq!((answer["to"].as_str(), answer["mode"].as_str()), (Some("b"), Some("passthrough")));
    assert_eq!((answer["codecs"]["a"]["codec"].as_str(), answer["codecs"]["b"]["codec"].as_str()), (Some("pcma"), Some("pcma")));
    let to_b = sdp::parse(answer["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(to_b.addr, Some(call.relay_b), "B keeps its relay address");
    assert!(versions(answer["sdp"].as_str().unwrap()) > versions(reoffer["sdp"].as_str().unwrap()), "the o= version goes up");

    // Media flows A → B in the new codec.
    call.b.send_to(&rtp_packet(8, 0, 0, 3, &[0xD5; 160]), call.relay_b).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    call.a.send_to(&rtp_packet(8, 1, 160, 4, &[0x55; 160]), call.relay_a).await.unwrap();
    let (p, _) = recv(&call.b).await;
    assert_eq!(rtp::parse(&p).unwrap().payload_type, 8);
    server.shutdown().await;
}

#[tokio::test]
async fn a_call_can_start_with_leg_b_offering() {
    let server = start(44820, 44839, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);
    let offer = c
        .ok(json!({"cmd": "offer", "call_id": "b-first", "leg": "b", "sdp": endpoint_sdp(b.local_addr().unwrap(), "0", &[])}))
        .await;
    assert_eq!((offer["to"].as_str(), offer["call"]["created"].as_bool()), (Some("a"), Some(true)));
    let answer = c.ok(json!({"cmd": "answer", "call_id": "b-first", "sdp": endpoint_sdp(a.local_addr().unwrap(), "0", &[])})).await;
    assert_eq!((answer["to"].as_str(), answer["mode"].as_str()), (Some("b"), Some("passthrough")));
    server.shutdown().await;
}
