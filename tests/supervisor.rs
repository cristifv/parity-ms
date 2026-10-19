//! Mute (soft), supervisor modes (listen, whisper, barge) and playing files into legs,
//! end to end.

mod common;

use std::f64::consts::TAU;
use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use media_server::codec::{self, FRAME_SAMPLES, PCM_RATE};
use media_server::{rtp, sdp};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::time::sleep;

const WINDOW: Duration = Duration::from_millis(300);

struct Participant {
    sock: UdpSocket,
    relay: SocketAddr,
}

async fn join(c: &mut Control, conference: &str, participant: &str, extra: Value) -> Participant {
    let sock = udp().await;
    let mut request = json!({"cmd": "join", "conference_id": conference, "participant_id": participant,
                             "sdp": endpoint_sdp(sock.local_addr().unwrap(), "0", &[])});
    request.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    let joined = c.ok(request).await;
    let relay = sdp::parse(joined["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    Participant { sock, relay }
}

fn pcmu_tone(frame: u32, hz: f64) -> Vec<u8> {
    (0..FRAME_SAMPLES)
        .map(|n| {
            let t = (frame as usize * FRAME_SAMPLES + n) as f64 / PCM_RATE as f64;
            codec::linear_to_ulaw(((t * hz * TAU).sin() * 6000.0) as i16)
        })
        .collect()
}

/// Sends `frames` frames of a PCMU tone, paced at 20 ms.
async fn speak(p: &Participant, hz: f64, frames: u32) {
    for i in 0..frames {
        p.sock.send_to(&rtp_packet(0, i as u16, i * 160, 42, &pcmu_tone(i, hz)), p.relay).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

/// Everything the socket received, decoded from PCMU.
async fn hear(sock: &UdpSocket) -> Vec<i16> {
    let mut pcm = Vec::new();
    for packet in collect(sock, WINDOW).await {
        let h = rtp::parse(&packet).unwrap();
        pcm.extend(packet[h.payload].iter().map(|&b| codec::ulaw_to_linear(b)));
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

/// Writes a 16-bit 8 kHz mono WAV of a tone into the prompt directory.
fn write_prompt(name: &str, hz: f64, ms: u32) {
    let samples = (PCM_RATE * ms / 1000) as usize;
    let data: Vec<u8> = (0..samples)
        .map(|n| (((n as f64 / PCM_RATE as f64) * hz * TAU).sin() * 8000.0) as i16)
        .flat_map(|s| s.to_le_bytes())
        .collect();
    let mut w = Vec::new();
    w.extend_from_slice(b"RIFF");
    w.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
    w.extend_from_slice(b"WAVEfmt ");
    w.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    w.extend_from_slice(&1u16.to_le_bytes()); // PCM
    w.extend_from_slice(&1u16.to_le_bytes()); // mono
    w.extend_from_slice(&8000u32.to_le_bytes()); // sample rate
    w.extend_from_slice(&16000u32.to_le_bytes()); // byte rate
    w.extend_from_slice(&2u16.to_le_bytes()); // block align
    w.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    w.extend_from_slice(b"data");
    w.extend_from_slice(&(data.len() as u32).to_le_bytes());
    w.extend_from_slice(&data);
    std::fs::create_dir_all(prompt_dir()).unwrap();
    std::fs::write(prompt_dir().join(name), w).unwrap();
}

fn kinds(events: &[(String, Value)]) -> Vec<String> {
    events.iter().map(|(_, e)| e["type"].as_str().unwrap().to_string()).collect()
}

#[tokio::test]
async fn soft_mute_keeps_the_stream_and_sends_silence_in_each_packet() {
    let (server, events) = start_capturing(44700, 44719).await;
    let mut c = Control::connect(&server).await;
    let call = c.ok(json!({"cmd": "create", "call_id": "m1"})).await;
    let (relay_a, relay_b) = (relay(&call["a"]["rtp_port"]), relay(&call["b"]["rtp_port"]));
    let (a, b) = (udp().await, udp().await);
    b.send_to(&rtp_packet(0, 0, 0, 2, &[0xFF; 160]), relay_b).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let send = |seq: u16| rtp_packet(0, seq, seq as u32 * 160, 1, &[0x12; 160]);
    a.send_to(&send(1), relay_a).await.unwrap();
    let (p, _) = recv(&b).await;
    assert_eq!(p[rtp::parse(&p).unwrap().payload], [0x12; 160], "passthrough: as sent");

    let muted = c.ok(json!({"cmd": "mute", "call_id": "m1", "leg": "a"})).await;
    assert_eq!((muted["muted"].as_bool(), muted["changed"].as_bool()), (Some(true), Some(true)));
    let again = c.ok(json!({"cmd": "mute", "call_id": "m1", "leg": "a"})).await;
    assert_eq!(again["changed"], false);
    let q = c.ok(json!({"cmd": "query", "call_id": "m1"})).await;
    assert_eq!((q["a"]["muted"].as_bool(), q["b"]["muted"].as_bool()), (Some(true), Some(false)));
    assert_eq!(q["mode"], "passthrough", "a soft mute doesn't change the stream");

    a.send_to(&send(2), relay_a).await.unwrap();
    let (p, _) = recv(&b).await;
    let h = rtp::parse(&p).unwrap();
    assert_eq!((h.sequence, h.timestamp, h.ssrc), (2, 320, 1), "same packet, same timing");
    assert_eq!(p[h.payload], [0xFF; 160], "but silence");

    c.ok(json!({"cmd": "unmute", "call_id": "m1", "leg": "a"})).await;
    a.send_to(&send(3), relay_a).await.unwrap();
    let (p, _) = recv(&b).await;
    assert_eq!(p[rtp::parse(&p).unwrap().payload], [0x12; 160]);

    let k = kinds(&events.captured());
    assert!(k.ends_with(&["leg.muted".to_string(), "leg.unmuted".to_string()]), "{k:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn a_supervisor_listens_whispers_and_barges_in() {
    let (server, events) = start_capturing(44720, 44739).await;
    let mut c = Control::connect(&server).await;
    let customer = join(&mut c, "q1", "customer", json!({})).await;
    let agent = join(&mut c, "q1", "agent", json!({})).await;
    let supervisor = join(&mut c, "q1", "supervisor", json!({"muted": true})).await;
    let (customer_hz, agent_hz, supervisor_hz) = (440.0, 1000.0, 1700.0);

    let round = || async {
        tokio::join!(speak(&customer, customer_hz, 20), speak(&agent, agent_hz, 20), speak(&supervisor, supervisor_hz, 20));
        tokio::join!(hear(&customer.sock), hear(&agent.sock), hear(&supervisor.sock))
    };

    // Listen: the supervisor hears both; nobody hears the supervisor.
    let (hc, ha, hs) = round().await;
    assert!(level(&hs, customer_hz) > 2000.0 && level(&hs, agent_hz) > 2000.0, "the supervisor hears both");
    assert!(level(&hc, supervisor_hz) < 300.0, "the customer doesn't hear the supervisor");
    assert!(level(&ha, supervisor_hz) < 300.0, "nor does the agent");
    assert!(level(&hc, agent_hz) > 2000.0, "customer and agent hear each other");

    // Whisper: only the agent hears the supervisor.
    let r = c.ok(json!({"cmd": "set_mode", "conference_id": "q1", "participant_id": "supervisor",
                        "mode": "whisper", "whisper_to": "agent"})).await;
    assert_eq!(r["mode"], "whisper");
    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "q1"})).await;
    let sup = q["participants"].as_array().unwrap().iter().find(|p| p["participant_id"] == "supervisor").unwrap().clone();
    assert_eq!((sup["muted"].as_bool(), sup["whisper_to"].as_str()), (Some(false), Some("agent")));
    let (hc, ha, _) = round().await;
    assert!(level(&ha, supervisor_hz) > 2000.0, "the agent hears the whisper");
    assert!(level(&hc, supervisor_hz) < 300.0, "the customer doesn't: {}", level(&hc, supervisor_hz));

    // Barge: everyone hears everyone.
    c.ok(json!({"cmd": "set_mode", "conference_id": "q1", "participant_id": "supervisor", "mode": "barge"})).await;
    let (hc, ha, _) = round().await;
    assert!(level(&hc, supervisor_hz) > 2000.0 && level(&ha, supervisor_hz) > 2000.0, "everyone hears the supervisor");

    // Mistakes are refused.
    let r = c.call(json!({"cmd": "set_mode", "conference_id": "q1", "participant_id": "supervisor", "mode": "whisper"})).await;
    assert_eq!(r["error"]["code"], "bad_request", "whisper needs whisper_to");
    let r = c.call(json!({"cmd": "set_mode", "conference_id": "q1", "participant_id": "supervisor",
                          "mode": "whisper", "whisper_to": "nobody"})).await;
    assert_eq!(r["error"]["code"], "not_found");

    let captured = events.captured();
    let joined: Vec<&Value> = captured.iter().map(|(_, e)| e).filter(|e| e["type"] == "participant.joined").collect();
    assert_eq!(joined.iter().map(|e| e["muted"].as_bool().unwrap()).collect::<Vec<_>>(), [false, false, true]);
    let modes: Vec<&str> =
        captured.iter().filter(|(_, e)| e["type"] == "participant.mode_changed").map(|(_, e)| e["mode"].as_str().unwrap()).collect();
    assert_eq!(modes, ["whisper", "barge"]);
    server.shutdown().await;
}

#[tokio::test]
async fn a_file_played_into_a_leg_is_heard_and_recorded_frame_by_frame() {
    write_prompt("supervisor-test-tone.wav", 1000.0, 400);
    let dir = std::env::temp_dir().join(format!("media-server-play-{}", std::process::id()));
    let server = start_recording_to(44740, 44759, Duration::from_secs(60), dir.clone()).await;
    let mut c = Control::connect(&server).await;
    let p1 = join(&mut c, "announce", "p1", json!({"record": true})).await;
    p1.sock.send_to(&rtp_packet(0, 0, 0, 5, &[0xFF; 160]), p1.relay).await.unwrap();
    sleep(Duration::from_millis(60)).await;

    let started = c.ok(json!({"cmd": "play", "conference_id": "announce", "participant_id": "p1",
                              "file": "supervisor-test-tone.wav"})).await;
    assert_eq!((started["file"].as_str(), started["duration_ms"].as_u64()), (Some("supervisor-test-tone.wav"), Some(400)));
    let heard = hear(&p1.sock).await;
    assert!(level(&heard, 1000.0) > 5000.0, "p1 hears the file: {}", level(&heard, 1000.0));
    sleep(Duration::from_millis(300)).await;

    let stopped = c.ok(json!({"cmd": "stop_recording", "conference_id": "announce", "participant_id": "p1"})).await;
    let data = std::fs::read(stopped["path"].as_str().unwrap()).unwrap();
    let offset = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
    let right: Vec<i16> =
        data[offset..].chunks_exact(4).map(|f| i16::from_be_bytes([f[2], f[3]])).collect();
    let loud_frames = right.chunks(FRAME_SAMPLES).filter(|f| f.iter().any(|&s| s.abs() > 4000)).count();
    assert_eq!(loud_frames, 20, "the 400 ms file is in the heard channel, 20 frames of it");
    let timeline: Value = serde_json::from_slice(&std::fs::read(stopped["timeline"].as_str().unwrap()).unwrap()).unwrap();
    let types: Vec<&str> = timeline["events"].as_array().unwrap().iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(types, ["play", "play_finished"]);
    assert_eq!(timeline["events"][1]["reason"], "completed");

    // Only files inside the prompt directory, and only ones that exist.
    for (file, code) in [("../../etc/passwd", "bad_request"), ("nope.wav", "not_found")] {
        let r = c.call(json!({"cmd": "play", "conference_id": "announce", "participant_id": "p1", "file": file})).await;
        assert_eq!(r["error"]["code"], code, "{file}");
    }
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn playing_into_a_call_leg_and_stopping_it() {
    write_prompt("supervisor-test-long.wav", 600.0, 3000);
    let (server, events) = start_capturing(44760, 44779).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);
    let offer = c.ok(json!({"cmd": "offer", "call_id": "p2", "sdp": endpoint_sdp(a.local_addr().unwrap(), "0", &[])})).await;
    let relay_b = sdp::parse(offer["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    c.ok(json!({"cmd": "answer", "call_id": "p2", "sdp": endpoint_sdp(b.local_addr().unwrap(), "0", &[])})).await;
    b.send_to(&rtp_packet(0, 0, 0, 9, &[0xFF; 160]), relay_b).await.unwrap();

    c.ok(json!({"cmd": "play", "call_id": "p2", "leg": "b", "file": "supervisor-test-long.wav"})).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "p2"})).await;
    assert_eq!(q["mode"], "transcode", "B's stream is made here from now on");
    let heard = hear(&b).await;
    assert!(level(&heard, 600.0) > 5000.0, "B hears the file");
    let q = c.ok(json!({"cmd": "query", "call_id": "p2"})).await;
    assert_eq!(q["b"]["playing"]["file"], "supervisor-test-long.wav");

    let stopped = c.ok(json!({"cmd": "stop_play", "call_id": "p2", "leg": "b"})).await;
    assert_eq!(stopped["stopped"], true);
    sleep(Duration::from_millis(100)).await;
    let finished: Vec<Value> =
        events.captured().into_iter().map(|(_, e)| e).filter(|e| e["type"] == "play.finished").collect();
    assert_eq!(finished.len(), 1, "{finished:?}");
    assert_eq!((finished[0]["reason"].as_str(), finished[0]["leg"].as_str()), (Some("stopped"), Some("b")));
    assert!(finished[0]["played_ms"].as_u64().unwrap() < 3000);
    let k = kinds(&events.captured());
    assert!(k.contains(&"play.started".to_string()));

    let r = c.ok(json!({"cmd": "stop_play", "call_id": "p2", "leg": "b"})).await;
    assert_eq!(r["stopped"], false, "nothing left to stop");
    server.shutdown().await;
}
