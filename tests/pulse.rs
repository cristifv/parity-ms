//! The test codec (pulse positions) end to end: exact through conferences and mixing,
//! exact sample positions in recordings and through transcoding.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use media_server::codec::{self, FRAME_SAMPLES, pulse};
use media_server::{rtp, sdp};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::time::sleep;

const PT: u8 = 97;

fn pulse_sdp(sock: &UdpSocket) -> String {
    endpoint_sdp(sock.local_addr().unwrap(), "97", &["97 x-pulse/8000"])
}

async fn join(c: &mut Control, conference: &str, participant: &str, record: bool) -> (UdpSocket, SocketAddr, Value) {
    let sock = udp().await;
    let joined = c
        .ok(json!({"cmd": "join", "conference_id": conference, "participant_id": participant, "record": record, "sdp": pulse_sdp(&sock)}))
        .await;
    let answer = sdp::parse(joined["sdp"].as_str().unwrap()).unwrap();
    assert_eq!(answer.payloads[0].name, "x-pulse");
    (sock, answer.addr.unwrap(), joined)
}

/// Sends `frames` packets, frame `i` carrying `pulses(i)`, paced at 20 ms.
async fn send(sock: &UdpSocket, to: SocketAddr, frames: u32, pulses: impl Fn(u32) -> Vec<(u8, i16)>) {
    for i in 0..frames {
        let packet = rtp_packet(PT, i as u16, i * 160, 0x50 + to.port() as u32, &pulse::packet(&pulses(i)));
        sock.send_to(&packet, to).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

/// The pulses of every packet received for `window`.
async fn received(sock: &UdpSocket, window: Duration) -> Vec<Vec<(u8, i16)>> {
    collect(sock, window)
        .await
        .iter()
        .map(|p| {
            let h = rtp::parse(p).unwrap();
            assert_eq!(h.payload_type, PT);
            pulse::pulses(&p[h.payload]).expect("a well-formed pulse packet")
        })
        .collect()
}

#[tokio::test]
async fn pulses_cross_a_conference_exactly_and_mix_where_expected() {
    let server = start(44900, 44919, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (a, a_relay, _) = join(&mut c, "pulses", "a", false).await;
    let (b, b_relay, _) = join(&mut c, "pulses", "b", false).await;
    let (listener, _, _) = join(&mut c, "pulses", "listener", false).await;

    // A: one pulse at sample 10, its height telling the frame number. B: one at sample 100.
    let ((), (), frames) = tokio::join!(
        send(&a, a_relay, 50, |i| vec![(10, 1000 + i as i16)]),
        send(&b, b_relay, 50, |_| vec![(100, -2000)]),
        received(&listener, Duration::from_millis(1400)),
    );

    let mut a_frames = Vec::new();
    for pulses in &frames {
        for &(offset, amplitude) in pulses {
            match offset {
                10 => a_frames.push(amplitude - 1000),
                100 => assert_eq!(amplitude, -2000, "B's pulse, exactly"),
                other => panic!("a pulse at sample {other}: nothing was sent there ({pulses:?})"),
            }
        }
    }
    assert!(a_frames.windows(2).all(|w| w[1] > w[0]), "A's frames in order, none twice: {a_frames:?}");
    assert!(a_frames.len() >= 45, "A's frames arrive, nearly all: {} of 50", a_frames.len());
    assert!(frames.iter().any(|p| p.len() == 2), "both speakers mixed into one frame");
    server.shutdown().await;
}

#[tokio::test]
async fn the_recording_puts_each_pulse_on_its_sample() {
    let dir = std::env::temp_dir().join(format!("media-server-pulse-{}", std::process::id()));
    let server = start_recording_to(44920, 44939, Duration::from_secs(60), dir.clone()).await;
    let mut c = Control::connect(&server).await;
    let (a, a_relay, _) = join(&mut c, "rec", "a", true).await;
    let (b, b_relay, _) = join(&mut c, "rec", "b", false).await;

    tokio::join!(send(&a, a_relay, 30, |_| vec![(80, 1111)]), send(&b, b_relay, 30, |_| vec![(0, 2222)]));
    sleep(Duration::from_millis(100)).await;
    let stopped = c.ok(json!({"cmd": "stop_recording", "conference_id": "rec", "participant_id": "a"})).await;
    let data = std::fs::read(stopped["path"].as_str().unwrap()).unwrap();
    let offset = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
    let samples: Vec<i16> = data[offset..].chunks_exact(2).map(|b| i16::from_be_bytes([b[0], b[1]])).collect();
    let (left, right): (Vec<_>, Vec<_>) = samples.chunks_exact(2).map(|lr| (lr[0], lr[1])).unzip();

    // Left is what A said, right is what A heard; both keep the frame grid.
    let nonzero = |ch: &[i16]| ch.iter().enumerate().filter(|(_, s)| **s != 0).map(|(i, s)| (i, *s)).collect::<Vec<_>>();
    let (said, heard) = (nonzero(&left), nonzero(&right));
    assert!(said.len() >= 25 && said.iter().all(|&(i, s)| i % FRAME_SAMPLES == 80 && s == 1111), "{said:?}");
    assert!(heard.len() >= 25 && heard.iter().all(|&(i, s)| i % FRAME_SAMPLES == 0 && s == 2222), "{heard:?}");
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pulses_keep_their_place_through_transcoding() {
    let server = start(44940, 44959, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);
    let offer = c.ok(json!({"cmd": "offer", "call_id": "tc", "sdp": pulse_sdp(&a)})).await;
    let relay_b = sdp::parse(offer["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    let answer = c.ok(json!({"cmd": "answer", "call_id": "tc", "sdp": endpoint_sdp(b.local_addr().unwrap(), "0", &[])})).await;
    assert_eq!(answer["mode"], "transcode", "x-pulse to PCMU");
    let relay_a = sdp::parse(answer["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    b.send_to(&rtp_packet(0, 0, 0, 9, &[0xFF; 160]), relay_b).await.unwrap();

    let ((), packets) = tokio::join!(send(&a, relay_a, 25, |_| vec![(40, 8000)]), collect(&b, Duration::from_millis(700)));
    let mut loud = 0;
    for p in packets {
        let h = rtp::parse(&p).unwrap();
        let pcm: Vec<i16> = p[h.payload].iter().map(|&x| codec::ulaw_to_linear(x)).collect();
        if let Some((i, s)) = pcm.iter().enumerate().max_by_key(|(_, s)| s.unsigned_abs())
            && s.unsigned_abs() > 1000
        {
            assert_eq!(i, 40, "the pulse is still on sample 40 of its frame");
            assert!((*s as i32 - 8000).abs() < 300, "G.711 keeps it near 8000: {s}");
            loud += 1;
        }
    }
    assert!(loud >= 15, "{loud} frames with the pulse");
    server.shutdown().await;
}

#[tokio::test]
async fn without_the_flag_the_test_codec_is_unknown() {
    let server = start_without_test_codec(44960, 44979).await;
    let mut c = Control::connect(&server).await;
    let sock = udp().await;
    let r = c.call(json!({"cmd": "join", "conference_id": "x", "participant_id": "p", "sdp": pulse_sdp(&sock)})).await;
    assert_eq!(r["error"]["code"], "unsupported_codec", "it can't be mixed when it isn't known");
    server.shutdown().await;
}
