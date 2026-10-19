//! RTP thread binding and thread cycles, seen through the control protocol.

mod common;

use std::time::Duration;

use common::*;
use serde_json::{Value, json};

fn thread<'a>(stats: &'a Value, name: &str) -> &'a Value {
    stats["threads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["thread"] == name)
        .unwrap_or_else(|| panic!("no thread {name} in {stats}"))
}

#[tokio::test]
async fn legs_are_bound_to_rtp_threads_and_every_thread_reports_its_cycles() {
    let server = start(44600, 44619, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;

    let call = c.ok(json!({"cmd": "create", "call_id": "bind-1"})).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "bind-1"})).await;
    let (a, b) = (q["a"]["rtp_thread"].as_str().unwrap(), q["b"]["rtp_thread"].as_str().unwrap());
    assert_ne!(a, b, "two idle threads: each leg goes to the one with fewer legs");
    assert!(call["a"]["rtp_port"].is_u64());

    // Let every thread run a few cycles (the slowest default is 100 ms).
    tokio::time::sleep(Duration::from_millis(350)).await;
    let stats = c.ok(json!({"cmd": "stats"})).await;
    for name in ["rtp-0", "rtp-1"] {
        let t = thread(&stats, name);
        assert_eq!(t["cycle_ms"], 20);
        assert_eq!(t["legs"], 1, "{t}");
        assert!(t["cycles"].as_u64().unwrap() >= 10, "{t}");
        assert!(t["since_last_cycle_ms"].as_u64().unwrap() < 200, "{t}");
        assert!(t["load_pct"].is_number(), "RTP threads measure their load: {t}");
    }
    for name in ["main", "io", "watchdog"] {
        let t = thread(&stats, name);
        assert_eq!(t["cycle_ms"], 100);
        assert!(t["cycles"].as_u64().unwrap() >= 2, "{t}");
        assert!(t.get("legs").is_none(), "{t}");
    }
    assert!(thread(&stats, "main")["load_pct"].is_null(), "not measured on the runtime thread");

    // A third leg goes to a thread with one leg; ending the call frees both bindings.
    c.ok(json!({"cmd": "join", "conference_id": "conf-1", "participant_id": "p1",
                "sdp": endpoint_sdp("127.0.0.1:9".parse().unwrap(), "0", &[])}))
        .await;
    let stats = c.ok(json!({"cmd": "stats"})).await;
    let legs = |s: &Value| (thread(s, "rtp-0")["legs"].as_u64().unwrap(), thread(s, "rtp-1")["legs"].as_u64().unwrap());
    let (l0, l1) = legs(&stats);
    assert_eq!(l0 + l1, 3);
    c.ok(json!({"cmd": "delete", "call_id": "bind-1"})).await;
    let (l0, l1) = legs(&c.ok(json!({"cmd": "stats"})).await);
    assert_eq!(l0 + l1, 1);

    server.shutdown().await;
}

struct Endpoint {
    sock: tokio::net::UdpSocket,
    relay: std::net::SocketAddr,
}

async fn join_pcmu(c: &mut Control, conference: &str, participant: &str, record: bool) -> Endpoint {
    let sock = udp().await;
    let joined = c
        .ok(json!({"cmd": "join", "conference_id": conference, "participant_id": participant, "record": record,
                   "sdp": endpoint_sdp(sock.local_addr().unwrap(), "0", &[])}))
        .await;
    let relay = media_server::sdp::parse(joined["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    Endpoint { sock, relay }
}

fn participant_threads(q: &Value) -> Vec<String> {
    q["participants"].as_array().unwrap().iter().map(|p| p["rtp_thread"].as_str().unwrap().to_string()).collect()
}

#[tokio::test]
async fn a_conference_moves_between_rtp_threads_without_breaking_its_streams() {
    use media_server::engine::Group;

    let server = start(44620, 44639, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let p1 = join_pcmu(&mut c, "mv", "p1", true).await;
    let p2 = join_pcmu(&mut c, "mv", "p2", false).await;
    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "mv"})).await;
    let before = participant_threads(&q);
    assert_ne!(before[0], before[1], "bound to different threads at first");

    // p1 talks for 1.6 s. Meanwhile the conference moves 10 times, and p1 is muted and
    // unmuted along the way, ending unmuted.
    let talk = async {
        for i in 0..80u32 {
            let payload: Vec<u8> = tone(i as usize, 1000.0, 8000.0).iter().map(|&s| media_server::codec::linear_to_ulaw(s)).collect();
            p1.sock.send_to(&rtp_packet(0, i as u16, i * 160, 11, &payload), p1.relay).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let shuffle = async {
        let engine = server.api().engine();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mute = if i % 2 == 0 { "mute" } else { "unmute" };
            c.ok(json!({"cmd": mute, "conference_id": "mv", "participant_id": "p1"})).await;
            engine.move_group(&Group::Conference("mv".into()), i % 2).unwrap();
        }
    };
    let ((), (), received) = tokio::join!(talk, shuffle, collect(&p2.sock, Duration::from_millis(1800)));

    // p2's stream never broke: one SSRC, no missing sequence numbers.
    let headers: Vec<media_server::rtp::Header> = received.iter().map(|p| media_server::rtp::parse(p).unwrap()).collect();
    assert!(headers.len() > 60, "{} packets", headers.len());
    assert!(headers.iter().all(|h| h.ssrc == headers[0].ssrc), "one stream");
    for w in headers.windows(2) {
        assert_eq!(w[1].sequence, w[0].sequence.wrapping_add(1), "no gap in the sequence numbers");
    }

    // The last command (unmute) won, after all the moves: p2 hears p1 again.
    let last: Vec<i16> = received[received.len() - 15..]
        .iter()
        .flat_map(|p| {
            let h = media_server::rtp::parse(p).unwrap();
            p[h.payload].iter().map(|&b| media_server::codec::ulaw_to_linear(b)).collect::<Vec<_>>()
        })
        .collect();
    let peak = last.iter().map(|s| s.unsigned_abs()).max().unwrap();
    assert!(peak > 3000, "p1 is heard at the end (unmuted): peak {peak}");

    // The whole conference is on one thread now, and the counts agree.
    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "mv"})).await;
    let after = participant_threads(&q);
    assert_eq!(after, ["rtp-1", "rtp-1"], "moved together, last to thread 1");
    let stats = c.ok(json!({"cmd": "stats"})).await;
    assert_eq!((thread(&stats, "rtp-0")["legs"].as_u64(), thread(&stats, "rtp-1")["legs"].as_u64()), (Some(0), Some(2)));

    // Commands still reach the legs: the recording comes back, with no big gap.
    let rec = c.ok(json!({"cmd": "stop_recording", "conference_id": "mv", "participant_id": "p1"})).await;
    let duration = rec["duration_ms"].as_u64().unwrap();
    assert!(duration >= 1700, "recording kept going through the moves: {duration} ms");
    server.shutdown().await;
}

#[tokio::test]
async fn rebalancing_moves_a_whole_call_off_a_hot_thread() {
    use media_server::leg::ThreadLoad;
    use media_server::rebalance::{RebalanceConfig, Reason};

    let server = start(44640, 44659, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    c.ok(json!({"cmd": "create", "call_id": "hot"})).await;
    let engine = server.api().engine();
    let q = c.ok(json!({"cmd": "query", "call_id": "hot"})).await;
    assert_ne!(q["a"]["rtp_thread"], q["b"]["rtp_thread"]);

    // rtp-0 reports 90% load over 10 legs; rtp-1 is idle.
    let figures = [
        ThreadLoad { index: 0, name: "rtp-0".into(), load: 900, late: 0, legs: 10 },
        ThreadLoad { index: 1, name: "rtp-1".into(), load: 0, late: 0, legs: 1 },
    ];
    let moved = engine.rebalance_with(&RebalanceConfig::default(), &figures);
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_eq!((moved[0].from, moved[0].to, moved[0].legs, moved[0].reason), (0, 1, 1, Reason::Load(900)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "hot"})).await;
    assert_eq!((q["a"]["rtp_thread"].as_str(), q["b"]["rtp_thread"].as_str()), (Some("rtp-1"), Some("rtp-1")));

    // It just moved: it stays put for 10 s, however hot the thread.
    let figures = [
        ThreadLoad { index: 0, name: "rtp-0".into(), load: 0, late: 0, legs: 0 },
        ThreadLoad { index: 1, name: "rtp-1".into(), load: 950, late: 30, legs: 2 },
    ];
    assert!(engine.rebalance_with(&RebalanceConfig::default(), &figures).is_empty());
    server.shutdown().await;
}
