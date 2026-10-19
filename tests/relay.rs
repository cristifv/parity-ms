//! End-to-end tests over the real TCP control channel and UDP sockets.
//! Each test uses its own port range so they can run in parallel.

mod common;

use std::time::Duration;

use common::*;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[tokio::test]
async fn ping_and_request_errors() {
    let server = start(42000, 42019, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;

    let pong = c.call(json!({"id": 1, "cmd": "ping"})).await;
    assert_eq!(pong, json!({"id": 1, "node": "test-node", "ok": true, "result": {"pong": true}}));

    let bad = c.send_raw("this is not json").await;
    assert_eq!(bad["ok"], false);
    assert_eq!(bad["error"]["code"], "bad_request");

    let unknown = c.call(json!({"id": "x", "cmd": "explode"})).await;
    assert_eq!(unknown["id"], "x", "id is echoed even when the command is invalid");
    assert_eq!(unknown["error"]["code"], "bad_request");

    let empty = c.call(json!({"cmd": "create", "call_id": ""})).await;
    assert_eq!(empty["error"]["code"], "bad_request");

    let bad_addr = c.call(json!({"cmd": "create", "call_id": "c", "a": {"remote": "nope"}})).await;
    assert_eq!(bad_addr["error"]["code"], "bad_request");

    let missing = c.call(json!({"cmd": "delete", "call_id": "nope"})).await;
    assert_eq!(missing["error"]["code"], "not_found");

    server.shutdown().await;
}

#[tokio::test]
async fn relays_rtp_after_latching_and_drops_strangers() {
    let server = start(42100, 42119, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let s = c.ok(json!({"cmd": "create", "call_id": "call-1"})).await;
    assert_eq!(s["created"], true);
    assert_eq!(s["ip"], LOCALHOST);
    let (relay_a, relay_b) = (relay(&s["a"]["rtp_port"]), relay(&s["b"]["rtp_port"]));
    assert_eq!(s["a"]["rtcp_port"].as_u64().unwrap(), s["a"]["rtp_port"].as_u64().unwrap() + 1);

    let (a, b, stranger) = (udp().await, udp().await, udp().await);

    // B's address is still unknown, so A's first packet has nowhere to go.
    a.send_to(b"a1", relay_a).await.unwrap();
    assert_nothing(&b).await;

    // B latches; A latched on its first packet, so this reaches A, from A's relay port.
    b.send_to(b"b1", relay_b).await.unwrap();
    assert_eq!(recv(&a).await, (b"b1".to_vec(), relay_a));

    a.send_to(b"a2", relay_a).await.unwrap();
    assert_eq!(recv(&b).await, (b"a2".to_vec(), relay_b));

    // Leg A is latched to `a`; anyone else is dropped.
    stranger.send_to(b"evil", relay_a).await.unwrap();
    assert_nothing(&b).await;

    let q = c.ok(json!({"cmd": "query", "call_id": "call-1"})).await;
    let (qa, qb) = (&q["a"]["rtp"], &q["b"]["rtp"]);
    assert_eq!(qa["remote"], a.local_addr().unwrap().to_string());
    assert_eq!(qa["latched"], true);
    assert_eq!(qa["received"], json!({"packets": 2, "bytes": 4}));
    assert_eq!(qa["dropped"], 2, "a1 had no destination, the stranger's packet was refused");
    assert_eq!(qa["sent"]["packets"], 1);
    assert_eq!(qb["received"]["packets"], 1);
    assert_eq!(qb["sent"]["packets"], 1);

    server.shutdown().await;
}

#[tokio::test]
async fn signalled_remote_lets_media_flow_before_latching() {
    let server = start(42200, 42219, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (a, b) = (udp().await, udp().await);

    // Start with a wrong address for B, then correct it with update (as after a re-INVITE).
    let s = c.ok(json!({"cmd": "create", "call_id": "call-2", "b": {"remote": "127.0.0.1:9"}})).await;
    let (relay_a, relay_b) = (relay(&s["a"]["rtp_port"]), relay(&s["b"]["rtp_port"]));
    let u = c
        .ok(json!({"cmd": "update", "call_id": "call-2", "b": {"remote": b.local_addr().unwrap().to_string()}}))
        .await;
    assert_eq!(u["b"]["rtp"]["latched"], false);

    a.send_to(b"hello", relay_a).await.unwrap();
    assert_eq!(recv(&b).await, (b"hello".to_vec(), relay_b));

    // Once B has latched, later updates don't move it.
    b.send_to(b"back", relay_b).await.unwrap();
    assert_eq!(recv(&a).await.0, b"back");
    let u = c.ok(json!({"cmd": "update", "call_id": "call-2", "b": {"remote": "127.0.0.1:9"}})).await;
    assert_eq!(u["b"]["rtp"]["remote"], b.local_addr().unwrap().to_string());

    server.shutdown().await;
}

#[tokio::test]
async fn relays_rtcp_on_the_odd_ports() {
    let server = start(42300, 42319, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let s = c.ok(json!({"cmd": "create", "call_id": "call-3"})).await;
    let (rtcp_a, rtcp_b) = (relay(&s["a"]["rtcp_port"]), relay(&s["b"]["rtcp_port"]));
    let (a, b) = (udp().await, udp().await);

    b.send_to(b"rr-b", rtcp_b).await.unwrap();
    // B's address is latched on B's thread; A's thread forwards only once it is known.
    for _ in 0..100 {
        let q = c.ok(json!({"cmd": "query", "call_id": "call-3"})).await;
        if q["b"]["rtcp"]["received"]["packets"] == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    a.send_to(b"sr-a", rtcp_a).await.unwrap();
    assert_eq!(recv(&b).await, (b"sr-a".to_vec(), rtcp_b));
    a.send_to(b"sr-a2", rtcp_a).await.unwrap();
    b.send_to(b"rr-b2", rtcp_b).await.unwrap();
    assert_eq!(recv(&b).await.0, b"sr-a2");
    assert_eq!(recv(&a).await, (b"rr-b2".to_vec(), rtcp_a));

    let q = c.ok(json!({"cmd": "query", "call_id": "call-3"})).await;
    assert_eq!(q["a"]["rtcp"]["received"]["packets"], 2);
    assert_eq!(q["a"]["rtp"]["received"]["packets"], 0, "RTCP doesn't touch RTP counters");

    server.shutdown().await;
}

#[tokio::test]
async fn create_is_idempotent_and_delete_frees_ports() {
    // Two port pairs: exactly one call.
    let server = start(42400, 42403, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;

    let first = c.ok(json!({"cmd": "create", "call_id": "one"})).await;
    let again = c.ok(json!({"cmd": "create", "call_id": "one"})).await;
    assert_eq!(again["created"], false);
    assert_eq!(again["a"], first["a"]);
    assert_eq!(again["b"], first["b"]);

    let full = c.call(json!({"cmd": "create", "call_id": "two"})).await;
    assert_eq!(full["error"]["code"], "no_ports");

    let stats = c.ok(json!({"cmd": "stats"})).await;
    assert_eq!((stats["calls"].as_u64(), stats["legs"].as_u64()), (Some(1), Some(2)));
    assert_eq!(stats["port_pairs_available"], 0);
    assert_eq!(c.ok(json!({"cmd": "list"})).await, json!({"calls": ["one"], "conferences": []}));

    let ended = c.ok(json!({"cmd": "delete", "call_id": "one"})).await;
    assert_eq!(ended["call_id"], "one");
    let gone = c.call(json!({"cmd": "query", "call_id": "one"})).await;
    assert_eq!(gone["error"]["code"], "not_found");

    // The sockets are closed before the ports go back to the pool, so they bind again.
    let two = c.ok(json!({"cmd": "create", "call_id": "two"})).await;
    assert_eq!(two["created"], true);

    server.shutdown().await;
}

#[tokio::test]
async fn idle_calls_are_ended() {
    let server = start(42500, 42519, Duration::from_millis(300)).await;
    let mut c = Control::connect(&server).await;
    c.ok(json!({"cmd": "create", "call_id": "quiet"})).await;
    let busy = c.ok(json!({"cmd": "create", "call_id": "busy"})).await;
    let (relay_a, sender) = (relay(&busy["a"]["rtp_port"]), udp().await);

    for _ in 0..8 {
        sender.send_to(b"rtp", relay_a).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(c.call(json!({"cmd": "query", "call_id": "quiet"})).await["error"]["code"], "not_found");
    assert_eq!(c.ok(json!({"cmd": "list"})).await, json!({"calls": ["busy"], "conferences": []}));

    server.shutdown().await;
}

#[tokio::test]
async fn oversized_request_is_refused_and_connection_closed() {
    let server = start(42600, 42619, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let huge = format!("{{\"cmd\":\"ping\",\"pad\":\"{}\"}}", "x".repeat(70 * 1024));
    let resp = c.send_raw(&huge).await;
    assert_eq!(resp["error"]["code"], "bad_request");
    let closed = timeout(Duration::from_secs(1), c.lines.next_line()).await.unwrap().unwrap();
    assert!(closed.is_none(), "server closes the connection");

    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_ends_calls_and_closes_control() {
    let server = start(42700, 42719, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    c.ok(json!({"cmd": "create", "call_id": "live"})).await;
    let addr = server.control_addr();
    let api = server.api().clone();

    server.shutdown().await;

    assert_eq!(api.engine().counts(), (0, 0, 0), "no calls, conferences or legs left");
    assert!(TcpStream::connect(addr).await.is_err(), "control port is closed");
}
