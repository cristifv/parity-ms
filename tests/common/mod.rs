//! Helpers shared by the end-to-end tests.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use media_server::codec::{FRAME_SAMPLES, PCM_RATE};
use media_server::events::Events;
use media_server::rtp;
use media_server::{Config, Server};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;

pub const LOCALHOST: &str = "127.0.0.1";
/// Two, so the legs of a call are bound to different threads.
pub const RTP_THREADS: usize = 2;

/// Where the tests put the files they `play`.
pub fn prompt_dir() -> PathBuf {
    std::env::temp_dir().join("media-server-test-prompts")
}

pub async fn start(port_min: u16, port_max: u16, idle: Duration) -> Server {
    start_recording_to(port_min, port_max, idle, std::env::temp_dir().join("media-server-tests")).await
}

pub async fn start_recording_to(port_min: u16, port_max: u16, idle: Duration, recording_dir: PathBuf) -> Server {
    start_configured(port_min, port_max, idle, recording_dir, true).await
}

/// A server without `--test-codec`: the test codec is an unknown codec there.
pub async fn start_without_test_codec(port_min: u16, port_max: u16) -> Server {
    start_configured(port_min, port_max, Duration::from_secs(60), std::env::temp_dir().join("media-server-tests"), false).await
}

async fn start_configured(port_min: u16, port_max: u16, idle: Duration, recording_dir: PathBuf, test_codec: bool) -> Server {
    Server::start(Config {
        node_id: "test-node".into(),
        control_listen: "127.0.0.1:0".parse().unwrap(),
        media_ip: LOCALHOST.parse().unwrap(),
        advertise_ip: LOCALHOST.parse().unwrap(),
        port_min,
        port_max,
        idle_timeout: idle,
        max_legs: 200,
        recording_dir,
        prompt_dir: prompt_dir(),
        test_codec,
        jitter: media_server::jitter::JitterConfig::default(),
        threads: media_server::sched::ThreadPolicies::default(),
        cycles: media_server::cycle::Cycles::default(),
        rebalance: media_server::rebalance::RebalanceConfig::off(),
        rtp_threads: RTP_THREADS,
        watchdog_timeout: Duration::from_secs(1),
        registry: media_server::cycle::Registry::new(),
    })
    .await
    .expect("server starts")
}

pub struct Control {
    pub lines: Lines<BufReader<OwnedReadHalf>>,
    pub writer: OwnedWriteHalf,
}

impl Control {
    pub async fn connect(server: &Server) -> Control {
        let (r, w) = TcpStream::connect(server.control_addr()).await.unwrap().into_split();
        Control { lines: BufReader::new(r).lines(), writer: w }
    }

    pub async fn send_raw(&mut self, line: &str) -> Value {
        self.writer.write_all(line.as_bytes()).await.unwrap();
        self.writer.write_all(b"\n").await.unwrap();
        let reply = timeout(Duration::from_secs(2), self.lines.next_line()).await.unwrap().unwrap();
        serde_json::from_str(&reply.expect("a response line")).unwrap()
    }

    pub async fn call(&mut self, request: Value) -> Value {
        self.send_raw(&request.to_string()).await
    }

    pub async fn ok(&mut self, request: Value) -> Value {
        let resp = self.call(request.clone()).await;
        assert_eq!(resp["ok"], true, "{request} failed: {resp}");
        resp["result"].clone()
    }
}

pub async fn udp() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").await.unwrap()
}

pub fn relay(port: &Value) -> SocketAddr {
    SocketAddr::new(LOCALHOST.parse().unwrap(), port.as_u64().unwrap() as u16)
}

pub async fn recv(sock: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buf = [0u8; 2048];
    let (n, from) = timeout(Duration::from_secs(1), sock.recv_from(&mut buf))
        .await
        .expect("a packet within 1 s")
        .unwrap();
    (buf[..n].to_vec(), from)
}

pub async fn assert_nothing(sock: &UdpSocket) {
    let mut buf = [0u8; 2048];
    let got = timeout(Duration::from_millis(200), sock.recv_from(&mut buf)).await;
    assert!(got.is_err(), "expected no packet, got {got:?}");
}


pub fn endpoint_sdp(addr: SocketAddr, formats: &str, rtpmaps: &[&str]) -> String {
    let mut s = format!(
        "v=0\r\no=- 1 1 IN IP4 {ip}\r\ns=-\r\nc=IN IP4 {ip}\r\nt=0 0\r\nm=audio {port} RTP/AVP {formats}\r\n",
        ip = addr.ip(),
        port = addr.port()
    );
    for m in rtpmaps {
        s.push_str(&format!("a=rtpmap:{m}\r\n"));
    }
    s.push_str("a=sendrecv\r\n");
    s
}

pub fn tone(frame: usize, hz: f64, amplitude: f64) -> Vec<i16> {
    (0..FRAME_SAMPLES)
        .map(|i| {
            let t = (frame * FRAME_SAMPLES + i) as f64 / PCM_RATE as f64;
            ((t * hz * std::f64::consts::TAU).sin() * amplitude) as i16
        })
        .collect()
}

pub fn rtp_packet(pt: u8, seq: u16, ts: u32, ssrc: u32, payload: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    rtp::write(&mut p, seq == 0, pt, seq, ts, ssrc, payload);
    p
}

pub async fn start_capturing(port_min: u16, port_max: u16) -> (Server, Arc<Events>) {
    let events = Arc::new(Events::capture());
    let server = Server::start_with_events(
        Config {
            node_id: "test-node".into(),
            control_listen: "127.0.0.1:0".parse().unwrap(),
            media_ip: LOCALHOST.parse().unwrap(),
            advertise_ip: LOCALHOST.parse().unwrap(),
            port_min,
            port_max,
            idle_timeout: Duration::from_secs(60),
            max_legs: 200,
            recording_dir: std::env::temp_dir().join("media-server-tests"),
            prompt_dir: prompt_dir(),
            test_codec: true,
            jitter: media_server::jitter::JitterConfig::default(),
            threads: media_server::sched::ThreadPolicies::default(),
            cycles: media_server::cycle::Cycles::default(),
            rebalance: media_server::rebalance::RebalanceConfig::off(),
            rtp_threads: RTP_THREADS,
            watchdog_timeout: Duration::from_secs(1),
            registry: media_server::cycle::Registry::new(),
        },
        events.clone(),
    )
    .await
    .expect("server starts");
    (server, events)
}

/// Everything `sock` receives for `window` (or until it has been quiet for 300 ms). Streams
/// made by the server send a frame every 20 ms, silence included, so they never go quiet.
pub async fn collect(sock: &UdpSocket, window: Duration) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let mut buf = [0u8; 2048];
    let deadline = tokio::time::Instant::now() + window;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let wait = left.min(Duration::from_millis(300));
        match timeout(wait, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => packets.push(buf[..n].to_vec()),
            _ => break,
        }
        if left.is_zero() {
            break;
        }
    }
    packets
}
