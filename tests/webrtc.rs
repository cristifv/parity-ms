//! WebRTC legs end to end: a test "browser" (ICE checks, DTLS client, SRTP) against the
//! server, with plain RTP on the other side.

mod common;

use std::f64::consts::TAU;
use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use media_server::codec::{self, Codec, FRAME_SAMPLES, PCM_RATE};
use media_server::rtp;
use media_server::sdp::{self, Setup};
use media_server::webrtc::dtls::{self, Dtls, Event, Identity, Role};
use media_server::webrtc::ice::{self, Credentials};
use media_server::webrtc::{Kind, classify};
use serde_json::json;
use srtp::openssl::{InboundSession, OutboundSession};
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};

/// What a browser needs: a socket, a certificate and ICE credentials.
struct Browser {
    sock: UdpSocket,
    identity: Identity,
    ice: Credentials,
}

impl Browser {
    async fn new() -> Browser {
        Browser { sock: udp().await, identity: Identity::generate().unwrap(), ice: Credentials::random() }
    }

    /// Like Chrome's: Opus, PCMU, PCMA and telephone-event, plus a video section.
    fn sdp(&self, setup: &str) -> String {
        let addr = self.sock.local_addr().unwrap();
        format!(
            "v=0\r\no=- 4611731400430051336 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\n\
             a=msid-semantic: WMS\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 0 8 126\r\nc=IN IP4 0.0.0.0\r\n\
             a=rtcp:9 IN IP4 0.0.0.0\r\na=candidate:1 1 udp 2122260223 {ip} {port} typ host\r\n\
             a=ice-ufrag:{ufrag}\r\na=ice-pwd:{pwd}\r\na=ice-options:trickle\r\na=fingerprint:{fp}\r\n\
             a=setup:{setup}\r\na=mid:0\r\na=sendrecv\r\na=rtcp-mux\r\na=rtpmap:111 opus/48000/2\r\n\
             a=rtcp-fb:111 transport-cc\r\na=fmtp:111 minptime=10;useinbandfec=1\r\na=rtpmap:0 PCMU/8000\r\n\
             a=rtpmap:8 PCMA/8000\r\na=rtpmap:126 telephone-event/8000\r\n\
             m=video 9 UDP/TLS/RTP/SAVPF 96\r\nc=IN IP4 0.0.0.0\r\na=mid:1\r\na=ice-ufrag:{ufrag}\r\n\
             a=ice-pwd:{pwd}\r\na=fingerprint:{fp}\r\na=setup:{setup}\r\na=sendrecv\r\na=rtcp-mux\r\n\
             a=rtpmap:96 VP8/90000\r\n",
            ip = addr.ip(),
            port = addr.port(),
            ufrag = self.ice.ufrag,
            pwd = self.ice.pwd,
            fp = self.identity.fingerprint(),
        )
    }

    /// ICE (one nominating check), then DTLS as the client, against the server's SDP.
    /// Returns the SRTP sessions.
    async fn connect(&self, server_sdp: &str) -> (InboundSession, OutboundSession) {
        let server = sdp::parse(server_sdp).unwrap();
        let w = server.webrtc.clone().expect("a WebRTC SDP");
        let relay = candidate(server_sdp);
        let lite = Credentials { ufrag: w.ufrag.clone(), pwd: w.pwd.clone() };

        self.sock.send_to(&ice::browser_check(&lite, &self.ice.ufrag, true, [9; 12]), relay).await.unwrap();
        let reply = self.recv_kind(Kind::Stun).await;
        let m = media_server::webrtc::stun::Message::parse(&reply).unwrap();
        assert!(m.check_integrity(w.pwd.as_bytes()) && m.check_fingerprint(), "a valid ICE response");
        assert_eq!(m.xor_mapped_address(), Some(self.sock.local_addr().unwrap()));

        let mut client = Dtls::new(&self.identity, Role::Client).unwrap();
        for d in client.start().send {
            self.sock.send_to(&d, relay).await.unwrap();
        }
        loop {
            let d = self.recv_kind(Kind::Dtls).await;
            let out = client.receive(&d);
            for d in out.send {
                self.sock.send_to(&d, relay).await.unwrap();
            }
            match out.event {
                Some(Event::Connected(keys)) => {
                    assert!(dtls::verify(keys.peer.as_ref().unwrap(), &w.fingerprint), "the server's certificate is the one in its SDP");
                    return (keys.inbound, keys.outbound);
                }
                Some(Event::Failed(e)) => panic!("DTLS failed: {e}"),
                None => {}
            }
        }
    }

    async fn recv_kind(&self, kind: Kind) -> Vec<u8> {
        let mut buf = [0u8; 2048];
        loop {
            let (n, _) = timeout(Duration::from_secs(2), self.sock.recv_from(&mut buf)).await.expect("a reply").unwrap();
            if classify(&buf[..n]) == kind {
                return buf[..n].to_vec();
            }
        }
    }

    /// Decrypted RTP received for `window`, decoded from `codec`.
    async fn hear(&self, inbound: &mut InboundSession, codec: Codec, window: Duration) -> (Vec<i16>, usize) {
        let mut dec = codec.decoder().unwrap();
        let (mut pcm, mut rtcp) = (Vec::new(), 0);
        for packet in collect(&self.sock, window).await {
            let mut p = packet.clone();
            match classify(&packet) {
                Kind::Rtp => {
                    inbound.unprotect(&mut p).expect("SRTP decrypts");
                    let h = rtp::parse(&p).unwrap();
                    dec.decode(&p[h.payload], &mut pcm).unwrap();
                }
                Kind::Rtcp => {
                    inbound.unprotect_rtcp(&mut p).expect("SRTCP decrypts");
                    rtcp += 1;
                }
                _ => {}
            }
        }
        (pcm, rtcp)
    }

    /// Sends `frames` of a tone, encrypted, paced at 20 ms.
    async fn speak(&self, outbound: &mut OutboundSession, relay: SocketAddr, codec: Codec, pt: u8, hz: f64, frames: u32) {
        let mut enc = codec.encoder().unwrap();
        let ticks = codec.clock_rate() / 50;
        let mut payload = Vec::new();
        for i in 0..frames {
            enc.encode(&tone_frame(i, hz), &mut payload).unwrap();
            let mut p = rtp_packet(pt, i as u16, i * ticks, 0xB0B, &payload);
            outbound.protect(&mut p).unwrap();
            self.sock.send_to(&p, relay).await.unwrap();
            sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The address in an SDP's host candidate.
fn candidate(sdp: &str) -> SocketAddr {
    let line = sdp.lines().find(|l| l.starts_with("a=candidate:")).expect("a candidate");
    let f: Vec<&str> = line.split_whitespace().collect();
    format!("{}:{}", f[4], f[5]).parse().unwrap()
}

fn tone_frame(i: u32, hz: f64) -> Vec<i16> {
    (0..FRAME_SAMPLES)
        .map(|n| {
            let t = (i as usize * FRAME_SAMPLES + n) as f64 / PCM_RATE as f64;
            ((t * hz * TAU).sin() * 6000.0) as i16
        })
        .collect()
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

async fn send_pcmu(sock: &UdpSocket, to: SocketAddr, hz: f64, frames: u32) {
    for i in 0..frames {
        let payload: Vec<u8> = tone_frame(i, hz).iter().map(|&s| codec::linear_to_ulaw(s)).collect();
        sock.send_to(&rtp_packet(0, i as u16, i * 160, 77, &payload), to).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_browser_joins_a_conference_over_dtls_srtp() {
    let server = start(44860, 44879, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let browser = Browser::new().await;

    let joined = c.ok(json!({"cmd": "join", "conference_id": "web", "participant_id": "agent", "sdp": browser.sdp("actpass")})).await;
    let answer_sdp = joined["sdp"].as_str().unwrap();
    let answer = sdp::parse(answer_sdp).unwrap();
    assert_eq!(answer.proto, "UDP/TLS/RTP/SAVPF");
    let w = answer.webrtc.as_ref().unwrap();
    assert_eq!((w.setup, w.rtcp_mux), (Setup::Passive, true), "we wait for the browser's ClientHello");
    assert_eq!(answer.mid.as_deref(), Some("0"));
    assert_eq!(answer.payloads[0].name, "opus", "the browser's first choice");
    assert!(answer_sdp.contains("a=ice-lite") && answer_sdp.contains("a=group:BUNDLE 0\r\n"));
    assert!(answer_sdp.contains("m=video 0 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\n"), "video rejected in its place");
    assert_eq!(candidate(answer_sdp).port() as u64, joined["rtp_port"].as_u64().unwrap());

    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "web"})).await;
    assert_eq!(q["participants"][0]["srtp"], "pending", "nothing goes out before DTLS");

    let (mut inbound, mut outbound) = browser.connect(answer_sdp).await;
    sleep(Duration::from_millis(50)).await;
    let q = c.ok(json!({"cmd": "conference_query", "conference_id": "web"})).await;
    assert_eq!(q["participants"][0]["srtp"], "active");

    // A plain RTP participant (PCMU) joins. Each hears the other, through the encryption.
    let phone = udp().await;
    let joined = c
        .ok(json!({"cmd": "join", "conference_id": "web", "participant_id": "phone", "sdp": endpoint_sdp(phone.local_addr().unwrap(), "0", &[])}))
        .await;
    let phone_relay = sdp::parse(joined["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    let relay = candidate(answer_sdp);
    let opus_pt = answer.payloads[0].pt;
    let ((), ()) = tokio::join!(
        send_pcmu(&phone, phone_relay, 440.0, 30),
        browser.speak(&mut outbound, relay, Codec::Opus, opus_pt, 1000.0, 30),
    );
    let (heard, rtcp) = browser.hear(&mut inbound, Codec::Opus, Duration::from_millis(1200)).await;
    assert!(level(&heard, 440.0) > 1500.0, "the browser hears the phone: {}", level(&heard, 440.0));
    assert!(rtcp > 0, "Sender Reports come as SRTCP on the same port (rtcp-mux)");
    let mut phone_heard = Vec::new();
    for p in collect(&phone, Duration::from_millis(300)).await {
        let h = rtp::parse(&p).unwrap();
        phone_heard.extend(p[h.payload].iter().map(|&b| codec::ulaw_to_linear(b)));
    }
    assert!(level(&phone_heard, 1000.0) > 1500.0, "the phone hears the browser: {}", level(&phone_heard, 1000.0));
    server.shutdown().await;
}

#[tokio::test]
async fn a_carrier_call_reaches_an_agent_browser_that_connects_before_its_answer() {
    let server = start(44880, 44899, Duration::from_secs(60)).await;
    let mut c = Control::connect(&server).await;
    let (carrier, browser) = (udp().await, Browser::new().await);

    // The carrier offers PCMU; the agent's leg is WebRTC.
    let offer = c
        .ok(json!({"cmd": "offer", "call_id": "agent-call", "webrtc": true,
                   "sdp": endpoint_sdp(carrier.local_addr().unwrap(), "0", &[])}))
        .await;
    let offer_sdp = offer["sdp"].as_str().unwrap();
    let parsed = sdp::parse(offer_sdp).unwrap();
    assert_eq!(parsed.proto, "UDP/TLS/RTP/SAVPF");
    assert_eq!(parsed.webrtc.as_ref().unwrap().setup, Setup::ActPass);
    assert!(parsed.payloads.iter().any(|p| p.codec == Some(Codec::Opus)), "Opus offered to the browser too");

    // The browser connects (ICE, DTLS as the client) before its answer is signalled.
    let (mut inbound, mut outbound) = browser.connect(offer_sdp).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "agent-call"})).await;
    assert_eq!(q["b"]["srtp"], "pending", "keys held until the answer's fingerprint confirms them");

    // Its answer (PCMU only, like the carrier; setup:active) arrives: the keys are released.
    let answer_sdp = browser.sdp("active").replace("111 0 8 126", "0");
    let answer = c.ok(json!({"cmd": "answer", "call_id": "agent-call", "sdp": answer_sdp})).await;
    assert_eq!(answer["mode"], "passthrough", "PCMU both ways: passed through, decrypted and encrypted per leg");
    sleep(Duration::from_millis(50)).await;
    let q = c.ok(json!({"cmd": "query", "call_id": "agent-call"})).await;
    assert_eq!(q["b"]["srtp"], "active");

    let relay_a = sdp::parse(answer["sdp"].as_str().unwrap()).unwrap().addr.unwrap();
    let relay_b = candidate(offer_sdp);
    let ((), ()) = tokio::join!(
        send_pcmu(&carrier, relay_a, 440.0, 25),
        browser.speak(&mut outbound, relay_b, Codec::Pcmu, 0, 1000.0, 25),
    );
    let (heard, _) = browser.hear(&mut inbound, Codec::Pcmu, Duration::from_millis(300)).await;
    assert!(level(&heard, 440.0) > 1500.0, "the agent hears the carrier");
    let mut carrier_heard = Vec::new();
    for p in collect(&carrier, Duration::from_millis(300)).await {
        let h = rtp::parse(&p).unwrap();
        assert_eq!(h.payload_type, 0, "plain RTP to the carrier");
        carrier_heard.extend(p[h.payload].iter().map(|&b| codec::ulaw_to_linear(b)));
    }
    assert!(level(&carrier_heard, 1000.0) > 1500.0, "the carrier hears the agent");
    server.shutdown().await;
}
