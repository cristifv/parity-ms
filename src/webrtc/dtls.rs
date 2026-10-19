//! DTLS-SRTP (RFC 5763, 5764) with OpenSSL. The DTLS handshake runs over the leg's media
//! port; once it's done, both sides derive their SRTP keys from it, and libsrtp takes over.
//!
//! - **Identity:** one self-signed certificate (ECDSA P-256) for the whole server, made at
//!   start. Its SHA-256 fingerprint goes in every WebRTC SDP we send.
//! - **The peer's certificate** is self-signed too. It's trusted when its fingerprint
//!   matches the one in the peer's SDP, which arrives over the signalling channel.
//! - **Datagrams:** OpenSSL reads and writes through an in-memory queue. The leg's RTP
//!   thread feeds it the DTLS packets it receives and sends what comes out, so DTLS needs
//!   no socket and no thread of its own.
//! - **Retransmissions:** `tick` lets OpenSSL resend a lost flight when its timer is up.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::ptr;

use foreign_types::ForeignTypeRef;
use openssl::asn1::{Asn1Integer, Asn1Time};
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::ssl::{
    HandshakeError, MidHandshakeSslStream, Ssl, SslContext, SslContextBuilder, SslMethod, SslOptions, SslRef, SslStream,
    SslVerifyMode, SslVersion,
};
use openssl::x509::{X509, X509NameBuilder, X509Ref};
use srtp::openssl::{Config, InboundSession, OutboundSession};

use crate::sdp::Fingerprint;

/// SRTP protection profiles offered in the handshake, preferred first.
const SRTP_PROFILES: &str = "SRTP_AEAD_AES_128_GCM:SRTP_AES128_CM_SHA1_80";
/// DTLS records are kept well under the path MTU.
const MTU: u32 = 1200;

/// The server's DTLS certificate and the OpenSSL context built on it.
pub struct Identity {
    context: SslContext,
    fingerprint: Fingerprint,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity").field("fingerprint", &self.fingerprint.to_string()).finish()
    }
}

impl Identity {
    /// A fresh self-signed certificate, valid from a day ago (clock skew) for a year.
    pub fn generate() -> Result<Identity, ErrorStack> {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
        let key = PKey::from_ec_key(EcKey::generate(&group)?)?;
        let mut name = X509NameBuilder::new()?;
        name.append_entry_by_text("CN", "parity-ms")?;
        let name = name.build();
        let mut serial = BigNum::new()?;
        serial.rand(64, MsbOption::MAYBE_ZERO, false)?;
        let mut cert = X509::builder()?;
        cert.set_version(2)?;
        let serial = Asn1Integer::from_bn(&serial)?;
        cert.set_serial_number(&serial)?;
        cert.set_subject_name(&name)?;
        cert.set_issuer_name(&name)?;
        cert.set_pubkey(&key)?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs()) as i64;
        let (not_before, not_after) = (Asn1Time::from_unix(now - 86_400)?, Asn1Time::days_from_now(365)?);
        cert.set_not_before(&not_before)?;
        cert.set_not_after(&not_after)?;
        cert.sign(&key, MessageDigest::sha256())?;
        let cert = cert.build();
        let fingerprint =
            Fingerprint { algorithm: "sha-256".into(), value: cert.digest(MessageDigest::sha256())?.to_vec() };

        let mut ctx = SslContextBuilder::new(SslMethod::dtls())?;
        ctx.set_min_proto_version(Some(SslVersion::DTLS1_2))?;
        ctx.set_certificate(&cert)?;
        ctx.set_private_key(&key)?;
        ctx.check_private_key()?;
        ctx.set_tlsext_use_srtp(SRTP_PROFILES)?;
        // Self-signed on both sides: the peer's certificate is checked against its SDP
        // fingerprint once the handshake is done.
        ctx.set_verify_callback(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT, |_, _| true);
        ctx.set_options(SslOptions::NO_QUERY_MTU);
        Ok(Identity { context: ctx.build(), fingerprint })
    }

    /// For the `a=fingerprint` line of our SDP.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }
}

/// Whether we wait for the peer's ClientHello (server) or send our own (client).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

/// The in-memory datagram queue OpenSSL reads from and writes to.
#[derive(Debug, Default)]
struct Datagrams {
    incoming: VecDeque<Vec<u8>>,
    outgoing: Vec<Vec<u8>>,
}

impl Read for Datagrams {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.incoming.pop_front() {
            Some(d) => {
                let n = d.len().min(buf.len());
                buf[..n].copy_from_slice(&d[..n]);
                Ok(n)
            }
            None => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}

impl Write for Datagrams {
    /// Each write is one datagram.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.outgoing.push(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum State {
    /// Not started: a server waits for the first ClientHello, a client for `start`.
    Idle(Ssl),
    Handshaking(MidHandshakeSslStream<Datagrams>),
    /// Done: the stream stays to answer retransmitted flights and alerts.
    Connected(SslStream<Datagrams>),
    Failed,
}

/// What a DTLS step produced.
#[derive(Default)]
pub struct Output {
    /// Datagrams to send to the peer.
    pub send: Vec<Vec<u8>>,
    pub event: Option<Event>,
}

pub enum Event {
    /// The handshake is done and the peer's certificate matches its SDP fingerprint (or no
    /// fingerprint was known yet: see [`Dtls::verify`]).
    Connected(Box<Keys>),
    Failed(String),
}

/// The SRTP sessions derived from the handshake.
pub struct Keys {
    pub inbound: InboundSession,
    pub outbound: OutboundSession,
    /// SHA-256 of the peer's certificate, kept to check a fingerprint that arrives late.
    pub peer: Option<X509>,
}

/// One leg's DTLS endpoint.
pub struct Dtls {
    state: State,
    role: Role,
}

impl Dtls {
    pub fn new(identity: &Identity, role: Role) -> Result<Dtls, ErrorStack> {
        let mut ssl = Ssl::new(&identity.context)?;
        ssl.set_mtu(MTU)?;
        Ok(Dtls { state: State::Idle(ssl), role })
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected(_))
    }

    /// Client: sends the ClientHello. Does nothing for a server, or once started.
    pub fn start(&mut self) -> Output {
        if self.role != Role::Client {
            return Output::default();
        }
        match std::mem::replace(&mut self.state, State::Failed) {
            State::Idle(ssl) => self.step(ssl.connect(Datagrams::default())),
            other => {
                self.state = other;
                Output::default()
            }
        }
    }

    /// A DTLS datagram from the peer.
    pub fn receive(&mut self, datagram: &[u8]) -> Output {
        match std::mem::replace(&mut self.state, State::Failed) {
            State::Idle(ssl) if self.role == Role::Server => {
                let mut io = Datagrams::default();
                io.incoming.push_back(datagram.to_vec());
                self.step(ssl.accept(io))
            }
            State::Idle(ssl) => {
                // A client hears nothing before it starts.
                self.state = State::Idle(ssl);
                Output::default()
            }
            State::Handshaking(mut mid) => {
                mid.get_mut().incoming.push_back(datagram.to_vec());
                self.step(mid.handshake())
            }
            State::Connected(mut stream) => {
                stream.get_mut().incoming.push_back(datagram.to_vec());
                let mut buf = [0u8; 1500];
                // Application data isn't used; reading lets OpenSSL answer retransmitted
                // flights and process alerts.
                while stream.ssl_read(&mut buf).is_ok() {}
                let send = std::mem::take(&mut stream.get_mut().outgoing);
                self.state = State::Connected(stream);
                Output { send, event: None }
            }
            State::Failed => Output::default(),
        }
    }

    /// Resends a lost flight when its timer is up. Called every cycle; cheap otherwise.
    pub fn tick(&mut self) -> Output {
        let State::Handshaking(mid) = &mut self.state else { return Output::default() };
        handle_timeout(mid.ssl());
        Output { send: std::mem::take(&mut mid.get_mut().outgoing), event: None }
    }

    fn step(&mut self, result: Result<SslStream<Datagrams>, HandshakeError<Datagrams>>) -> Output {
        match result {
            Ok(mut stream) => {
                let send = std::mem::take(&mut stream.get_mut().outgoing);
                let event = match keys(stream.ssl()) {
                    Ok(keys) => Event::Connected(Box::new(keys)),
                    Err(e) => Event::Failed(e),
                };
                self.state = State::Connected(stream);
                Output { send, event: Some(event) }
            }
            Err(HandshakeError::WouldBlock(mut mid)) => {
                let send = std::mem::take(&mut mid.get_mut().outgoing);
                self.state = State::Handshaking(mid);
                Output { send, event: None }
            }
            Err(HandshakeError::Failure(mut mid)) => {
                let send = std::mem::take(&mut mid.get_mut().outgoing);
                let error = mid.error().to_string();
                Output { send, event: Some(Event::Failed(error)) }
            }
            Err(HandshakeError::SetupFailure(e)) => Output { send: Vec::new(), event: Some(Event::Failed(e.to_string())) },
        }
    }
}

/// The SRTP sessions from a finished handshake.
fn keys(ssl: &SslRef) -> Result<Keys, String> {
    let (inbound, outbound) = srtp::openssl::session_pair(ssl, Config::default()).map_err(|e| e.to_string())?;
    Ok(Keys { inbound, outbound, peer: ssl.peer_certificate() })
}

/// The peer's certificate matches the fingerprint from its SDP.
pub fn verify(cert: &X509Ref, fingerprint: &Fingerprint) -> bool {
    let digest = match fingerprint.algorithm.as_str() {
        "sha-1" => MessageDigest::sha1(),
        "sha-224" => MessageDigest::sha224(),
        "sha-256" => MessageDigest::sha256(),
        "sha-384" => MessageDigest::sha384(),
        "sha-512" => MessageDigest::sha512(),
        _ => return false,
    };
    cert.digest(digest).is_ok_and(|d| openssl::memcmp::eq(&d, &fingerprint.value) && d.len() == fingerprint.value.len())
}

/// `DTLSv1_handle_timeout`, which the `openssl` crate doesn't wrap: retransmits the last
/// flight if its timer has run out.
fn handle_timeout(ssl: &SslRef) {
    const DTLS_CTRL_HANDLE_TIMEOUT: libc::c_int = 74;
    // SAFETY: `ssl` is a live SSL object; this control takes no argument.
    unsafe {
        openssl_sys::SSL_ctrl(ssl.as_ptr(), DTLS_CTRL_HANDLE_TIMEOUT, 0, ptr::null_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a handshake between two endpoints, passing datagrams until both are done.
    fn handshake(server: &mut Dtls, client: &mut Dtls) -> (Option<Keys>, Option<Keys>) {
        let (mut server_keys, mut client_keys) = (None, None);
        let mut to_server = client.start().send;
        let mut to_client = Vec::new();
        for _ in 0..20 {
            for d in to_server.drain(..) {
                let out = server.receive(&d);
                to_client.extend(out.send);
                if let Some(Event::Connected(k)) = out.event {
                    server_keys = Some(*k);
                }
            }
            for d in to_client.drain(..) {
                let out = client.receive(&d);
                to_server.extend(out.send);
                if let Some(Event::Connected(k)) = out.event {
                    client_keys = Some(*k);
                }
            }
            if server_keys.is_some() && client_keys.is_some() {
                break;
            }
        }
        (server_keys, client_keys)
    }

    fn rtp(seq: u16) -> Vec<u8> {
        let mut p = Vec::new();
        crate::rtp::write(&mut p, false, 0, seq, seq as u32 * 160, 0x1234, &[0x55; 160]);
        p
    }

    #[test]
    fn a_handshake_gives_both_sides_matching_srtp_keys() {
        let (a, b) = (Identity::generate().unwrap(), Identity::generate().unwrap());
        assert_eq!(a.fingerprint().value.len(), 32);
        assert_ne!(a.fingerprint(), b.fingerprint());
        let mut server = Dtls::new(&a, Role::Server).unwrap();
        let mut client = Dtls::new(&b, Role::Client).unwrap();
        let (s, c) = handshake(&mut server, &mut client);
        let (mut s, mut c) = (s.expect("server done"), c.expect("client done"));
        assert!(server.is_connected() && client.is_connected());

        // Each side's certificate is the one its SDP would announce.
        assert!(verify(s.peer.as_ref().unwrap(), b.fingerprint()));
        assert!(verify(c.peer.as_ref().unwrap(), a.fingerprint()));
        assert!(!verify(s.peer.as_ref().unwrap(), a.fingerprint()));

        // Client → server and server → client SRTP round trips.
        let mut packet = rtp(1);
        c.outbound.protect(&mut packet).unwrap();
        assert_ne!(packet, rtp(1), "encrypted");
        s.inbound.unprotect(&mut packet).unwrap();
        assert_eq!(packet, rtp(1));
        let mut packet = rtp(2);
        s.outbound.protect(&mut packet).unwrap();
        c.inbound.unprotect(&mut packet).unwrap();
        assert_eq!(packet, rtp(2));

        // A tampered packet is refused.
        let mut packet = rtp(3);
        c.outbound.protect(&mut packet).unwrap();
        packet[20] ^= 1;
        assert!(s.inbound.unprotect(&mut packet).is_err());
    }

    #[test]
    fn a_lost_flight_is_resent_when_its_timer_runs_out() {
        let (a, b) = (Identity::generate().unwrap(), Identity::generate().unwrap());
        let mut client = Dtls::new(&b, Role::Client).unwrap();
        let hello = client.start().send;
        assert!(!hello.is_empty(), "ClientHello");
        assert!(client.tick().send.is_empty(), "not yet");
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let again = client.tick().send;
        assert!(!again.is_empty(), "resent after the initial 1 s timer");

        // The resent ClientHello still gets the handshake going.
        let mut server = Dtls::new(&a, Role::Server).unwrap();
        let out = server.receive(&again[0]);
        assert!(!out.send.is_empty());
    }
}
