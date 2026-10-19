//! WebRTC media: ICE-lite, DTLS-SRTP and `rtcp-mux` on one UDP port per leg.
//!
//! - **Demultiplexing** (RFC 7983), by the first byte of each datagram:
//!   - 0–3: STUN, ICE connectivity checks ([`ice`]);
//!   - 20–63: DTLS, the handshake that keys SRTP ([`dtls`]);
//!   - 128–191: SRTP, or SRTCP when the payload type is 64–95 (`rtcp-mux`, RFC 5761).
//! - **ICE-lite:** checks are answered, and the nominated address becomes where media goes.
//! - **DTLS:** OpenSSL, with the server's self-signed certificate. The peer's certificate
//!   must match the fingerprint from its SDP. The fingerprint can arrive after the
//!   handshake: a browser answering our offer may connect before its answer reaches us. The
//!   keys are held until it does.
//! - **SRTP:** libsrtp sessions from the DTLS keys. The leg's thread decrypts what it
//!   receives; whatever sends to the leg encrypts with the leg's outbound session.
//!
//! A [`Transport`] belongs to one leg and lives on that leg's RTP thread.

pub mod dtls;
pub mod ice;
pub mod stun;

use std::net::SocketAddr;
use std::sync::Arc;

use dtls::{Dtls, Event, Identity, Keys, Role};
use ice::{Credentials, IceLite};
use srtp::openssl::{InboundSession, OutboundSession};

use crate::sdp::Fingerprint;

/// What a datagram on a WebRTC leg's port is (RFC 7983).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Stun,
    Dtls,
    Rtp,
    Rtcp,
    Other,
}

pub fn classify(packet: &[u8]) -> Kind {
    match packet.first() {
        Some(0..=3) => Kind::Stun,
        Some(20..=63) => Kind::Dtls,
        Some(128..=191) if packet.len() >= 2 && (192..=223).contains(&packet[1]) => Kind::Rtcp,
        Some(128..=191) => Kind::Rtp,
        _ => Kind::Other,
    }
}

/// The peer's side of a WebRTC leg, from its SDP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
    pub ufrag: String,
    pub fingerprint: Fingerprint,
    /// Our DTLS role, from the peer's `a=setup`.
    pub role: Role,
}

/// What the main thread gives a leg's thread to set up (or update) WebRTC.
#[derive(Clone)]
pub struct Setup {
    pub identity: Arc<Identity>,
    pub local: Credentials,
    /// Not known yet when we made the offer and the answer hasn't come back.
    pub remote: Option<Remote>,
}

/// What happened with a datagram.
pub enum Received {
    /// A check answered: send `reply` back to the source. If `nominated`, media goes there.
    Checked { reply: Vec<u8>, nominated: bool },
    /// DTLS: datagrams to send back to the source, and the outbound SRTP session once the
    /// handshake is done and verified.
    Dtls { send: Vec<Vec<u8>>, ready: Option<OutboundSession> },
    /// Decrypted RTP, now in the buffer passed in.
    Rtp,
    /// Decrypted RTCP, now in the buffer passed in.
    Rtcp,
    /// Nothing to do (not for us, not valid, or not decryptable).
    Dropped,
}

/// One leg's WebRTC transport.
pub struct Transport {
    ice: IceLite,
    dtls: Dtls,
    identity: Arc<Identity>,
    remote: Option<Remote>,
    inbound: Option<InboundSession>,
    /// A handshake finished before the peer's fingerprint was known.
    unverified: Option<Keys>,
}

impl Transport {
    pub fn new(setup: &Setup) -> Result<Transport, openssl::error::ErrorStack> {
        // Until the peer says otherwise, it starts the handshake (browsers answer `active`).
        let role = setup.remote.as_ref().map_or(Role::Server, |r| r.role);
        let mut t = Transport {
            ice: IceLite::new(setup.local.clone()),
            dtls: Dtls::new(&setup.identity, role)?,
            identity: setup.identity.clone(),
            remote: None,
            inbound: None,
            unverified: None,
        };
        if let Some(remote) = &setup.remote {
            t.set_remote(remote.clone());
        }
        Ok(t)
    }

    pub fn local(&self) -> &Credentials {
        self.ice.local()
    }

    /// The peer's SDP arrived (or changed). Returns the outbound session if a handshake was
    /// waiting for this fingerprint.
    pub fn set_remote(&mut self, remote: Remote) -> Option<OutboundSession> {
        self.ice.set_remote_ufrag(remote.ufrag.clone());
        if remote.role != self.dtls.role() && !self.dtls.is_connected() {
            // The peer chose the other way round: start over in that role.
            if let Ok(d) = Dtls::new(&self.identity, remote.role) {
                self.dtls = d;
            }
        }
        self.remote = Some(remote);
        let keys = self.unverified.take()?;
        self.accept(keys)
    }

    /// Client role: start the handshake (once the peer's address is known).
    pub fn start(&mut self) -> Vec<Vec<u8>> {
        self.dtls.start().send
    }

    pub fn is_client(&self) -> bool {
        self.dtls.role() == Role::Client
    }

    /// A datagram from `src`. `remote` is where the leg's media goes now (ICE): DTLS and
    /// SRTP are only taken from there. Decrypted RTP or RTCP is left in `buf`.
    pub fn receive(&mut self, packet: &[u8], src: SocketAddr, remote: Option<SocketAddr>, buf: &mut Vec<u8>) -> Received {
        match classify(packet) {
            Kind::Stun => match self.ice.check(packet, src) {
                Some(c) => Received::Checked { reply: c.reply, nominated: c.nominated },
                None => Received::Dropped,
            },
            _ if remote != Some(src) => Received::Dropped,
            Kind::Dtls => {
                let out = self.dtls.receive(packet);
                let ready = match out.event {
                    Some(Event::Connected(keys)) => self.accept(*keys),
                    Some(Event::Failed(e)) => {
                        tracing::warn!(error = %e, "DTLS handshake failed");
                        None
                    }
                    None => None,
                };
                Received::Dtls { send: out.send, ready }
            }
            kind @ (Kind::Rtp | Kind::Rtcp) => {
                let Some(inbound) = &mut self.inbound else { return Received::Dropped };
                buf.clear();
                buf.extend_from_slice(packet);
                let ok = if kind == Kind::Rtp { inbound.unprotect(buf) } else { inbound.unprotect_rtcp(buf) };
                match (ok, kind) {
                    (Ok(()), Kind::Rtp) => Received::Rtp,
                    (Ok(()), _) => Received::Rtcp,
                    (Err(_), _) => Received::Dropped,
                }
            }
            Kind::Other => Received::Dropped,
        }
    }

    /// Every cycle: DTLS retransmissions to send.
    pub fn tick(&mut self) -> Vec<Vec<u8>> {
        self.dtls.tick().send
    }

    /// Keys from a finished handshake: used if the peer's certificate matches its SDP,
    /// held if the SDP isn't here yet, dropped otherwise.
    fn accept(&mut self, keys: Keys) -> Option<OutboundSession> {
        let Some(remote) = &self.remote else {
            self.unverified = Some(keys);
            return None;
        };
        let matches = keys.peer.as_ref().is_some_and(|cert| dtls::verify(cert, &remote.fingerprint));
        if !matches {
            tracing::warn!("DTLS peer certificate doesn't match the SDP fingerprint; no media");
            return None;
        }
        self.inbound = Some(keys.inbound);
        Some(keys.outbound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datagrams_are_told_apart_by_their_first_byte() {
        let mut rtp = vec![0x80, 0, 0, 1];
        rtp.resize(12, 0);
        assert_eq!(classify(&rtp), Kind::Rtp);
        let mut rtcp = vec![0x80, 200, 0, 6];
        rtcp.resize(28, 0);
        assert_eq!(classify(&rtcp), Kind::Rtcp, "a Sender Report");
        assert_eq!(classify(&[0x80, 0xE0]), Kind::Rtp, "RTP with the marker bit and payload type 96");
        assert_eq!(classify(&[22, 254, 253]), Kind::Dtls);
        assert_eq!(classify(&[0, 1, 0, 0]), Kind::Stun);
        assert_eq!(classify(&[]), Kind::Other);
    }
}
