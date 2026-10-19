//! ICE-lite (RFC 8445 §2.5). parity-ms runs on public addresses, so it doesn't gather
//! candidates or run checks of its own: it offers one host candidate, answers the
//! browser's connectivity checks, and sends media to the address the browser nominates
//! (`USE-CANDIDATE`). The browser, a full ICE agent, does the rest; TURN relays (coturn)
//! show up as just another source address.

use std::net::SocketAddr;

use super::stun::{self, BINDING_REQUEST, BINDING_SUCCESS, Builder, Message, USE_CANDIDATE};

/// ICE username fragment and password (RFC 8445 §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub ufrag: String,
    pub pwd: String,
}

impl Credentials {
    /// Fresh random credentials: 8 and 24 characters (at least 4 and 22 are required),
    /// from the `ice-char` alphabet.
    pub fn random() -> Credentials {
        Credentials { ufrag: random_chars(8), pwd: random_chars(24) }
    }
}

fn random_chars(n: usize) -> String {
    const ICE_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut bytes = vec![0u8; n];
    openssl::rand::rand_bytes(&mut bytes).expect("random bytes");
    bytes.iter().map(|b| ICE_CHARS[(*b as usize) % ICE_CHARS.len()] as char).collect()
}

/// A connectivity check answered.
#[derive(Debug, PartialEq, Eq)]
pub struct Checked {
    /// The Binding success response, to send back to where the request came from.
    pub reply: Vec<u8>,
    /// The browser nominated this address (`USE-CANDIDATE`): media goes there.
    pub nominated: bool,
}

/// One leg's ICE-lite agent.
#[derive(Debug)]
pub struct IceLite {
    local: Credentials,
    /// The browser's ufrag, once its SDP is known. Checks can come before that.
    remote_ufrag: Option<String>,
}

impl IceLite {
    pub fn new(local: Credentials) -> IceLite {
        IceLite { local, remote_ufrag: None }
    }

    pub fn local(&self) -> &Credentials {
        &self.local
    }

    pub fn set_remote_ufrag(&mut self, ufrag: String) {
        self.remote_ufrag = Some(ufrag);
    }

    /// A STUN packet from `src`. Answers a Binding request that is for us (USERNAME
    /// `ours:theirs`) and carries a valid MESSAGE-INTEGRITY (our password) and FINGERPRINT.
    /// Anything else gets nothing.
    pub fn check(&self, packet: &[u8], src: SocketAddr) -> Option<Checked> {
        let m = Message::parse(packet)?;
        if m.kind != BINDING_REQUEST || !m.check_fingerprint() {
            return None;
        }
        let (ours, theirs) = m.username()?.split_once(':')?;
        if ours != self.local.ufrag || self.remote_ufrag.as_deref().is_some_and(|r| r != theirs) {
            return None;
        }
        if !m.check_integrity(self.local.pwd.as_bytes()) {
            return None;
        }
        let reply = Builder::new(BINDING_SUCCESS, m.transaction)
            .xor_mapped_address(src)
            .integrity(self.local.pwd.as_bytes())
            .fingerprint();
        Some(Checked { reply, nominated: m.has(USE_CANDIDATE) })
    }
}

/// A connectivity check as a browser sends it, for tests: `USERNAME` is
/// `lite_ufrag:browser_ufrag`, signed with the lite agent's password.
pub fn browser_check(lite: &Credentials, browser_ufrag: &str, nominate: bool, transaction: [u8; 12]) -> Vec<u8> {
    let mut b = Builder::new(BINDING_REQUEST, transaction)
        .attr(stun::USERNAME, format!("{}:{browser_ufrag}", lite.ufrag).as_bytes())
        .attr(stun::PRIORITY, &0x6E00_01FFu32.to_be_bytes())
        .attr(stun::ICE_CONTROLLING, &[1, 2, 3, 4, 5, 6, 7, 8]);
    if nominate {
        b = b.attr(USE_CANDIDATE, &[]);
    }
    b.integrity(lite.pwd.as_bytes()).fingerprint()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_for_us_are_answered_and_nominations_noticed() {
        let creds = Credentials::random();
        assert_eq!((creds.ufrag.len(), creds.pwd.len()), (8, 24));
        let mut ice = IceLite::new(creds.clone());
        let src: SocketAddr = "198.51.100.7:51000".parse().unwrap();

        let check = ice.check(&browser_check(&creds, "brws", false, [1; 12]), src).expect("answered");
        assert!(!check.nominated);
        let reply = Message::parse(&check.reply).unwrap();
        assert_eq!((reply.kind, reply.transaction), (BINDING_SUCCESS, [1; 12]));
        assert_eq!(reply.xor_mapped_address(), Some(src), "tells the browser where we saw it");
        assert!(reply.check_integrity(creds.pwd.as_bytes()) && reply.check_fingerprint());

        assert!(ice.check(&browser_check(&creds, "brws", true, [2; 12]), src).unwrap().nominated);

        // Once the browser's ufrag is known, it has to match.
        ice.set_remote_ufrag("other".into());
        assert!(ice.check(&browser_check(&creds, "brws", true, [3; 12]), src).is_none());
    }

    #[test]
    fn checks_not_for_us_get_nothing() {
        let creds = Credentials::random();
        let ice = IceLite::new(creds.clone());
        let src: SocketAddr = "198.51.100.7:51000".parse().unwrap();
        let stranger = Credentials::random();
        assert!(ice.check(&browser_check(&stranger, "x", true, [1; 12]), src).is_none(), "another ufrag");
        let forged = Credentials { ufrag: creds.ufrag.clone(), pwd: stranger.pwd };
        assert!(ice.check(&browser_check(&forged, "x", true, [1; 12]), src).is_none(), "wrong password");
        assert!(ice.check(b"not stun at all, not stun at all", src).is_none());
    }
}
