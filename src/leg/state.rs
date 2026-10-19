//! One leg's media state, kept by the RTP thread the leg is bound to. Only that thread
//! touches it: packets and commands for the leg arrive there, and each 20 ms frame is run
//! there in two halves:
//! - [`Leg::receive`] decodes what the leg sent and publishes it for its peers;
//! - [`Leg::send`], once every leg on the thread has published, mixes what the leg hears,
//!   sends it and records both sides.
//!
//! So peers bound to the same thread hear each other's newest frame. Peers on other threads
//! read whatever they last published, through the lock of each frame ring.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use serde_json::json;

use super::inbound::Inbound;
use super::outbound::Outbound;
use super::stream::MAX_DATAGRAM;
use super::{LegCmd, LegConfig, LegDeps, LegEvent, LegShared};
use crate::clock::unix_ms;
use crate::dtmf::rfc4733;
use crate::media::mixer::{Frame, SILENCE};
use crate::media::passthrough;
use crate::webrtc::{self, Received, Transport};
use crate::recording::Recording;
use crate::rtp;

pub(super) struct Leg {
    shared: Arc<LegShared>,
    config: Arc<LegConfig>,
    deps: LegDeps,
    inbound: Inbound,
    outbound: Outbound,
    pub(super) recording: Option<Recording>,
    /// What the leg sent in the current frame, between `receive` and `send`.
    sent: Option<Frame>,
    /// A packet being rewritten (soft mute).
    scratch: Vec<u8>,
    /// WebRTC legs: ICE-lite, DTLS and SRTP on the leg's port.
    transport: Option<Transport>,
    /// A packet being decrypted.
    crypt: Vec<u8>,
}

impl Leg {
    pub(super) fn new(shared: Arc<LegShared>, config: Arc<LegConfig>, deps: LegDeps) -> Leg {
        let mut outbound = Outbound::new();
        outbound.configure(&config);
        Leg {
            inbound: Inbound::new(deps.jitter),
            shared,
            config,
            deps,
            outbound,
            recording: None,
            sent: None,
            scratch: Vec::with_capacity(MAX_DATAGRAM),
            transport: None,
            crypt: Vec::with_capacity(MAX_DATAGRAM),
        }
    }

    pub(super) fn shared(&self) -> &Arc<LegShared> {
        &self.shared
    }

    pub(super) fn command(&mut self, cmd: LegCmd) {
        match cmd {
            LegCmd::Configure(config) => {
                self.outbound.configure(&config);
                if config.place != self.config.place
                    && let Some(rec) = &mut self.recording
                {
                    // The recording goes with the leg; its timeline notes where to.
                    rec.event("moved", config.place.fields());
                }
                self.config = config;
            }
            // A WebRTC leg's media address comes from ICE only.
            LegCmd::SetRemote(_) if self.transport.is_some() => {}
            LegCmd::SetRemote(addr) => {
                self.shared.rtp.remote.set_hint(addr);
                self.shared.rtcp.remote.set_hint(SocketAddr::new(addr.ip(), addr.port().wrapping_add(1)));
            }
            LegCmd::StartRecording(recording) => {
                if let Some(old) = self.recording.replace(recording) {
                    drop(old.close());
                }
            }
            LegCmd::StopRecording(reply) => {
                let _ = reply.send(self.recording.take());
            }
            LegCmd::SendDtmf(digits) => self.outbound.enqueue_dtmf(digits),
            LegCmd::PeerDtmf(dtmf) => self.outbound.peer_dtmf(dtmf, &self.config, &self.shared, Instant::now()),
            LegCmd::Play(prompt) => {
                if let Some(rec) = &mut self.recording {
                    rec.event("play", json!({ "file": prompt.name, "duration_ms": prompt.duration_ms() }));
                }
                self.outbound.play(prompt);
                self.report_play();
            }
            LegCmd::StopPlay => {
                self.outbound.stop_play();
                self.report_play();
            }
            LegCmd::WebRtc(setup) => self.set_webrtc(*setup),
            // The RTP thread unbinds and moves the leg itself.
            LegCmd::Unbind(_) | LegCmd::Move(_) => {}
        }
    }

    /// Sets up the leg's WebRTC transport, or gives the existing one the peer's details.
    /// New ICE credentials (a first setup, or an ICE restart) mean a fresh ICE and DTLS
    /// session, and nothing is sent to the leg until it is keyed.
    fn set_webrtc(&mut self, setup: webrtc::Setup) {
        let same = self.transport.as_ref().is_some_and(|t| *t.local() == setup.local);
        if !same {
            self.shared.set_srtp_pending();
            match Transport::new(&setup) {
                Ok(t) => self.transport = Some(t),
                Err(e) => {
                    tracing::error!(port = self.shared.port, error = %e, "can't set up WebRTC for the leg");
                    return;
                }
            }
        } else if let Some(remote) = setup.remote
            && let Some(outbound) = self.transport.as_mut().and_then(|t| t.set_remote(remote))
        {
            self.srtp_ready(outbound);
        }
        self.start_dtls_client();
    }

    /// When we are the DTLS client and know where the peer is, send the ClientHello.
    fn start_dtls_client(&mut self) {
        let (Some(t), Some(addr)) = (&mut self.transport, self.shared.rtp.remote.addr()) else { return };
        if t.is_client() {
            for d in t.start() {
                self.shared.send_raw(&d, addr);
            }
        }
    }

    fn srtp_ready(&mut self, outbound: srtp::openssl::OutboundSession) {
        self.shared.set_srtp(outbound);
        tracing::info!(port = self.shared.port, "DTLS-SRTP keyed");
    }

    /// A datagram on the leg's RTP port.
    pub(super) fn on_rtp(&mut self, packet: &mut [u8], src: SocketAddr) {
        if self.transport.is_none() {
            return self.handle_rtp(packet, src);
        }
        self.shared.activity.touch();
        if packet.len() >= MAX_DATAGRAM {
            self.shared.rtp.drop_packet();
            return;
        }
        let remote = self.shared.rtp.remote.addr();
        let mut buf = std::mem::take(&mut self.crypt);
        let received = self.transport.as_mut().expect("checked").receive(packet, src, remote, &mut buf);
        match received {
            Received::Checked { reply, nominated } => {
                self.shared.send_raw(&reply, src);
                // The first checked address carries media until the browser nominates one.
                if (nominated || remote.is_none()) && remote != Some(src) {
                    tracing::debug!(port = self.shared.port, %src, nominated, "ICE: media address");
                    self.shared.rtp.remote.nominate(src);
                    self.start_dtls_client();
                }
            }
            Received::Dtls { send, ready } => {
                for d in send {
                    self.shared.send_raw(&d, src);
                }
                if let Some(outbound) = ready {
                    self.srtp_ready(outbound);
                }
            }
            Received::Rtp => self.handle_rtp(&mut buf, src),
            Received::Rtcp => self.handle_rtcp(&mut buf, src),
            Received::Dropped => self.shared.rtp.drop_packet(),
        }
        self.crypt = buf;
    }

    /// An RTP packet (decrypted, for a WebRTC leg).
    fn handle_rtp(&mut self, packet: &mut [u8], src: SocketAddr) {
        let shared = &self.shared;
        shared.activity.touch();
        if packet.len() >= MAX_DATAGRAM || !shared.rtp.remote.admit(src) {
            shared.rtp.drop_packet();
            return;
        }
        shared.rtp.received.add(packet.len());
        let config = &self.config;
        let Some(header) = rtp::parse(packet) else {
            // Not RTP (e.g. a STUN probe): passed along in passthrough, ignored otherwise.
            if let Some(f) = &config.forward
                && !f.to.send_rtp(packet)
            {
                shared.rtp.drop_packet();
            }
            return;
        };
        let now = Instant::now();
        let is_event = config.codec.as_ref().and_then(|c| c.dtmf) == Some(header.payload_type);
        if is_event {
            if let Some(event) = rfc4733::Event::parse(&packet[header.payload.clone()]) {
                self.inbound.on_event(config, header.timestamp, event, now);
            }
        } else {
            self.inbound.push_audio(config, &header, &packet[header.payload.clone()], now);
        }
        if let Some(f) = &config.forward {
            let codec = Inbound::codec_of(config, header.payload_type);
            if f.passthrough.apply(packet, &header) {
                let sent = match (config.muted, is_event) {
                    (false, _) => f.to.send_rtp(packet),
                    // Soft mute: digits aren't passed on, and the audio goes on as silence
                    // (or not at all, in a codec we can't make silence in).
                    (true, true) => true,
                    (true, false) => {
                        !passthrough::silence(packet, &header, codec, &mut self.scratch) || f.to.send_rtp(&self.scratch)
                    }
                };
                if !sent {
                    shared.rtp.drop_packet();
                }
            }
        }
        self.publish_digits();
    }

    /// Hands the digits the leg finished sending to the `dtmf` events and the recording.
    fn publish_digits(&mut self) {
        for digit in self.inbound.take_digits() {
            if let Some(rec) = &mut self.recording {
                rec.event("dtmf", serde_json::to_value(&digit).unwrap_or_default());
            }
            (self.deps.listener)(&self.config.place, LegEvent::Dtmf(digit));
        }
    }

    /// A datagram on the leg's RTCP port. WebRTC legs mux RTCP onto the RTP port instead.
    pub(super) fn on_rtcp(&mut self, packet: &mut [u8], src: SocketAddr) {
        if self.transport.is_some() {
            self.shared.rtcp.drop_packet();
            return;
        }
        if packet.len() >= MAX_DATAGRAM || !self.shared.rtcp.remote.admit(src) {
            self.shared.rtcp.drop_packet();
            return;
        }
        self.handle_rtcp(packet, src);
    }

    /// An RTCP packet from the leg's endpoint (decrypted, for a WebRTC leg).
    fn handle_rtcp(&mut self, packet: &mut [u8], _src: SocketAddr) {
        let shared = &self.shared;
        shared.activity.touch();
        shared.rtcp.received.add(packet.len());
        // RTCP describes the stream it came with: it only makes sense where that stream is
        // forwarded as it is.
        match &self.config.forward {
            Some(f) if f.to.send_rtcp(packet) => {}
            _ => shared.rtcp.drop_packet(),
        }
    }

    /// First half of a frame: decodes what the leg sent and publishes it for its peers.
    pub(super) fn receive(&mut self, now: Instant) {
        if let (Some(t), Some(addr)) = (&mut self.transport, self.shared.rtp.remote.addr()) {
            for d in t.tick() {
                self.shared.send_raw(&d, addr);
            }
        }
        self.sent = self.inbound.tick(&self.config, now);
        // Soft mute: what the leg says is replaced by silence for everyone, its recording
        // included. Its digits were already detected above.
        if self.config.muted && self.sent.is_some() {
            self.sent = Some(SILENCE);
        }
        if let Some(frame) = &self.sent {
            self.shared.frames.write(frame);
        }
        self.publish_digits();
    }

    /// Second half, once every leg on the thread has published: mixes what the leg hears,
    /// sends it, and records both sides.
    pub(super) fn send(&mut self, now: Instant) {
        let heard = self.outbound.tick(&self.config, &self.shared, now);
        let sent = self.sent.take();
        if let Some(rec) = &mut self.recording {
            rec.write(sent.as_ref().unwrap_or(&SILENCE), heard.as_ref().unwrap_or(&SILENCE), unix_ms());
        }
        self.report_play();
        self.shared.publish(self.inbound.stats(), self.recording.as_ref().map(|r| r.status()), self.outbound.playing());
    }

    /// Reports files that finished playing: in the recording's timeline and as events.
    fn report_play(&mut self) {
        for finished in self.outbound.take_finished() {
            if let Some(rec) = &mut self.recording {
                rec.event("play_finished", serde_json::to_value(&finished).unwrap_or_default());
            }
            (self.deps.listener)(&self.config.place, LegEvent::PlayFinished(finished));
        }
    }

    /// Private API: fills `frames` frames the RTP thread missed (it fell too far behind to
    /// catch up) with silence. The recording gets them as silence and notes the gap in its
    /// timeline. The leg's generated stream moves its RTP clock past them, so its timestamps
    /// keep to wall-clock time.
    pub(crate) fn fill_blanks(&mut self, frames: u32) {
        if let Some(rec) = &mut self.recording {
            rec.add_silence(frames, unix_ms(), "rtp thread behind");
        }
        self.outbound.fill_blanks(frames);
    }
}
