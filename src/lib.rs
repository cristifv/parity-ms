//! Media server for the contact-center platform.
//!
//! Threads:
//! - **Main thread:** parses the command line, then serves the TCP control channel. The
//!   calls and conferences live here, and it binds each new leg to an RTP thread.
//! - **RTP threads,** a fixed set (`--rtp-threads`): each runs the legs bound to it (their
//!   UDP sockets, adaptive jitter buffers, codecs, DTMF and recordings) in a 20 ms cycle.
//!   A leg is bound to the least-loaded thread and stays there until it ends.
//! - **IO thread:** writes the recordings.
//! - **Log thread:** writes the logs.
//! - **Watchdog thread:** logs a critical error when a thread stops cycling.
//!
//! Each kind of thread has its own scheduling policy and priority (`sched`) and its own
//! cycle length (`cycle`); the RTP threads run real-time by default. Every thread measures
//! its cycles: their delta, whether it is behind, and its load.
//!
//! Media:
//! - **Two-party calls:** codecs are negotiated from SDP. Media is passed through when the
//!   legs agree on a codec (any codec), and transcoded between G.711 and Opus otherwise.
//! - **Conferences:** each participant hears the mix of the others.
//! - **DTMF:** RFC 4733 and in-band, converted between legs as needed.

pub mod api;
pub mod clock;
pub mod codec;
pub mod control;
pub mod cycle;
pub mod dtmf;
pub mod engine;
pub mod events;
pub mod jitter;
pub mod leg;
pub mod logging;
pub mod media;
pub mod ports;
pub mod rebalance;
pub mod recording;
pub mod rtp;
pub mod sched;
pub mod sdp;
pub mod server;
pub mod watchdog;
pub mod webrtc;

pub use server::{Config, Server};
