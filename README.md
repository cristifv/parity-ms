# parity-ms
parity contact center media server
=======
# media-server

The media server for the contact-center platform (see
`../contact-center-architecture/`). It anchors media for:
- **two-party calls:** relayed, or transcoded when the legs' codecs differ;
- **conferences:** any number of participants, mixed.

It also handles DTMF and records legs. Any leg can be plain RTP (carriers, phones) or
WebRTC (agents' browsers: ICE-lite, DTLS-SRTP).

| Interface | Transport | Used by |
|---|---|---|
| Control | TCP, newline-delimited JSON | Call control: media setup (offer/answer) and every other command |
| Service API | Planned: the service's own API, to be described (see "The service's own API") | Every other service |
| Media | UDP: each leg has an even RTP port and the next odd port for RTCP | Carriers, freeswitch, agents |

## Threads

| Thread | Name | Does | Cycle | Default scheduling |
|---|---|---|---|---|
| Main | `media-server` | Parses the command line, then serves TCP control. Holds the call and conference structures (`engine`) and binds new legs to RTP threads | 100 ms | `other:0` |
| RTP, a fixed set | `rtp-0` … `rtp-<N-1>` | Runs the legs bound to it: their sockets, adaptive jitter buffers, transcoding sources and destinations, DTMF, recordings. A mio poll on all its legs' sockets and its inbox | 20 ms | `fifo:70` (real-time) |
| IO | `io` | Writes every recording file | 100 ms | `other:5` |
| Log | `log` | Writes the logs; other threads never block on output (lines are dropped, and counted, if it falls far behind) | 100 ms | `other:10` |
| Watchdog | `watchdog` | Reports hung threads, nothing else | 100 ms | `fifo:80` (above RTP) |

Each thread class's policy and priority is set on the command line (`--sched-*`):
- `fifo:N` or `rr:N`: real-time, 1–99;
- `other:N` or `batch:N`: nice −20–19;
- `idle`;
- `inherit`.

Real-time needs `CAP_SYS_NICE` or an `RLIMIT_RTPRIO` allowance:
- systemd: `AmbientCapabilities=CAP_SYS_NICE` or `LimitRTPRIO=99`;
- Docker: `--cap-add SYS_NICE`.

Without it, the threads keep their normal scheduling and one warning is logged per thread
class.

### Cycles

Every thread runs in cycles of its own length, set per class (`--cycle-*-ms`). Each cycle
is timed on its thread (`cycle.rs`):
- **delta:** time since the previous cycle started; it should equal the cycle;
- **late:** how far past its due time the cycle started. More than a quarter of a cycle
  means the thread is **behind**: a warning with the delta, the lateness and the late
  cycles since the last warning (at most one a second per thread);
- **load:** time spent working rather than waiting, over 1 s windows.
  `CycleMeter::load_changed` tells when it moved by 5 points or more, and only then
  publishes it, logged for RTP threads as `rtp thread load changed`.

`stats` reports every thread's figures. The main thread's load isn't measured (its time
is spread over the async runtime), but its lateness shows a blocked event loop.

### RTP thread binding

`--rtp-threads` RTP threads start with the server (default: one per CPU core). Each new
leg is **bound** to one of them, where it stays unless rebalancing moves it:
- the thread with the lowest published load wins, comparing loads in 5-point steps;
- within a step, the thread with the fewest legs wins, then the lowest number. Leg counts
  change at once, while loads are measured over a second, so bursts of new legs spread out.

Each RTP thread, every cycle (20 ms, or a multiple of it with `--cycle-rtp-ms`), runs the
frames due, in two passes over its legs:
- **Receive:** the jitter buffer releases audio, which the leg's transcoding source
  decodes into a 20 ms frame, published for the leg's peers.
- **Send:** when the leg's stream is made here, the peers' frames are mixed (everyone hears
  the others, never themselves), DTMF is applied, and the leg's transcoding destination
  encodes and sends the result. If the leg is recorded, both channels go to the IO thread.

Legs bound to the same thread hear each other's frame from the same cycle. A thread that
is late runs up to 5 overdue cycles back to back. Further behind than that, it skips them
and **fills the blanks with silence**: each recording gets silent frames and a `silence`
event in its timeline, and each generated stream's RTP clock moves past the gap. So
recordings and timestamps keep to wall-clock time.

A two-party call whose legs agree on a codec skips the per-frame work. Each packet goes
straight out through the other leg's socket, unchanged, as it arrives.

### Rebalancing

Once a second the main thread checks whether any RTP thread is **hot**. A thread is hot
when either:
- its load is at or over `--rebalance-load-pct` (70%); or
- it started `--rebalance-late-cycles` (5) or more cycles late in the last second, which
  means its core is crowded even if its own work is light.

A hot thread gives up **one whole call or conference** per second: the one with the most
legs on it. All of that group's legs go to the coolest thread that isn't hot, so its legs
end up together on one thread. The limits:
- **The target:** when the source is hot by load, the target must be at least 10 points
  cooler. The moving legs, estimated at the source's load ÷ its legs each, must not make
  the target hot.
- **Pace:** each thread gives or takes at most one group per second, and a group that moved
  stays put for 10 s.
- **Off:** setting both flags to 0 turns rebalancing off.

Each move is logged (`rebalanced: moved to a cooler rtp thread`). `query` shows each leg's
`rtp_thread`, and `stats` shows the legs per thread.

**How a leg moves:**
1. its thread takes it out of its poll;
2. it hands the leg's whole state (jitter buffer, codecs, DTMF, recording, file being
   played) and its sockets to the new thread;
3. the new thread runs the leg from its next cycle.

The leg loses at most one frame, and it keeps its SSRC, sequence numbers and timestamps.

### Watchdog

Every watchdog cycle, each thread's last cycle is checked. A thread with no cycle for
`--watchdog-timeout-ms` (or two of its own cycles, if longer) is hung:
- an error marked `critical` is logged (also on stderr if the hung thread is `log`);
- `thread.stalled` is published.

When it cycles again, `thread.recovered` is published. The watchdog changes nothing.

### Shared state between threads

Legs bound to different RTP threads run at the same time, and a call's two legs may be on
different threads. Safe Rust rules out data races at compile time, so the design rules are
about ownership and ordering (details in `leg/mod.rs`):
- **One owner:** a leg's jitter buffer, codecs, DTMF state and recording are reachable only
  from its RTP thread. A move hands all of it over through the new thread's inbox, so there
  is one owner at every moment.
- **Messages:** commands reach a leg through the inbox of its current thread, found through
  the leg's route. They arrive in order and after its binding, even across a move:
  - the old thread holds the route's lock while it hands the leg over;
  - it forwards the leg's commands still queued behind it, then points the route at the
    new thread;
  - posting takes the same lock, so nothing slips in between.

  Threads never wait on each other; only the main thread waits when it unbinds a leg, with
  a 2 s timeout.
- **Shared, small and locked or atomic:** the frames a leg publishes (one writer, readers
  with their own cursors), its latched addresses, counters and the stats for `query`.
- **Configuration is replaced, not edited:** a new `LegConfig` is swapped in whole.

| Module | What it does |
|---|---|
| `engine` | Calls (two legs, SDP forwarded between them) and conferences (N participants, each negotiating with the server); merge; move a call into a conference |
| `leg` | RTP threads, binding and moving legs between them (`rtp_thread`), a bound leg's state (`state`), `inbound` (jitter buffer → transcoding source → DTMF in), `outbound` (mix → DTMF out → transcoding destination), `stream` (latching, counters) |
| `cycle` | Thread cycles: delta, lateness, load, `load_changed`, the registry of threads |
| `rebalance` | Which calls and conferences move off hot RTP threads, and where to |
| `watchdog` | The watchdog thread and its checks |
| `jitter` | Adaptive jitter buffer |
| `media::passthrough` | Forwards a peer's packets unchanged (payload type renumbered if needed) |
| `webrtc` | ICE-lite (`ice`, `stun`), DTLS-SRTP (`dtls`, OpenSSL and libsrtp), and the per-leg `Transport` that sorts the datagrams on a WebRTC leg's port |
| `media::transcode` | `Source` (a leg's incoming codec: holds a `codec::Decoder`) and `Destination` (a leg's outgoing codec and RTP stream: holds a `codec::Encoder`). Both codec types are enums with a variant per codec |
| `media::mixer` | Frame rings the legs publish into, and N−1 mixing |
| `recording` | The IO thread and the `.au` writer |
| `codec` | G.711 μ-law/A-law (pure Rust), Opus (bundled libopus); in test builds, the pulse test codec (`codec::pulse`) |
| `dtmf` | RFC 4733 events, in-band detection (Goertzel) and tone generation |
| `sdp`, `rtp` | Parsing and writing |

## Run

```sh
cargo run --release -- \
  --node-id media-1 --media-ip 10.0.1.11 --control-listen 127.0.0.1:2223 \
  --port-min 30000 --port-max 39999 \
  --recording-dir /var/lib/media-server/recordings
```

| Flag | Env | Default |
|---|---|---|
| `--node-id` | `MEDIA_NODE_ID` | `media-1` |
| `--control-listen` | `MEDIA_CONTROL_LISTEN` | `127.0.0.1:2223` |
| `--media-ip` | `MEDIA_IP` | `0.0.0.0` |
| `--advertise-ip` (required if `--media-ip` is unspecified; this address goes in the SDP) | `MEDIA_ADVERTISE_IP` | `--media-ip` |
| `--port-min` / `--port-max` | `MEDIA_PORT_MIN` / `MEDIA_PORT_MAX` | `30000` / `39999` |
| `--idle-timeout-secs` | `MEDIA_IDLE_TIMEOUT_SECS` | `60` |
| `--max-legs` | `MEDIA_MAX_LEGS` | `4000` |
| `--rtp-threads` (0: one per CPU core) | `MEDIA_RTP_THREADS` | `0` |
| `--recording-dir` | `MEDIA_RECORDING_DIR` | `recordings` |
| `--prompt-dir` (files `play` may use) | `MEDIA_PROMPT_DIR` | `prompts` |
| `--jitter-delay-ms` (starting and smallest play-out delay) | `MEDIA_JITTER_DELAY_MS` | `20` |
| `--jitter-max-delay-ms` | `MEDIA_JITTER_MAX_DELAY_MS` | `200` |
| `--jitter-capacity-bytes` (packets the buffer can hold) | `MEDIA_JITTER_CAPACITY_BYTES` | `192000` |
| `--log-json` | `MEDIA_LOG_JSON` | off |
| `--sched-main` / `--sched-rtp` / `--sched-io` / `--sched-log` / `--sched-watchdog` | `MEDIA_SCHED_MAIN` / `_RTP` / `_IO` / `_LOG` / `_WATCHDOG` | `other:0` / `fifo:70` / `other:5` / `other:10` / `fifo:80` |
| `--cycle-main-ms` / `--cycle-rtp-ms` (a multiple of 20) / `--cycle-io-ms` / `--cycle-log-ms` / `--cycle-watchdog-ms` | `MEDIA_CYCLE_MAIN_MS` / `_RTP_MS` / `_IO_MS` / `_LOG_MS` / `_WATCHDOG_MS` | `100` / `20` / `100` / `100` / `100` |
| `--watchdog-timeout-ms` | `MEDIA_WATCHDOG_TIMEOUT_MS` | `1000` |
| `--rebalance-load-pct` / `--rebalance-late-cycles` (0 turns a trigger off) | `MEDIA_REBALANCE_LOAD_PCT` / `_LATE_CYCLES` | `70` / `5` |

The log level comes from `RUST_LOG` (default `info`).

The build links the system OpenSSL (`openssl-devel` or `libssl-dev`). Some libraries are built from source:
- libopus, by `opusic-sys` (needs cmake and a C compiler);
- libsrtp 2.3.0, bundled with `srtp2-sys` (needs make, a C compiler and libclang). The
  system's libsrtp 2.6 could be used instead once `libsrtp-devel` is installed.

On SIGTERM or Ctrl-C the server stops taking commands, ends every call and conference
(finishing recordings), stops the RTP threads, the watchdog and the IO thread, and exits.

## Protocol

The same JSON commands work on both transports. Each request is one object with a `cmd`
field. The optional `id` is echoed back. A response is `{"id", "node", "ok": true,
"result"}` or `{"id", "node", "ok": false, "error": {"code", "message"}}`.

### Calls (two legs, A and B)

| cmd | Fields | Result |
|---|---|---|
| `offer` | `call_id`, `sdp` (the offering leg's offer); optional `leg` (`"a"`, the default, or `"b"`), `record: true` (records the offering leg), `webrtc: true` (the other leg is a WebRTC endpoint) | `to`: the other leg. `sdp`: the offer to send to it. `call`: IP and ports. Creates the call if needed |
| `answer` | `call_id`, `sdp` (the other leg's answer); optional `leg`, which must name the answering leg | `to`: the offering leg. `sdp`: the answer to send to it, plus `mode` and `codecs` |
| `create` | `call_id`; optional `a.remote` and `b.remote` | IP and ports. No SDP: pure passthrough. Idempotent |
| `update` | `call_id`; optional `a.remote` and `b.remote` | Call stats. Ignored for a leg that has already latched |
| `delete` | `call_id` | Final call stats, with finished recordings |
| `query` | `call_id` | Call stats: `mode`, `codecs`, and per leg its `rtp_thread`, streams, jitter buffer and recording |
| `move_to_conference` | `call_id`, `conference_id`; optional `a_participant_id` and `b_participant_id` (default `<call_id>-a` / `-b`) | The participants. The call is gone; its legs keep their ports and recordings |

**Negotiation.** Either leg may offer: leg A at the start, either leg later (a
re-INVITE: hold, a new address, another codec). The other leg answers. Legs keep their
ports, and the `o=` version goes up with every SDP. Below, "the offerer" is the leg whose
offer it is.

- **The offer to the other leg:** all the offerer's audio formats, as they are and in its
  order, including codecs we can't decode (e.g. G.729). If the offer has something we
  decode, the other codecs we can transcode to are added. Then `telephone-event` for each
  clock rate (the offerer's own, or ours where we can generate DTMF).
- **The answer's choice:** the answering leg's first audio format is its codec.
  - **Passthrough:** the offerer uses the same codec if it offered it. This works for any
    codec.
  - **Transcoding:** otherwise the offerer uses its first codec we decode, and the call
    transcodes between PCMU, PCMA and Opus.
  - **No usable codec:** the answer picked something the offerer didn't offer and we
    can't transcode, so it fails with `unsupported_codec`.
- **Other `m=` lines** in an offer (video, data channels) are rejected in the answer, in
  their places (port 0).

### Conferences

| cmd | Fields | Result |
|---|---|---|
| `join` | `conference_id`, `participant_id`, `sdp` (the participant's offer); optional `record: true`, and `muted: true` (joins not heard, e.g. a supervisor listening) | `sdp`: the answer for the participant, plus its ports and `codec`, and `conference_created` |
| `set_mode` | `conference_id`, `participant_id`, `mode` (`listen`, `whisper`, `barge`); `whisper_to` (a participant) for `whisper` only | `mode`, `whisper_to` |
| `leave` | `conference_id`, `participant_id` | The participant's final stats. The conference ends with its last participant |
| `reoffer` | `conference_id`, `participant_id`, `sdp` (the participant's new offer: a re-INVITE) | `sdp`: the answer for it, and `codec`. Same ports, recording and mode |
| `conference_query` | `conference_id` | Its participants and their stats |
| `conference_delete` | `conference_id` | Every participant's final stats |
| `conference_merge` | `from`, `into` | Moves every participant of `from` into `into` (same ports, same recordings) and ends `from` |

How conferences behave:
- **Mixing:** each participant hears the mix of all the others.
- **Codecs:** participants may use different codecs (PCMU, PCMA, Opus). Mixing needs
  decoding, so a codec we can't decode is refused (`unsupported_codec`).
- **A participant alone:** hears silence. The stream keeps flowing, so NAT bindings and
  RTP timeouts stay alive, and the conference carries on.
- **Supervisor modes:** the supervisor joins with `muted: true`, which is **listen**: they
  hear everyone, and nobody hears them. `set_mode` switches without rejoining:
  - **whisper:** only `whisper_to` (the agent) hears the supervisor;
  - **barge:** everyone hears everyone;
  - **listen:** back to muted.

  To supervise a two-party call, move it into a conference first (`move_to_conference`;
  the customer and agent keep their ports, codecs and recordings).

### Any leg

A leg is named by `call_id` + `leg` (`"a"`/`"b"`), or by `conference_id` +
`participant_id`.

| cmd | Fields | Result |
|---|---|---|
| `start_recording` | a leg (for calls `leg` defaults to `a`) | Recording status: `path`, `timeline`, `active`, `started_at` (RFC 3339), `started_at_ms`, `duration_ms`, `bytes` |
| `stop_recording` | a leg | Final recording status |
| `send_dtmf` | a leg, `digits` (`0-9*#A-D`, up to 64); optional `duration_ms` (100) and `pause_ms` (70) | `{"queued", "method": "rfc4733" or "inband"}` |
| `mute` / `unmute` | a leg | The leg, `muted`, and `changed` (false if it already was) |
| `play` | a leg, `file` (a path inside `--prompt-dir`) | The leg, `file`, `duration_ms` |
| `stop_play` | a leg | `stopped` (false if nothing was playing), `file` |

**Mute is a soft mute:** the leg's audio is replaced by silence, packet by packet. It still
hears the others.
- **In a passthrough call:** each packet still goes out with the same sequence number,
  timestamp and SSRC, but its payload is silence: the G.711 silence code, or a 20 ms Opus
  silence frame. The other side's stream never stops, so there's no RTP-timeout hang-up and
  NAT bindings stay open. A codec we can't encode (e.g. G.729) can't carry silence, so its
  packets are dropped while muted.
- **Mixed or generated audio:** the leg is mixed in as silence.
- **Recording:** the leg's own channel has silence while it is muted.
- **Digits:** they are still detected and reported in `dtmf` events (so card digits can be
  taken while muted), but not passed on to the other side.

**Playing a file:**
- **Where it comes from:** WAV or `.au`, mono, 8 kHz (16-bit PCM, μ-law or A-law), from
  `--prompt-dir`. Paths leading out of it are refused (`bad_request`), and missing files
  are `not_found`.
- **Where it goes:** it's decoded on the main thread and mixed into what the leg hears, 20 ms
  at a time. So the leg's recording gets it in its heard channel, frame by frame. The
  timeline marks `play` and `play_finished`.
- **On a call leg:** that leg's stream is made here from then on, as with `send_dtmf`.
- **Replacing:** a new `play` replaces a file still playing.

### General

| cmd | Result |
|---|---|
| `ping` | `{"pong": true}` |
| `list` | `{"calls": [...], "conferences": [...]}` |
| `stats` | Node, uptime, calls, conferences, legs, free and total port pairs, and `threads`: per thread its `cycle_ms`, `cycles`, `late_cycles`, `max_late_ms` (last second), `load_pct`, `since_last_cycle_ms`, and for RTP threads `legs` |

Error codes: `bad_request` (including invalid SDP), `not_found`, `full`, `no_ports`,
`unsupported_codec`, `invalid_state`, `unavailable` (shutting down), `internal`.

## Media behaviour

- **Latching and symmetric RTP:** a leg's destination starts as the address from SDP (or
  `create`/`update`). The first packet received replaces it with the real source, which
  fixes NAT. Packets from any other source are dropped. Each leg sends from its own
  port.
- **Passthrough:** in a call whose legs share a codec and DTMF method, packets go straight
  through, unchanged except for payload type numbers. RTCP is relayed too.
  - This also covers codecs we can't decode.
  - `telephone-event` is dropped toward a leg that didn't negotiate it.
- **Generated streams:** used for conferences and transcoded calls, and toward a leg that
  is sent DTMF.
  - The stream has its own SSRC, sequence numbers and timestamps, and a packet every
    20 ms (silence included).
  - The endpoint's RTCP isn't relayed for these streams. Instead the server sends its own
    **RTCP Sender Reports** (with SDES CNAME): after 1 s, then every 5 s.
  - Each report gives the wall-clock (NTP) time matching the stream's RTP timestamp, plus
    the packets and bytes sent.
- **Timestamps:**
  - **Every RTP packet carries an RTP timestamp** (media clock: 8 kHz for G.711, 48 kHz
    for Opus). The jitter buffer orders and plays out by it, and generated streams advance
    it by exactly 20 ms per frame.
  - **Wall-clock time:** in passthrough the endpoints' own RTCP is relayed; in generated
    streams the server's Sender Reports give it.
- **Jitter buffer:** adaptive, one per leg, used wherever audio is decoded.
  - **Delay:** starts at 20 ms. The target follows the measured interarrival jitter
    (RFC 3550), up to 200 ms.
  - **Capacity:** holds up to 192,000 bytes, so late and reordered packets are kept.
  - **Losses:** concealed (Opus with its own concealment).
  - **Running dry:** the play-out waits, which adds delay.
  - **Running full:** it skips a frame now and then.
  - Stats are under each leg's `jitter`.
- **DTMF:**
  - **Reporting:** every digit a leg sends is reported as a `dtmf` event, whether it
    arrives as RFC 4733 events or as tones in the audio.
  - **In calls:** digits are converted toward the other leg when its method differs
    (events become tones, tones become events).
  - **`send_dtmf`:** plays digits into a leg as RFC 4733 events if it negotiated them,
    tones otherwise.
- **Recording:** a recording belongs to a leg and goes with it into a conference or
  through a merge.
  - Left channel: what the leg says. Right channel: what it hears.
  - Format: Sun `.au`, 16-bit big-endian PCM, 8 kHz, stereo.
  - File name: `<recording-dir>/<call>-<leg>-<unix ms>.au` or
    `<conference>-<participant>-<unix ms>.au`.
  - **Timestamped** in two places:
    - **The `.au` header annotation:** `start=<RFC 3339>;start_ms=<unix ms>;rate=8000;channels=2;left=sent;right=heard;leg=<call>/<leg>`.
    - **A timeline** `<same name>.json`, written when the recording ends: start and stop
      times, frame size, wall-clock anchors every 5 s (frame number ↔ capture time), and
      events with their offset and time (`moved` into a conference or through a merge,
      `dtmf` with each digit, `play` and `play_finished` for files played into the leg, and
      `silence` where missed frames were filled).
- **Idle timeout:** a call with no packets on either leg, or a conference participant with
  none, for `--idle-timeout-secs` is ended (reason `idle_timeout`).

## Events

Each event has `type`, `node` and `at_ms`, and a key: its call or conference id, or the
node for thread events. Until the service's own API delivers them, they go to the log at
debug level (they carry phone-call details and digits), and tests capture them.

| type | Fields |
|---|---|
| `call.created` | `call` (IP, ports) |
| `call.ended` | `call_id`, `reason` (`deleted`, `idle_timeout`, `shutdown`), `call` (final stats) |
| `call.moved` | `call_id`, `conference_id`, `participants` |
| `conference.created` | `conference_id` |
| `participant.joined` | `conference_id`, `participant_id`, `codec`, `muted` |
| `participant.left` | `conference_id`, `participant_id`, `reason` (`left`, `deleted`, `idle_timeout`, `shutdown`), `participant` (final stats) |
| `conference.merged` | `from`, `into`, `participants` |
| `conference.ended` | `conference_id`, `reason` (`empty`, `deleted`, `merged`, `shutdown`) |
| `recording.started` / `recording.stopped` | the leg (`call_id` + `leg`, or `conference_id` + `participant_id`), `recording`; `stopped` also has `reason` (`requested`, `ended`) |
| `dtmf` | the leg, `digit`, `duration_ms`, `source` (`rfc4733` or `inband`) |
| `leg.muted` / `leg.unmuted` | the leg |
| `participant.mode_changed` | `conference_id`, `participant_id`, `mode`, `whisper_to` (whisper only) |
| `play.started` | the leg, `file`, `duration_ms` |
| `play.finished` | the leg, `file`, `reason` (`completed`, `stopped`, `replaced`), `played_ms`, `duration_ms` |
| `thread.stalled` | `thread`, `cycle_ms`, `stalled_ms` (keyed by node) |
| `thread.recovered` | `thread`, `stalled_ms` (keyed by node) |

## The service's own API (planned)

parity-ms will get its own API, to be described. Kafka was removed on 2026-10-17. The API
will carry what the TCP control channel carries today, plus what Kafka used to deliver:
- **Commands and responses:** the commands in "Protocol" above, with their results and
  error codes.
- **Events:** the table above, with an agreed delivery guarantee.

To settle when it's described:
- **Transport.**
- **Reaching a call's node:** a call or conference lives on one node.
- **Latency:** offer/answer sits in call setup.
- **Event delivery and ordering:** today they are ordered per call or conference.
- **Authentication:** the control port has none.

Until then, the TCP control channel is the way in.

## WebRTC legs

A leg is WebRTC when its SDP is `UDP/TLS/RTP/SAVPF` (a browser's offer or answer), or when
`offer` says the other leg is (`webrtc: true`, e.g. a carrier call going to an agent's
browser). A WebRTC leg talks to any other leg, plain or WebRTC: what it sends is decrypted
on arrival, what goes to it is encrypted on the way out, and passthrough and transcoding
work as usual in between.

- **One port** for everything (RFC 7983): STUN, DTLS, SRTP, and SRTCP (`rtcp-mux`,
  RFC 5761). Our SDP has `a=ice-lite`, BUNDLE, `a=mid`, our ICE credentials and
  fingerprint, and one host candidate (the advertised address and the leg's port).
- **ICE-lite:** we answer the browser's connectivity checks and send media to the address
  it nominates; the first checked address carries media until then. A later nomination
  moves it (a network change). TURN relays (coturn) are just another source address.
- **DTLS:**
  - **Certificate:** one self-signed certificate (ECDSA P-256) per server start,
    announced in our SDP.
  - **Roles:** we answer a browser's offer `passive` (it connects), and offer `actpass`.
  - **The peer's certificate** must match the fingerprint in its SDP. A browser answering
    our offer may connect before its answer reaches us: the keys are held until the
    fingerprint arrives.
- **SRTP:** libsrtp. Profiles are AES-128-GCM and AES-CM-128-HMAC-SHA1-80. Nothing is
  sent to the leg until DTLS has keyed SRTP; `query` shows `srtp: "pending"` or
  `"active"`.
- **Checked against aiortc,** an independent WebRTC stack: ICE completed, DTLS-SRTP keyed,
  and audio went both ways between it and a plain PCMU phone in one conference.

## Not supported yet

- WebRTC extras:
  - SDES-SRTP (`a=crypto`);
  - ICE over TCP;
  - RTCP feedback (NACK, transport-cc);
  - more than one audio stream;
  - video and data channels (rejected in answers).
- Wideband: decoding, mixing and recording run at 8 kHz.
- RTCP Receiver Reports (we send Sender Reports only), and RTCP-based quality stats.
- CPU pinning of RTP threads (only policy and priority are set).
- Authentication on the control port (keep it on localhost or a private network). Digits
  in `dtmf` events are in clear, which needs care for card payments (PCI).

## Tests

```sh
cargo test        # unit tests, plus end-to-end tests over real TCP and UDP sockets
```

### The test codec

`x-pulse/8000` is a codec for the media server's own tests: **pulse positions**. Each
20 ms packet lists the frame's pulses (its non-zero samples):

```text
count (1 byte), then count × [offset 0–159 (1 byte), amplitude (2 bytes, big-endian)]
```

- **What it's for:** silence is one byte, and any frame comes back exactly. A test sends a
  few impulses and checks where and how loud they come out, to the sample: delay, lost or
  repeated frames, mixing (two speakers' pulses add up where expected), recording
  alignment, and positions through transcoding. G.711 quantises and Opus smears; this
  doesn't.
- **Test builds only:** it exists only with the `test-codec` Cargo feature.
  - `cargo test` turns the feature on (the crate is its own dev-dependency, with the
    feature).
  - `cargo build` and `cargo build --release` leave the codec out.
  - `cargo build --all-targets` builds the tests too, so its binary has the codec.
- **And a flag:** even in such a build, the server accepts the codec only with
  `--test-codec` (`MEDIA_TEST_CODEC`; `Config::test_codec`). Without it, `x-pulse` is a
  codec it doesn't know.
- **Never offered:** it is never added to offers as something to transcode to. It is used
  only when an endpoint offers it.
- **Test helpers:** `codec::pulse::packet(&[(offset, amplitude)])` builds a payload, and
  `codec::pulse::pulses(payload)` reads one back. `tests/pulse.rs` uses them end to end.
