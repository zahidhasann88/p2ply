# Changelog

## 0.2.1 — 2026-10-25

Fixed:

- Automatic reconnection could stall when only one peer noticed the drop (seen on Linux): the peer with the larger ID now also reconnects, after a 2-second head start for the other side. The 0.2.0 package on npm was published without this fix.

## 0.2.0 — 2026-10-25

Added:

- **Automatic peer reconnection.** When a connected peer drops (network change, signaling server restart), P2Ply rebuilds the connection with backoff for up to `peerReconnect.windowMs` (default 60 s). New options `peerReconnect.auto` (default `true`) and `peerReconnect.windowMs`; new peer state `reconnecting`.
- **Sends wait during a reconnect.** `send()` and `request()` wait for the new connection (up to their timeout) instead of rejecting. Acknowledged messages and requests are re-sent after the reconnect and deduplicated by the receiver; repeated requests get the original response.
- **Resumable file transfers.** Transfers pause during a drop and continue from the receiver's last chunk; the SHA-256 check still covers the whole file.
- Peers that `leave()` or `close()` tell connected peers first (`bye`), so those do not try to reconnect.

Changed:

- `peerReconnect.maxAttempts` now defaults to `5` and applies to each drop.
- `reconnectPeer()` rebuilds the connection even if it looks healthy, and keeps file transfers.
- A DataChannel closed by the remote peer now fails the connection immediately instead of waiting for ICE to time out.
- The DataChannel protocol gains `bye`, `file-resume`, and `file-resume-ack` messages. Peers on 0.1.x close the connection when they receive one, so upgrade both sides.

## 0.1.1 — 2026-10-25

Fixed:

- `getDiagnostics()` now reports `selectedCandidateType`, `roundTripTimeMs`, `bytesSent`, and `bytesReceived`. In 0.1.0, `RTCStatsReport` entries were read incorrectly, so these fields were missing and `selectedCandidateType` was always `'unknown'`.
- The `RTCPeerConnectionLike.getStats()` adapter type is now `Promise<ReadonlyMap<string, unknown>>`, matching `RTCStatsReport`. Custom `rtcFactory` implementations must return a Map-like report.

## 0.1.0 — 2026-10-25

Initial release:

- Strict TypeScript P2Ply API with browser-native WebRTC and WebSocket signaling.
- Versioned, Zod-validated signaling and DataChannel envelopes.
- Self-hosted in-memory signaling server with room registry, authorization hooks, origin restrictions, heartbeat cleanup, limits, and health endpoints.
- Explicit peer lifecycle, polite/impolite offer collision handling, signaling backoff, bounded peer reconnection, typed events, and diagnostics.
- Bounded message queue, acknowledgements, request/response, incremental chunked file transfer, cancellation, and SHA-256 verification.
- Optional Node adapter using `@roamhq/wrtc` and `ws`.
- Unit, local server/Node WebRTC integration, and Playwright browser integration tests; examples and playground.
