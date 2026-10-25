# Changelog

## 0.1.0 — 2026-10-25

Initial release:

- Strict TypeScript P2Ply API with browser-native WebRTC and WebSocket signaling.
- Versioned, Zod-validated signaling and DataChannel envelopes.
- Self-hosted in-memory signaling server with room registry, authorization hooks, origin restrictions, heartbeat cleanup, limits, and health endpoints.
- Explicit peer lifecycle, polite/impolite offer collision handling, signaling backoff, bounded peer reconnection, typed events, and diagnostics.
- Bounded message queue, acknowledgements, request/response, incremental chunked file transfer, cancellation, and SHA-256 verification.
- Optional Node adapter using `@roamhq/wrtc` and `ws`.
- Unit, local server/Node WebRTC integration, and Playwright browser integration tests; examples and playground.
