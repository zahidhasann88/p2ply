# Security policy

## Reporting a vulnerability

**Please do not report security issues in public GitHub issues.**

Use GitHub's **private vulnerability reporting** (the "Report a vulnerability" button under the repository's _Security_ tab). If that is unavailable, contact the maintainers through a private channel linked from the repository.

Please include:

- The affected version
- What an attacker could do (impact)
- Steps to reproduce
- A suggested fix, if you have one

Do not include real credentials or private user data. We will acknowledge your report, investigate, coordinate a fix, and publish an advisory when appropriate.

## What P2Ply protects, and what it does not

**Where data goes**

- Your messages and files travel directly between peers over WebRTC DataChannels.
- The signaling server only sees peer IDs, room membership, and the connection handshake (SDP and ICE candidates). It never receives your messages or files.
- The included server does not store or log payloads, SDP, ICE candidates, or auth tokens.

**Encryption is not identity**

- DataChannels are encrypted with DTLS, but DTLS does **not** prove who the other peer is.
- Peer IDs are random session labels, not cryptographic identities.
- P2Ply does not sign signaling messages or pin identity keys. A malicious or compromised signaling server could tamper with the handshake and sit in the middle of a connection.
- If your app needs verified identities, add your own authenticated identity layer and check peers before allowing sensitive actions.

## Deploying safely

| Do this                                                               | Why                                                                        |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Use `wss://` (TLS) everywhere except local development                | `authToken` and the handshake are sent over the signaling connection       |
| Set `allowedOrigins` for browser apps                                 | Blocks other websites. Not authentication: non-browser clients can fake it |
| Implement `authenticate`, `authorizeJoin`, and `authorizeSignal`      | Controls who can connect, join which rooms, and reach which peers          |
| Use short-lived, scoped tokens                                        | Limits damage if a token leaks                                             |
| Generate TURN credentials on your backend, short-lived                | Never put permanent TURN secrets in browser code                           |
| Validate everything peers send (`validateMessage`)                    | A well-formed message can still contain hostile content                    |
| Treat room IDs and peer IDs as labels, not passwords                  | Anyone who knows them could try to use them                                |
| Tune message, room, peer, transfer, and rate limits for your workload | Defaults are a starting point                                              |
| Add network-level connection and rate limits for public servers       | The built-in limits are per process                                        |
| Use a shared `PeerRegistry` before running multiple server processes  | The default registry is in-memory and local to one process                 |
| Never log secrets, SDP, ICE candidates, or payloads                   | They can reveal network addresses and private data                         |

## Built-in protections

- **Random IDs:** peer, session, and transfer IDs come from a cryptographically secure random generator.
- **Strict validation:** every signaling and DataChannel message is checked against a versioned Zod schema. No `eval` or unsafe deserialization.
- **Server limits:** payload size limits, per-connection rate limiting (token bucket), room and connection caps, handshake timeout, heartbeat cleanup, and graceful shutdown.
- **Access hooks:** optional `authenticate`, `authorizeJoin`, and `authorizeSignal` on the server, and `authorizePeer` on the client.
- **Bounded resources:** origin allowlists, bounded queues, message and file size limits, bounded request/transfer state, and cleanup of disconnected peers.
- **File integrity:** SHA-256 verification of every file using a standard incremental hash. No custom cryptography.
- **Quiet logging:** by default, neither client nor server logs SDP, ICE candidates, auth tokens, or payloads.

## Known limits

This release does **not** provide:

- Application-level encryption beyond WebRTC's DTLS
- Signed or verified peer identities
- Durable (persisted) message delivery
- Replay protection across restarts. The duplicate-message cache is in-memory and bounded; it is not a security control.
- Distributed rate limiting, abuse billing, or server-side credential storage

A public signaling server needs production TLS, authentication, authorization, quotas, and monitoring suited to how it is deployed.
