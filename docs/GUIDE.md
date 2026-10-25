# P2Ply Guide

The full reference. For a quick introduction, read the [README](../README.md) first.

- [1. How it works](#1-how-it-works)
- [2. API reference](#2-api-reference)
- [3. Events](#3-events)
- [4. Configuration](#4-configuration)
- [5. Delivery guarantees](#5-delivery-guarantees)
- [6. File transfer](#6-file-transfer)
- [7. Signaling server](#7-signaling-server)
- [8. STUN and TURN](#8-stun-and-turn)
- [9. Node.js](#9-nodejs)
- [10. Compatibility](#10-compatibility)
- [11. Security](#11-security)
- [12. Troubleshooting](#12-troubleshooting)
- [13. Limitations](#13-limitations)
- [14. Protocol details](#14-protocol-details)

---

## 1. How it works

P2Ply has two separate paths:

| Path              | Transport               | Carries                                                   |
| ----------------- | ----------------------- | --------------------------------------------------------- |
| **Control plane** | WebSocket → your server | Registration, room presence, SDP offer/answer, ICE        |
| **Data plane**    | WebRTC DataChannel      | Your messages, requests, and files, directly peer-to-peer |

The signaling server only introduces peers. It never receives your application messages or files.

**Connection flow**

1. `start()` opens the WebSocket, sends `hello` with a random peer ID, and waits for `welcome`.
2. `join(room)` asks the server for room membership. The server replies with the current peer list and tells existing members about the newcomer (`peer:join`).
3. `connect(peerId)` creates an `RTCPeerConnection` and an ordered, reliable DataChannel, then exchanges offer/answer/ICE through the server.
4. When the DataChannel opens, `connect()` resolves and the peer's state becomes `connected`. From here on, data goes peer-to-peer.

If both sides call `connect()` at the same time, P2Ply resolves the collision automatically (the WebRTC "perfect negotiation" pattern), so both calls succeed with a single connection.

---

## 2. API reference

Entry points:

| Import         | Exports                                                                 |
| -------------- | ----------------------------------------------------------------------- |
| `p2ply`        | `P2Ply`, error classes, `validateP2PlyOptions`, types                   |
| `p2ply/node`   | `NodeP2Ply`, `createNodeP2Ply`                                          |
| `p2ply/server` | `createSignalingServer`, `startSignalingServer`, `InMemoryPeerRegistry` |

### Lifecycle

| Method                       | Resolves when                                                                             |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| `new P2Ply(options)`         | Synchronous. Throws `ConfigurationError` if options are invalid.                          |
| `await start()`              | The server sent a valid `welcome`. Does not join any room.                                |
| `await join(roomId)`         | The server confirmed membership. Room IDs: 1–128 chars, no edge spaces or control chars.  |
| `await leave(roomId)`        | The server confirmed you left.                                                            |
| `await connect(peerId)`      | The DataChannel is open. Rejects on denial, negotiation failure, or timeout.              |
| `await reconnectPeer(id)`    | A new connection is open. Rebuilds it even if it looks healthy; transfers resume.         |
| `await reconnectSignaling()` | Signaling is reopened and rooms are rejoined. Dropped peers then reconnect automatically. |
| `await close()`              | Everything is released. Closing is final: create a new instance to start again.           |

### Inspection

- `mesh.peerId`: this instance's random ID.
- `mesh.state`: `idle | starting | started | closing | closed`.
- `mesh.signalingState`: `connecting | connected | reconnecting | disconnected | closed`.
- `mesh.roomIds`: rooms you have joined.
- `getPeers()` / `getPeer(id)`: discovered peers with their state (`discovered`, `connecting`, `connected`, `disconnected`, `reconnecting`, `failed`, `closed`).
- `await getDiagnostics()`: see below.

### Messaging

| Method                                      | Resolves when                                                                               |
| ------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `send(peerId, data)`                        | The frame was handed to `RTCDataChannel.send()` (after any backpressure wait).              |
| `send(peerId, data, { acknowledge: true })` | …and the remote P2Ply confirmed it parsed the message.                                      |
| `broadcast(data, options?)`                 | `send` succeeded for every peer connected at call time. Returns `Map<peerId, SendReceipt>`. |
| `request(peerId, data, { timeoutMs? })`     | The remote side called `respond(value)`. Rejects on `reject()` or timeout.                  |

`send()` returns a `SendReceipt`: `{ messageId, acceptedAt, transmittedAt, acknowledgedAt? }`.

`send()` rejects with:

| Code                   | Cause                                              |
| ---------------------- | -------------------------------------------------- |
| `PEER_NOT_CONNECTED`   | No open DataChannel to that peer                   |
| `MESSAGE_TOO_LARGE`    | Encoded message exceeds `maxMessageBytes`          |
| `QUEUE_OVERFLOW`       | The per-peer send queue is full                    |
| `SERIALIZATION_FAILED` | Data is not JSON-serialisable (or `undefined`)     |
| `INVALID_MESSAGE`      | Data failed your `validateMessage` function        |
| `REQUEST_TIMEOUT`      | `acknowledge: true` and no acknowledgement in time |

Responding to requests:

```ts
mesh.on('request', ({ peerId, data, respond, reject }) => {
  if (isValid(data)) void respond({ ok: true });
  else void reject('Bad request');
});
```

Call `respond` or `reject` exactly once. If no `request` listener exists, P2Ply rejects the request for you.

### Typed messages

```ts
interface Chat {
  kind: 'chat';
  text: string;
}
const isChat = (v: unknown): v is Chat =>
  typeof v === 'object' &&
  v !== null &&
  (v as Chat).kind === 'chat' &&
  typeof (v as Chat).text === 'string';

const mesh = new P2Ply<Chat>({ signalingUrl, validateMessage: isChat });
mesh.on('message', ({ data }) => data.text); // data is Chat
```

The generic type provides compile-time types only. `validateMessage` checks remote data at runtime, and remote data should always be treated as untrusted.

### Diagnostics

`await mesh.getDiagnostics()` returns:

```ts
{
  peerId, meshState, signalingState, signalingReconnectAttempts, joinedRooms,
  peers: [{
    peerId, state, roomIds, transport: 'webrtc-datachannel',
    connectionState?, iceConnectionState?, dataChannelState?,
    bytesSent?, bytesReceived?, roundTripTimeMs?,
    selectedCandidateType?,      // 'host' | 'srflx' | 'prflx' | 'relay' | 'unknown'
    queuedMessages, queuedBytes, activeTransfers, reconnectAttempts, lastConnectionError?
  }]
}
```

Measurements come from `RTCPeerConnection.getStats()`. Fields the platform does not report are **omitted**; they are never filled with fake zeros. `selectedCandidateType: 'relay'` means traffic is going through TURN.

### Errors

Every error is a `P2PlyError` with a `code` property. Subclasses: `ConfigurationError`, `QueueOverflowError`, `TransferError`. See `P2PlyErrorCode` in `src/errors.ts` for the full list.

---

## 3. Events

`mesh.on(event, listener)` returns an unsubscribe function. `once()` and `off()` are also available.

| Event             | Payload                                                           |
| ----------------- | ----------------------------------------------------------------- |
| `peer:join`       | `{ peerId, roomId }`: a peer appeared in a room                   |
| `peer:leave`      | `{ peerId, roomId }`                                              |
| `peer:state`      | `{ peerId, roomId, state, error? }`                               |
| `signaling:state` | `{ state, attempt?, error? }`                                     |
| `message`         | `{ peerId, messageId, data }`                                     |
| `request`         | `{ peerId, requestId, data, respond(), reject() }`                |
| `file:offer`      | `{ peerId, transferId, metadata, accept(), reject() }`            |
| `file:progress`   | `{ peerId, transferId, direction, transferredBytes, totalBytes }` |
| `file:complete`   | `{ peerId, transferId, result }`                                  |
| `file:error`      | `{ peerId, transferId, error }`                                   |
| `error`           | `P2PlyError`                                                      |

Exceptions thrown inside your listeners are caught and logged, so one bad listener cannot break the mesh.

---

## 4. Configuration

All options are validated with Zod in the constructor. Unknown options are rejected.

| Option                                    | Default         | Notes                                                                         |
| ----------------------------------------- | --------------- | ----------------------------------------------------------------------------- |
| `signalingUrl`                            | required        | `ws://` or `wss://`                                                           |
| `authToken`                               | none            | Sent in the signaling `hello`; checked by server `authenticate`               |
| `peerId`                                  | random          | Must be a UUID if you set it yourself                                         |
| `rtcConfiguration`                        | `{}`            | Standard `RTCConfiguration`: STUN/TURN servers etc.                           |
| `autoConnect`                             | `false`         | Automatically `connect()` to every discovered peer                            |
| `reconnect.enabled`                       | `true`          | Signaling WebSocket only                                                      |
| `reconnect.maxAttempts`                   | `10`            | Exponential backoff with jitter                                               |
| `reconnect.minDelayMs` / `maxDelayMs`     | `250` / `30000` |                                                                               |
| `reconnect.factor` / `jitter`             | `2` / `0.2`     | Backoff multiplier and random spread (0–1)                                    |
| `peerReconnect.auto`                      | `true`          | Reconnect dropped peers automatically (see [Reconnection](#reconnection))     |
| `peerReconnect.windowMs`                  | `60000`         | How long to keep trying after a drop before giving up                         |
| `peerReconnect.maxAttempts`               | `5`             | Connection attempts per drop                                                  |
| `peerReconnect.minDelayMs` / `maxDelayMs` | `250` / `5000`  | Same backoff settings as `reconnect` (also `factor`, `jitter`)                |
| `connectionTimeoutMs`                     | `15000`         | Max wait for the DataChannel to open                                          |
| `negotiationTimeoutMs`                    | `15000`         | Max time for offer/answer/ICE                                                 |
| `disconnectGraceMs`                       | `10000`         | How long `disconnected` may last before becoming `failed`                     |
| `signalingTimeoutMs`                      | `10000`         | Welcome, join, and leave timeouts                                             |
| `requestTimeoutMs`                        | `15000`         | Default for `request()` and acknowledgements                                  |
| `maxMessageBytes`                         | `64 KiB`        | Max encoded message size, both directions                                     |
| `maxBufferedBytes`                        | `1 MiB`         | DataChannel high-water mark (backpressure)                                    |
| `maxQueuedMessages`                       | `256`           | Per-peer pending send queue                                                   |
| `maxQueuedBytes`                          | `1 MiB`         | Per-peer pending send queue                                                   |
| `maxPendingRequests`                      | `256`           | Outstanding requests/acks                                                     |
| `maxFileBytes`                            | `100 MiB`       | Applies to both sender and receiver                                           |
| `fileChunkSize`                           | `16 KiB`        | Must fit inside `maxMessageBytes` (plus 23 framing bytes)                     |
| `maxConcurrentTransfers`                  | `8`             |                                                                               |
| `transferTimeoutMs`                       | `30000`         | Offer / per-chunk / completion inactivity timeout                             |
| `fileSinkFactory`                         | memory          | Where received chunks go (see [File transfer](#6-file-transfer))              |
| `authorizePeer`                           | allow all       | `({ peerId, roomId, direction }) => boolean`, checked before every connection |
| `validateMessage`                         | none            | Runtime type guard for incoming and outgoing messages                         |
| `logger`                                  | no-op           | `{ log(level, message, context) }`; never receives SDP, ICE, or tokens        |
| `rtcFactory` / `webSocketFactory`         | browser globals | Advanced: inject custom implementations                                       |

---

## 5. Delivery guarantees

- DataChannels are **reliable and ordered** while a connection exists.
- `send()` resolving means "handed to the local DataChannel," not "the peer received it."
- `acknowledge: true` means "the remote P2Ply runtime parsed it." It does **not** mean your app on the other side finished processing it. For that, use `request()` and have the other side `respond()` once it is done.
- Duplicate message IDs are suppressed within a session (a bounded in-memory cache).
- **While a peer is reconnecting**, `send()` and `request()` wait for the new connection (up to their timeout) instead of failing. Messages sent with `acknowledge: true` and requests are sent again after the reconnect, and the receiver drops the duplicates, so each one is handled once. A repeated request gets the original response.
- A plain `send()` without `acknowledge` is at-most-once: if the connection drops right after it was handed to the DataChannel, it can be lost.
- Nothing is persisted. Delivery across page reloads or restarts is not guaranteed; for that, store messages yourself and give them your own IDs.
- Queues are bounded. When full, `send()` rejects with `QUEUE_OVERFLOW`; messages are never dropped silently.

### Reconnection

When a connected peer drops (network change, Wi-Fi blip, signaling server restart), P2Ply reconnects automatically:

1. Both sides mark the peer `reconnecting` (`peer:state` event). File transfers pause; sends and requests wait.
2. One side, chosen by peer ID so the two don't race, rebuilds the connection with exponential backoff. It also retries when the peer reappears in a room, for example after the signaling server comes back.
3. Once connected, transfers resume from the last received chunk, and unconfirmed messages and requests are sent again.
4. If the peer does not come back within `peerReconnect.windowMs` (60 s by default), P2Ply gives up: waiting sends reject, paused transfers fail, and an `error` event with `CONNECTION_FAILED` is emitted.

A peer that calls `leave()` or `close()` tells the others first, so they do not try to reconnect. A peer that disappears without that (a crashed tab, a lost network) is waited for until `windowMs` runs out.

Reconnection needs both peers on P2Ply 0.2 or later. Set `peerReconnect: { auto: false }` to turn it off; `reconnectPeer()` still works manually.

---

## 6. File transfer

```ts
// Receiver: offers are rejected unless a listener accepts them.
mesh.on('file:offer', ({ metadata, accept, reject }) => {
  if (metadata.size <= 20 * 1024 * 1024) void accept();
  else void reject('Too large');
});
mesh.on('file:complete', ({ result }) => {
  // result.blob is set when using the default memory sink
  console.log(result.name, result.size, result.sha256);
});

// Sender
const transfer = await mesh.sendFile(peerId, file, { name: 'report.pdf' });
transfer.completion.then((r) => console.log('verified', r.sha256));
await transfer.cancel(); // or mesh.cancelTransfer(transfer.transferId)
```

**How it works:**

- The sender reads the file one chunk at a time with `Blob.slice()` and does not load the whole file into memory.
- The receiver must ack each chunk before the next one is sent, which bounds memory and provides backpressure.
- The receiver checks sequence numbers and lengths against the advertised metadata.
- Both sides compute SHA-256 incrementally. `completion` resolves only after the receiver confirms the hashes match.
- **Transfers survive connection drops.** They pause while the peer reconnects, then continue from the last chunk the receiver has; nothing is sent twice, and the hash still covers the whole file. If the peer does not return within `peerReconnect.windowMs`, the transfer fails.
- Multiple transfers can run in parallel without mixing, because every chunk carries its transfer ID.
- File names from peers are sanitised (path components and control characters are removed).

**Large files:** the default receiver sink keeps chunks in memory and returns a `Blob`. For files larger than memory allows, supply a streaming sink:

```ts
new P2Ply({
  signalingUrl,
  maxFileBytes: 2 * 1024 ** 3,
  fileSinkFactory: async ({ metadata }) => {
    const writable = await openSomeStream(metadata.name);
    return {
      write: (chunk) => writable.write(chunk),
      close: () => writable.close(), // return value becomes result.sinkResult
      abort: (reason) => writable.abort(reason),
    };
  },
});
```

Transfers resume after a reconnect, but not after a page reload or restart: the transfer state lives in memory.

---

## 7. Signaling server

### CLI

```sh
pnpm dev                     # from this repo (auto-reloads)
npx p2ply-server            # after installing the package
```

| Environment variable    | Default   | Meaning                                         |
| ----------------------- | --------- | ----------------------------------------------- |
| `P2PLY_HOST`            | `0.0.0.0` | Listen address                                  |
| `P2PLY_PORT` / `PORT`   | `8787`    | Listen port                                     |
| `P2PLY_PATH`            | `/`       | WebSocket path                                  |
| `P2PLY_ALLOWED_ORIGINS` | any       | Comma-separated list of allowed browser origins |
| `P2PLY_ALLOW_NO_ORIGIN` | `true`    | Allow non-browser clients that send no `Origin` |

HTTP endpoints: `GET /health` returns connection and room counts. `GET /ready` returns 200, or 503 while the server is shutting down.

The CLI has **no authentication**. For a public deployment, use the programmatic API below.

### Programmatic

```ts
import { createSignalingServer } from 'p2ply/server';

const server = createSignalingServer({
  port: 8787,
  allowedOrigins: ['https://app.example.com'],
  allowNoOrigin: false,
  authenticate: ({ authToken, peerId, request }) => (verify(authToken) ? { userId: '…' } : false), // the returned value is the peer's "auth context"
  authorizeJoin: (auth, roomId) => canJoin(auth, roomId),
  authorizeSignal: (fromAuth, toAuth, roomId) => true, // gate who can connect to whom
});
await server.listen();
// later: await server.close();
```

A runnable version is in [`examples/self-hosted-server`](../examples/self-hosted-server/index.ts).

| Server option                        | Default                                |
| ------------------------------------ | -------------------------------------- |
| `maxConnections`                     | 1000                                   |
| `maxPeersPerRoom`                    | 128                                    |
| `maxRoomsPerPeer`                    | 16                                     |
| `maxMessageBytes`                    | 64 KiB                                 |
| `messagesPerSecond` / `messageBurst` | 60 / 120 per connection (token bucket) |
| `handshakeTimeoutMs`                 | 5000                                   |
| `heartbeatIntervalMs`                | 30000                                  |
| `staleConnectionMs`                  | 90000                                  |

A client that sends an invalid message, sends too fast, or skips `hello` is disconnected.

### Docker

```sh
docker build -t p2ply-signaling .
docker run --rm -p 8787:8787 -e P2PLY_ALLOWED_ORIGINS=https://app.example.com p2ply-signaling
```

Put it behind a TLS reverse proxy (nginx, Caddy, or a cloud load balancer) so clients can use `wss://`.

### Scaling

The built-in registry is **in-memory and single-process**. Two server instances do not share rooms. To run several, implement the `PeerRegistry` interface with shared state (for example Redis) and route each peer's messages to the instance that holds its socket. Pass it as `createSignalingServer({ registry })`. For most apps, one instance is plenty: it only relays small handshake messages.

---

## 8. STUN and TURN

- On the **same network** (localhost, LAN), no ICE servers are needed.
- **Across the internet**, add a STUN server so peers can discover their public addresses.
- Behind **strict NATs or corporate firewalls**, a direct connection can be impossible. A TURN server then relays the (still encrypted) traffic. Public STUN alone does not guarantee connectivity.

```ts
new P2Ply({
  signalingUrl: 'wss://signal.example.com',
  rtcConfiguration: {
    iceServers: [
      { urls: 'stun:stun.example.net:3478' },
      {
        urls: ['turn:turn.example.net:3478?transport=udp', 'turns:turn.example.net:5349'],
        username: shortLivedUsername,
        credential: shortLivedCredential,
      },
    ],
  },
});
```

Run your own TURN server (for example [coturn](https://github.com/coturn/coturn)) or use a provider. Generate **short-lived** TURN credentials on your backend; never ship permanent TURN secrets in browser code. P2Ply does not operate any STUN or TURN servers.

---

## 9. Node.js

Node has no built-in WebRTC, so P2Ply uses the optional native package `@roamhq/wrtc` (WebRTC M106) plus `ws` for signaling.

```sh
pnpm add p2ply @roamhq/wrtc
```

```ts
import { NodeP2Ply } from 'p2ply/node';
const mesh = await NodeP2Ply.create({ signalingUrl: 'ws://localhost:8787' });
// …same API as P2Ply
```

`NodeP2Ply.create()` throws a clear error if `@roamhq/wrtc` is not installed. Prebuilt native binaries exist only for some OS/architecture combinations; check `@roamhq/wrtc`'s supported platforms before deploying. The browser entry point (`p2ply`) never loads this package.

---

## 10. Compatibility

Only setups that were actually executed are marked as tested.

| Environment                      | Transport                         | Status                                                                                          |
| -------------------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------- |
| Chromium 156 (Playwright)        | Native WebRTC + WebSocket         | **Tested** on Linux x64 and Windows x64: two browser contexts, message + verified file transfer |
| Node.js 24.15, Windows x64       | `@roamhq/wrtc` 0.10.0 + `ws` 8.22 | **Tested**: unit + real WebRTC integration suite                                                |
| Node.js 20.20, Linux x64         | `@roamhq/wrtc` 0.10.0 + `ws` 8.x  | **Tested**: unit + real WebRTC integration suite                                                |
| Node.js 22                       | `@roamhq/wrtc` + `ws`             | Run by the CI matrix; not executed locally                                                      |
| Firefox, Safari, mobile browsers | Native WebRTC                     | Expected to work (standard APIs) but **not tested** yet                                         |

Minimum Node version: 20.19. Browsers cannot open raw TCP or UDP sockets, and P2Ply does not try to: browser data always goes over WebRTC.

---

## 11. Security

See [SECURITY.md](../SECURITY.md) for the threat model and how to report vulnerabilities. In short:

- DataChannels are encrypted with DTLS. DTLS does **not** prove _who_ the other peer is.
- Peer IDs are random labels, **not** identities. Room IDs are not passwords.
- Your signaling server is trusted. A malicious server could tamper with negotiation. For real access control, use `authenticate`, `authorizeJoin`, `authorizeSignal`, and the client-side `authorizePeer`.
- Use `wss://` in production, restrict `allowedOrigins`, and validate everything peers send (`validateMessage`).
- By default, neither the client nor the server logs SDP, ICE candidates, auth tokens, or message payloads.

---

## 12. Troubleshooting

| Symptom                                                               | Likely cause and fix                                                                                                |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `start()` rejects with `SIGNALING_UNAVAILABLE`                        | Server not running or wrong URL. Check `curl http://host:8787/health`. Use `ws://` locally and `wss://` behind TLS. |
| Upgrade rejected with 403                                             | Your page's origin is not in `P2PLY_ALLOWED_ORIGINS`.                                                               |
| `UNAUTHORIZED` on start or join                                       | The server's `authenticate` or `authorizeJoin` rejected the token or room.                                          |
| `connect()` → `PEER_NOT_FOUND`                                        | You are not both in the same joined room. Wait for `peer:join` before connecting.                                   |
| `connect()` → `CONNECTION_TIMEOUT` / "ICE connectivity checks failed" | NAT/firewall blocked the direct path. Add STUN, and TURN for strict networks (see [§8](#8-stun-and-turn)).          |
| Works on localhost but not between two networks                       | Same as above: you need STUN/TURN.                                                                                  |
| `send()` → `QUEUE_OVERFLOW`                                           | You are sending faster than the network can carry. Await each `send()`, or raise `maxQueued*`.                      |
| `send()` → `MESSAGE_TOO_LARGE`                                        | Raise `maxMessageBytes` on **both** peers, or send large data with `sendFile()`.                                    |
| File offer never arrives or is rejected immediately                   | The receiver has no `file:offer` listener, or the file exceeds the receiver's `maxFileBytes`.                       |
| `NodeP2Ply.create()` → "requires @roamhq/wrtc"                        | Run `pnpm add @roamhq/wrtc`, and check that your platform has a prebuilt binary.                                    |
| Peer stays `reconnecting`, then `CONNECTION_FAILED`                   | The peer did not come back within `peerReconnect.windowMs`, or both sides cannot reach each other (see STUN/TURN).  |

For more detail, pass a `logger` and inspect `await mesh.getDiagnostics()`. `lastConnectionError` and `selectedCandidateType` are usually the most revealing fields.

---

## 13. Limitations

- Nothing is persisted: messages and transfers survive connection drops, but not page reloads or restarts.
- A plain `send()` (without `acknowledge`) can be lost if the connection drops at that moment.
- The signaling server registry is single-process (see [Scaling](#scaling)).
- No built-in TURN, global discovery, DHT, or NAT-traversal guarantee.
- Peer IDs are not cryptographic identities.
- Full mesh topology: each peer connects directly to each other peer, so it suits small groups (roughly tens of peers), not hundreds.

---

## 14. Protocol details

Both planes use protocol version `1`. Every message is validated against a strict Zod schema; unknown types, wrong versions, or extra fields are rejected.

**Signaling (JSON text over WebSocket)**

- Client → server: `hello`, `join`, `leave`, `offer`, `answer`, `ice-candidate`.
- Server → client: `welcome`, `joined`, `left`, `peer-joined`, `peer-left`, the forwarded `offer`/`answer`/`ice-candidate`, and `error`.
- The server stamps the sender's ID on forwarded messages and forwards only when both peers are in the stated room. Session IDs scope each negotiation, so stale answers and candidates are ignored.

**DataChannel (label `p2ply/1`)**

- JSON envelopes `{ v: 1, type, … }`: `app`, `ack`, `bye`, `request`, `response`, `response-error`, and file control messages (`file-start`, `file-ready`, `file-reject`, `file-chunk-ack`, `file-complete`, `file-complete-ack`, `file-resume`, `file-resume-ack`, `file-cancel`).
- `bye` is sent before leaving on purpose. `file-resume` asks the receiver for its next expected chunk after a reconnect.
- File chunks are binary frames: `"P2"` magic (2 bytes) · version (1) · transfer UUID (16) · big-endian sequence (4) · payload. This is framing only, not cryptography.
