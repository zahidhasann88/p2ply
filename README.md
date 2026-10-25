# P2Ply

**Peer-to-peer, made simple.** Messaging and file sharing for TypeScript apps, built on WebRTC.

P2Ply connects browsers (and Node.js) **directly to each other**. You get typed messages, request/response, and verified file transfer without writing any WebRTC signaling code yourself. A small signaling server, which you host, introduces peers to each other. After that, your data flows peer-to-peer and never passes through the server.

```
  Browser A  ◄──── WebRTC DataChannel (your data) ────►  Browser B
       │                                                    │
       └─────────►  Signaling server (handshake only) ◄─────┘
```

## Features

- **Rooms and discovery:** join a room and see who else is there.
- **Messaging:** typed JSON messages, broadcast, optional delivery acknowledgements, request/response.
- **File transfer:** chunked, with progress, cancellation, and SHA-256 integrity checks.
- **Reliability:** automatic signaling reconnects, bounded queues with backpressure, timeouts everywhere.
- **Diagnostics:** connection state, bytes, round-trip time, and whether traffic is direct or relayed.
- **Self-hosted:** the signaling server is included. There is no paid service and no API key.
- **Secure defaults:** validated protocol, rate limits, origin checks, and auth hooks.

## Try it in 1 minute

Requires **Node.js 20.19+** and **pnpm** (`corepack enable`).

```sh
pnpm install
pnpm dev               # terminal 1: starts the signaling server on ws://localhost:8787
pnpm dev:playground    # terminal 2: opens the demo UI on http://localhost:5173
```

Open `http://localhost:5173` in **two browser tabs**, click **Connect** in both, then connect to the other peer. You can now chat, send files, and view diagnostics.

## Install

```sh
pnpm add p2ply          # or: npm install p2ply
```

## Usage

```ts
import { P2Ply } from 'p2ply';

const mesh = new P2Ply<{ text: string }>({
  signalingUrl: 'ws://localhost:8787',
});

// A new peer appeared in the room: open a direct connection to it.
mesh.on('peer:join', ({ peerId }) => {
  mesh.connect(peerId).catch(console.error);
});

// The connection is ready: say hello.
mesh.on('peer:state', ({ peerId, state }) => {
  if (state === 'connected') mesh.send(peerId, { text: 'Hello!' });
});

// A message arrived from a peer.
mesh.on('message', ({ peerId, data }) => {
  console.log(peerId, 'says', data.text);
});

mesh.on('error', (error) => console.error(error.code, error.message));

await mesh.start(); // connect to the signaling server
await mesh.join('my-room'); // join a room

// later
await mesh.close();
```

Both peers may call `connect()` at the same time; P2Ply handles the collision for you.

### Send a file

```ts
// Receiver: accept incoming files
mesh.on('file:offer', ({ metadata, accept }) => {
  console.log('Incoming', metadata.name, metadata.size, 'bytes');
  accept();
});
mesh.on('file:complete', ({ result }) => {
  // result.blob holds the file, and its SHA-256 has already been verified
});

// Sender
const transfer = await mesh.sendFile(peerId, fileInput.files[0]);
await transfer.completion;
```

### Request / response

```ts
// Peer B
mesh.on('request', ({ data, respond }) => {
  const { a, b } = data as { a: number; b: number }; // validate untrusted input in real code
  respond({ sum: a + b });
});

// Peer A
const answer = await mesh.request(peerB, { a: 2, b: 3 }); // { sum: 5 }
```

### Node.js

Install the optional WebRTC adapter (`pnpm add @roamhq/wrtc`), then use `await NodeP2Ply.create(options)` from `p2ply/node`. Same API as `P2Ply`. See [Node.js](docs/GUIDE.md#9-nodejs).

## Main API

| Method                                | What it does                                           |
| ------------------------------------- | ------------------------------------------------------ |
| `start()` / `close()`                 | Connect to / disconnect from everything                |
| `join(room)` / `leave(room)`          | Enter or leave a room                                  |
| `getPeers()` / `getPeer(peerId)`      | List peers you can see and their state                 |
| `connect(peerId)`                     | Open a direct connection to a peer                     |
| `send(peerId, data, { acknowledge })` | Send a message                                         |
| `broadcast(data)`                     | Send to every connected peer                           |
| `request(peerId, data)`               | Send and wait for a reply                              |
| `sendFile(peerId, file)`              | Send a file                                            |
| `cancelTransfer(transferId)`          | Stop a file transfer                                   |
| `reconnectPeer(peerId)`               | Rebuild a dropped peer connection                      |
| `reconnectSignaling()`                | Reopen the server connection and rejoin rooms          |
| `getDiagnostics()`                    | Connection stats for debugging                         |
| `on(event, fn)`                       | Subscribe to an event; returns an unsubscribe function |
| `once(event, fn)` / `off(event, fn)`  | Listen once / remove a listener                        |

Events: `peer:join`, `peer:leave`, `peer:state`, `signaling:state`, `message`, `request`, `file:offer`, `file:progress`, `file:complete`, `file:error`, `error`.

**[Read the full guide →](docs/GUIDE.md)** It covers every option, delivery guarantees, the server, STUN/TURN, security, and troubleshooting.

## Run your own signaling server

```sh
npx p2ply-server                                   # after installing the package
P2PLY_PORT=8787 P2PLY_ALLOWED_ORIGINS=https://myapp.com npx p2ply-server
docker build -t p2ply-signaling . && docker run -p 8787:8787 p2ply-signaling
```

Health check: `GET /health`. For authentication and room rules, create the server in code (see [`examples/self-hosted-server`](examples/self-hosted-server/index.ts) and the [guide](docs/GUIDE.md#7-signaling-server)).

> **Connecting across different networks?** Localhost and LAN work out of the box. Over the internet, add a STUN server, and a TURN server for strict firewalls. See [STUN and TURN](docs/GUIDE.md#8-stun-and-turn).

## Examples

Start the server with `pnpm dev` first, then run an example:

| Command                                       | Example                                                      |
| --------------------------------------------- | ------------------------------------------------------------ |
| `pnpm dev:playground`                         | Full demo UI: chat, files, diagnostics (`localhost:5173`)    |
| `pnpm example:node`                           | Node.js terminal chat (run in two terminals)                 |
| `pnpm example:custom`                         | Typed messages with runtime validation (Node)                |
| `pnpm example:reconnect`                      | Automatic peer reconnection (Node)                           |
| `P2PLY_AUTH_TOKEN=secret pnpm example:server` | Signaling server with token auth (run instead of `pnpm dev`) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, checks, and the source layout. Report security issues privately as described in [SECURITY.md](SECURITY.md).

## Limitations

- Dropped peer connections are not restored automatically; call `reconnectPeer()`.
- Full mesh: best for small groups (tens of peers).
- Tested in Chromium and Node.js; Firefox and Safari are not yet tested.

See the [full list](docs/GUIDE.md#13-limitations).

## License

[MIT](LICENSE)
