# Contributing to P2Ply

Thanks for helping out! For larger changes, please open an issue first so we can agree on the approach.

Report security issues privately as described in [SECURITY.md](SECURITY.md), not in public issues.

## Setup

Requires **Node.js 20.19+** and **pnpm** (`corepack enable`).

```sh
pnpm install
pnpm dev                # signaling server on ws://localhost:8787
pnpm dev:playground     # demo UI on http://localhost:5173
```

## Checks

```sh
pnpm build              # build to dist/
pnpm typecheck
pnpm lint
pnpm format             # auto-format (format:check to verify)
pnpm test               # unit + real WebRTC integration tests
pnpm test:e2e           # two real Chromium browsers (first: pnpm exec playwright install chromium)
pnpm pack:check         # preview the npm package contents
```

## Pull requests

- Make sure `pnpm format:check`, `pnpm lint`, `pnpm typecheck`, and `pnpm test` all pass.
- Add tests for new behaviour.
- Update the [README](README.md) or [guide](docs/GUIDE.md) if you change the public API.

## Source layout

| Path                                    | Responsibility                                                         |
| --------------------------------------- | ---------------------------------------------------------------------- |
| `src/p2ply.ts`                          | Public `P2Ply` class: rooms, peers, messaging, events, diagnostics     |
| `src/connection/peer-link.ts`           | One `RTCPeerConnection` + DataChannel, negotiation, bounded send queue |
| `src/signaling/client.ts`               | WebSocket client with validation and backoff reconnects                |
| `src/file-transfer/transfer-manager.ts` | Chunked transfers, flow control, SHA-256 verification                  |
| `src/protocol/`                         | Versioned Zod schemas for signaling and DataChannel messages           |
| `src/server/`                           | Self-hosted signaling server, registry, rate limiting, CLI             |
| `src/node.ts`                           | `NodeP2Ply`, which plugs in `@roamhq/wrtc` and `ws`                    |
| `tests/`                                | `unit/`, `integration/` (real WebRTC in Node), `e2e/` (Playwright)     |

## Deploying the playground

The playground is a static site deployed to GitHub Pages by `.github/workflows/pages.yml` on every push to `main` that touches it. GitHub Pages cannot run the signaling server, so host that separately.

1. **Host the signaling server** anywhere that runs Docker or Node (Render, Fly.io, Railway, a VPS) using the included `Dockerfile`. Allow only the Pages origin and serve it over TLS (`wss://`):

   ```sh
   P2PLY_ALLOWED_ORIGINS=https://<user>.github.io
   ```

2. **Enable Pages:** repository _Settings → Pages → Source: GitHub Actions_.
3. **Set repository variables** under _Settings → Secrets and variables → Actions → Variables_:

   | Variable                   | Example                          | Default                        |
   | -------------------------- | -------------------------------- | ------------------------------ |
   | `PLAYGROUND_SIGNALING_URL` | `wss://p2ply-signal.example.com` | `ws://localhost:8787`          |
   | `PLAYGROUND_STUN_URLS`     | `stun:stun.l.google.com:19302`   | `stun:stun.l.google.com:19302` |

4. Push to `main` or run the workflow manually. The site appears at `https://<user>.github.io/<repo>/`.

Visitors can override the defaults with URL parameters, which also makes shareable links: `?signaling=wss://…&room=my-room`.

To preview the production build locally: `pnpm build:playground`, then serve `playground-dist/`.

Without a TURN server, peers behind strict NATs or corporate firewalls may fail to connect (see [STUN and TURN](docs/GUIDE.md#8-stun-and-turn)). The demo server has no authentication, so rely on its built-in rate and connection limits.

## Releasing

Publishing is manual; nothing publishes automatically.

1. Update the `version` in `package.json` and add an entry to `CHANGELOG.md`.
2. Run every check: `pnpm format:check && pnpm lint && pnpm typecheck && pnpm test && pnpm test:e2e`.
3. Run `pnpm pack:check` and review the file list in the tarball.
4. Run `npm login`, then `npm publish --access public`. Publishing rebuilds `dist/` first (`prepublishOnly`), so the package always matches the source.
5. Tag the release with the new version, for example `git tag v0.1.0 && git push --tags`.
