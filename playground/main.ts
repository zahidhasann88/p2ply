import { P2Ply } from '../src/index.js';
import type { PeerSummary } from '../src/index.js';

const byId = <T extends HTMLElement>(id: string): T => {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing playground element: ${id}`);
  return element as T;
};

const urlInput = byId<HTMLInputElement>('signaling-url');
const roomInput = byId<HTMLInputElement>('room-id');
const connectionForm = byId<HTMLFormElement>('connection-form');
const joinButton = byId<HTMLButtonElement>('join-button');
const statusDot = byId<HTMLSpanElement>('status-dot');
const statusLabel = byId<HTMLSpanElement>('status-label');
const localPeerId = byId<HTMLElement>('local-peer-id');
const peerCount = byId<HTMLElement>('peer-count');
const peerList = byId<HTMLUListElement>('peer-list');
const messageForm = byId<HTMLFormElement>('message-form');
const messageInput = byId<HTMLInputElement>('message-input');
const messageTarget = byId<HTMLSelectElement>('message-target');
const messageLog = byId<HTMLDivElement>('message-log');
const fileForm = byId<HTMLFormElement>('file-form');
const fileInput = byId<HTMLInputElement>('file-input');
const fileTarget = byId<HTMLSelectElement>('file-target');
const transferList = byId<HTMLDivElement>('transfer-list');
const diagnosticsButton = byId<HTMLButtonElement>('diagnostics-button');
const diagnosticsOutput = byId<HTMLPreElement>('diagnostics-output');
const activityList = byId<HTMLUListElement>('activity-list');

// Defaults come from the page URL (?signaling=…&room=…), then build-time env, then localhost.
const params = new URLSearchParams(window.location.search);
const defaultSignalingUrl =
  params.get('signaling') ?? import.meta.env.VITE_SIGNALING_URL ?? 'ws://localhost:8787';
const iceServers = (import.meta.env.VITE_STUN_URLS ?? '')
  .split(',')
  .map((url) => url.trim())
  .filter(Boolean)
  .map((urls) => ({ urls }));
urlInput.value = defaultSignalingUrl;
roomInput.value = params.get('room') ?? roomInput.value;

let mesh: P2Ply<unknown> | undefined;
let activeRoom: string | undefined;
const transferElements = new Map<
  string,
  { item: HTMLDivElement; fill: HTMLDivElement; percent: HTMLSpanElement; sub: HTMLDivElement }
>();

connectionForm.addEventListener('submit', (event) => {
  event.preventDefault();
  if (!mesh) void startMesh();
  else if (activeRoom !== roomInput.value.trim()) void joinRoom(roomInput.value.trim());
  else void leaveRoom();
});

messageForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const peerId = messageTarget.value;
  const text = messageInput.value.trim();
  if (!mesh || !peerId || !text) return;
  try {
    const receipt = await mesh.send(peerId, { text, sentAt: Date.now() });
    addMessage('You', text, true);
    addActivity(
      `Message handed to the local DataChannel queue (${receipt.messageId.slice(0, 8)}).`,
    );
    messageInput.value = '';
  } catch (error) {
    showError(error);
  }
});

fileForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const peerId = fileTarget.value;
  const file = fileInput.files?.[0];
  if (!mesh || !peerId || !file) return;
  try {
    const handle = await mesh.sendFile(peerId, file);
    const transfer = makeTransferCard(handle.transferId, file.name, file.size, 'send');
    void handle.completion
      .then((result) => {
        updateTransfer(
          transfer,
          result.size,
          result.size,
          'Verified and delivered to the remote P2Ply runtime.',
        );
        addActivity(
          `File transfer complete: ${result.name} · SHA-256 ${result.sha256.slice(0, 12)}…`,
        );
      })
      .catch((error: unknown) => {
        updateTransfer(
          transfer,
          0,
          file.size,
          error instanceof Error ? error.message : 'Transfer failed.',
        );
        showError(error);
      });
    fileInput.value = '';
  } catch (error) {
    showError(error);
  }
});

diagnosticsButton.addEventListener('click', () => void refreshDiagnostics());
byId<HTMLButtonElement>('clear-errors').addEventListener('click', () => {
  activityList.replaceChildren();
  addActivity('Activity cleared.');
});

async function startMesh(): Promise<void> {
  const signalingUrl = urlInput.value.trim();
  const roomId = roomInput.value.trim();
  if (!signalingUrl || !roomId) return;
  setStatus('Connecting', 'pending');
  joinButton.disabled = true;
  try {
    mesh = new P2Ply<unknown>({
      signalingUrl,
      rtcConfiguration: { iceServers },
      reconnect: { enabled: true, maxAttempts: 10, minDelayMs: 250, maxDelayMs: 15_000 },
      fileChunkSize: 16 * 1024,
      maxFileBytes: 100 * 1024 * 1024,
      authorizePeer: ({ peerId, direction }) => {
        addActivity(
          `${direction === 'inbound' ? 'Incoming' : 'Outgoing'} connection permission checked for ${peerId.slice(0, 8)}.`,
        );
        return true;
      },
    });
    localPeerId.textContent = mesh.peerId;
    wireEvents(mesh);
    await mesh.start();
    await mesh.join(roomId);
    activeRoom = roomId;
    updateConnectionButton();
    setStatus(`In room ${roomId}`, 'online');
    addActivity(`Connected to signaling and joined room “${roomId}”.`);
  } catch (error) {
    setStatus('Offline', '');
    showError(error);
    await mesh?.close();
    mesh = undefined;
    joinButton.textContent = 'Connect';
  } finally {
    joinButton.disabled = false;
    renderPeers();
  }
}

async function joinRoom(roomId: string): Promise<void> {
  if (!mesh || !roomId) return;
  try {
    await mesh.join(roomId);
    activeRoom = roomId;
    updateConnectionButton();
    setStatus(`In room ${roomId}`, 'online');
    addActivity(`Joined room “${roomId}”.`);
    renderPeers();
  } catch (error) {
    showError(error);
  }
}

async function leaveRoom(): Promise<void> {
  if (!mesh || !activeRoom) return;
  const roomId = activeRoom;
  try {
    await mesh.leave(roomId);
    activeRoom = undefined;
    updateConnectionButton();
    setStatus('Connected · no room', 'online');
    addActivity(`Left room “${roomId}”.`);
    renderPeers();
  } catch (error) {
    showError(error);
  }
}

function wireEvents(current: P2Ply<unknown>): void {
  current.on('signaling:state', ({ state, attempt, error }) => {
    if (state === 'connected')
      setStatus(activeRoom ? `In room ${activeRoom}` : 'Connected', 'online');
    else if (state === 'reconnecting')
      setStatus(`Reconnecting · attempt ${attempt ?? 0}`, 'pending');
    else if (state === 'disconnected' || state === 'closed')
      setStatus(state === 'closed' ? 'Offline' : 'Signaling disconnected', '');
    if (error) addActivity(`Signaling: ${error.message}`, true);
  });
  current.on('peer:join', ({ peerId, roomId }) => {
    addActivity(`Peer ${peerId.slice(0, 8)} joined “${roomId}”.`);
    renderPeers();
  });
  current.on('peer:leave', ({ peerId, roomId }) => {
    addActivity(`Peer ${peerId.slice(0, 8)} left “${roomId}”.`);
    renderPeers();
  });
  current.on('peer:state', ({ peerId, state, error }) => {
    addActivity(
      `Peer ${peerId.slice(0, 8)} is ${state}${error ? `: ${error.message}` : ''}.`,
      state === 'failed',
    );
    renderPeers();
  });
  current.on('message', ({ peerId, data }) => {
    const value = data as { text?: unknown };
    const text = typeof value?.text === 'string' ? value.text : JSON.stringify(data);
    addMessage(peerId.slice(0, 8), text, false);
  });
  current.on('file:offer', (offer) => {
    const accepted = window.confirm(
      `Accept ${offer.metadata.name} (${formatBytes(offer.metadata.size)}) from ${offer.peerId.slice(0, 8)}?`,
    );
    if (accepted) {
      void offer
        .accept()
        .then(() => {
          makeTransferCard(offer.transferId, offer.metadata.name, offer.metadata.size, 'receive');
          addActivity(`Accepted incoming file “${offer.metadata.name}”.`);
        })
        .catch(showError);
    } else {
      void offer.reject('The user declined this file.');
    }
  });
  current.on('file:progress', ({ peerId, transferId, direction, transferredBytes, totalBytes }) => {
    let transfer = transferElements.get(transferId);
    if (!transfer) {
      transfer = makeTransferCard(
        transferId,
        `Transfer from ${peerId.slice(0, 8)}`,
        totalBytes,
        direction,
      );
    }
    updateTransfer(
      transfer,
      transferredBytes,
      totalBytes,
      `${direction === 'send' ? 'Sending' : 'Receiving'} · ${formatBytes(transferredBytes)} / ${formatBytes(totalBytes)}`,
    );
  });
  current.on('file:complete', ({ result }) => {
    if (result.blob) {
      const url = URL.createObjectURL(result.blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = result.name;
      link.textContent = `Download ${result.name}`;
      link.className = 'download-link';
      const card = transferElements.get(result.transferId);
      card?.item.append(link);
    }
  });
  current.on('file:error', ({ transferId, error }) => {
    const transfer = transferElements.get(transferId);
    if (transfer) updateTransfer(transfer, 0, 1, error.message);
  });
  current.on('error', showError);
}

function renderPeers(): void {
  const peers: PeerSummary[] = mesh?.getPeers() ?? [];
  peerCount.textContent = String(peers.length);
  peerList.replaceChildren();
  const connected = peers.filter((peer) => peer.state === 'connected');
  replaceTargets(messageTarget, connected);
  replaceTargets(fileTarget, connected);
  if (peers.length === 0) {
    const empty = document.createElement('li');
    empty.className = 'empty';
    empty.textContent = activeRoom
      ? 'No other peers have joined this room yet.'
      : 'Start and join a room to discover peers.';
    peerList.append(empty);
    return;
  }
  for (const peer of peers) {
    const item = document.createElement('li');
    item.className = 'peer-row';
    const top = document.createElement('div');
    top.className = 'peer-top';
    const id = document.createElement('code');
    id.className = 'peer-id';
    id.textContent = peer.peerId;
    const state = document.createElement('span');
    state.className = 'peer-state';
    state.textContent = peer.state;
    top.append(id, state);
    const actions = document.createElement('div');
    actions.className = 'peer-actions';
    const connect = document.createElement('button');
    connect.type = 'button';
    connect.textContent =
      peer.state === 'connected'
        ? 'Connected'
        : peer.state === 'failed' || peer.state === 'disconnected'
          ? 'Reconnect'
          : 'Connect';
    connect.disabled = peer.state === 'connecting' || peer.state === 'connected';
    connect.addEventListener('click', () => {
      if (!mesh) return;
      const operation =
        peer.state === 'failed' || peer.state === 'disconnected'
          ? mesh.reconnectPeer(peer.peerId)
          : mesh.connect(peer.peerId);
      void operation.then(refreshDiagnostics).catch(showError);
    });
    actions.append(connect);
    item.append(top, actions);
    peerList.append(item);
  }
}

function replaceTargets(select: HTMLSelectElement, peers: PeerSummary[]): void {
  const selected = select.value;
  select.replaceChildren(new Option('Select a connected peer', ''));
  for (const peer of peers) select.add(new Option(peer.peerId.slice(0, 8), peer.peerId));
  if (peers.some((peer) => peer.peerId === selected)) select.value = selected;
}

function addMessage(sender: string, text: string, own: boolean): void {
  if (messageLog.querySelector('.empty-state')) messageLog.replaceChildren();
  const entry = document.createElement('div');
  entry.className = `message-entry${own ? ' own' : ''}`;
  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = `${own ? 'YOU' : sender} · ${new Date().toLocaleTimeString()}`;
  const body = document.createElement('div');
  body.className = 'body';
  body.textContent = text;
  entry.append(meta, body);
  messageLog.append(entry);
  messageLog.scrollTop = messageLog.scrollHeight;
}

function makeTransferCard(
  transferId: string,
  name: string,
  total: number,
  direction: 'send' | 'receive',
) {
  const item = document.createElement('div');
  item.className = 'transfer-item';
  const head = document.createElement('div');
  head.className = 'transfer-head';
  const label = document.createElement('span');
  label.textContent = `${direction === 'send' ? '↑' : '↓'} ${name}`;
  const percent = document.createElement('span');
  percent.textContent = '0%';
  head.append(label, percent);
  const track = document.createElement('div');
  track.className = 'progress-track';
  const fill = document.createElement('div');
  fill.className = 'progress-fill';
  track.append(fill);
  const sub = document.createElement('div');
  sub.className = 'transfer-sub';
  sub.textContent = `0 B / ${formatBytes(total)} · waiting`;
  item.append(head, track, sub);
  if (transferList.querySelector('.empty-state')) transferList.replaceChildren();
  transferList.prepend(item);
  const result = { item, fill, percent, sub };
  transferElements.set(transferId, result);
  return result;
}

function updateTransfer(
  transfer: {
    item: HTMLDivElement;
    fill: HTMLDivElement;
    percent: HTMLSpanElement;
    sub: HTMLDivElement;
  },
  amount: number,
  total: number,
  description: string,
): void {
  const percent = total === 0 ? 100 : Math.min(100, Math.floor((amount / total) * 100));
  transfer.fill.style.width = `${percent}%`;
  transfer.percent.textContent = `${percent}%`;
  transfer.sub.textContent = description;
}

async function refreshDiagnostics(): Promise<void> {
  if (!mesh) return;
  try {
    const diagnostics = await mesh.getDiagnostics();
    diagnosticsOutput.textContent = JSON.stringify(diagnostics, null, 2);
  } catch (error) {
    showError(error);
  }
}

function setStatus(text: string, style: '' | 'online' | 'pending'): void {
  statusLabel.textContent = text;
  statusDot.className = `dot${style ? ` ${style}` : ''}`;
}

function updateConnectionButton(): void {
  joinButton.textContent = activeRoom ? 'Leave' : mesh ? 'Join room' : 'Connect';
}

function addActivity(text: string, isError = false): void {
  if (activityList.querySelector('.empty')) activityList.replaceChildren();
  const item = document.createElement('li');
  item.className = `activity${isError ? ' error' : ''}`;
  const time = document.createElement('div');
  time.className = 'time';
  time.textContent = new Date().toLocaleTimeString();
  const body = document.createElement('div');
  body.className = 'text';
  body.textContent = text;
  item.append(time, body);
  activityList.prepend(item);
  while (activityList.children.length > 100) activityList.lastElementChild?.remove();
}

function showError(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  addActivity(message, true);
}

function formatBytes(bytes: number): string {
  if (bytes < 1_000) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB'];
  let value = bytes / 1_000;
  let unit = units[0]!;
  for (let index = 1; index < units.length && value >= 1_000; index += 1) {
    value /= 1_000;
    unit = units[index]!;
  }
  return `${value.toFixed(1)} ${unit}`;
}

window.addEventListener('beforeunload', () => void mesh?.close());
addActivity(`Playground loaded. Signaling server: ${defaultSignalingUrl}`);
