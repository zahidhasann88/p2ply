import { P2Ply } from '../src/index.js';
import type { PeerDiagnostics, PeerSummary } from '../src/index.js';

const params = new URLSearchParams(window.location.search);
const signalingFromUrl = params.get('signaling');
const defaultSignalingUrl =
  signalingFromUrl || import.meta.env.VITE_SIGNALING_URL || 'ws://localhost:8787';
const iceServers = (import.meta.env.VITE_STUN_URLS || '')
  .split(',')
  .map((url) => url.trim())
  .filter(Boolean)
  .map((urls) => ({ urls }));

const ADJECTIVES = ['Swift', 'Calm', 'Bright', 'Clever', 'Brave', 'Quiet', 'Lucky', 'Sunny'];
const ANIMALS = ['Otter', 'Falcon', 'Panda', 'Fox', 'Koala', 'Heron', 'Lynx', 'Dolphin'];

type PlaygroundMessage =
  { kind: 'hello'; name: string } | { kind: 'chat'; name: string; text: string; direct: boolean };

function isPlaygroundMessage(value: unknown): value is PlaygroundMessage {
  if (typeof value !== 'object' || value === null) return false;
  const message = value as Record<string, unknown>;
  const validName = typeof message.name === 'string' && message.name.length <= 32;
  if (message.kind === 'hello') return validName;
  return (
    message.kind === 'chat' &&
    validName &&
    typeof message.text === 'string' &&
    message.text.length <= 2_000 &&
    typeof message.direct === 'boolean'
  );
}

const byId = <T extends HTMLElement>(id: string): T => {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing playground element: ${id}`);
  return element as T;
};

const ui = {
  welcome: byId('welcome'),
  joinForm: byId<HTMLFormElement>('join-form'),
  nameInput: byId<HTMLInputElement>('name-input'),
  roomInput: byId<HTMLInputElement>('room-input'),
  urlInput: byId<HTMLInputElement>('url-input'),
  joinButton: byId<HTMLButtonElement>('join-button'),
  joinError: byId('join-error'),
  room: byId('room'),
  roomBar: byId('room-bar'),
  roomLabel: byId('room-label'),
  status: byId('status'),
  statusLabel: byId('status-label'),
  detailsButton: byId<HTMLButtonElement>('details-button'),
  peerList: byId<HTMLUListElement>('peer-list'),
  peerCount: byId('peer-count'),
  peersEmpty: byId('peers-empty'),
  myAvatar: byId('my-avatar'),
  myName: byId('my-name'),
  timeline: byId('timeline'),
  dropzone: byId('dropzone'),
  composer: byId<HTMLFormElement>('composer'),
  targetSelect: byId<HTMLSelectElement>('target-select'),
  messageInput: byId<HTMLInputElement>('message-input'),
  fileInput: byId<HTMLInputElement>('file-input'),
  drawer: byId('drawer'),
  selfFacts: byId('self-facts'),
  peerDetails: byId('peer-details'),
  log: byId<HTMLOListElement>('log'),
  toasts: byId('toasts'),
};

let mesh: P2Ply<PlaygroundMessage> | undefined;
let roomId = '';
let myName = '';
let target = 'all';
let drawerTimer: ReturnType<typeof setInterval> | undefined;
const names = new Map<string, string>();
const greeted = new Set<string>();
const reconnecting = new Set<string>();
const fileCards = new Map<string, FileCard>();

ui.urlInput.value = defaultSignalingUrl;
ui.roomInput.value = params.get('room') || randomRoom();
ui.nameInput.value = loadName() || randomName();
if (params.get('room')) ui.nameInput.focus();

// ---------------------------------------------------------------------------
// Joining and leaving

ui.joinForm.addEventListener('submit', (event) => {
  event.preventDefault();
  void joinRoom();
});

async function joinRoom(): Promise<void> {
  const name = ui.nameInput.value.trim().slice(0, 32);
  const room = ui.roomInput.value.trim();
  const signalingUrl = ui.urlInput.value.trim();
  if (!name || !room || !signalingUrl) return;
  saveName(name);
  ui.joinError.hidden = true;
  ui.joinButton.disabled = true;
  ui.joinButton.textContent = 'Connecting…';
  setStatus('Connecting…', 'busy');

  try {
    mesh = new P2Ply<PlaygroundMessage>({
      signalingUrl,
      rtcConfiguration: { iceServers },
      autoConnect: true,
      reconnect: { maxAttempts: 10, minDelayMs: 250, maxDelayMs: 10_000 },
      maxFileBytes: 500 * 1024 * 1024,
      validateMessage: isPlaygroundMessage,
    });
    wireEvents(mesh);
    await mesh.start();
    await mesh.join(room);
    myName = name;
    roomId = room;
    enterRoom();
  } catch (error) {
    await mesh?.close();
    mesh = undefined;
    setStatus('Not connected', 'error');
    ui.joinError.textContent = `${errorMessage(error)} Check the signaling server in Server settings.`;
    ui.joinError.hidden = false;
  } finally {
    ui.joinButton.disabled = false;
    ui.joinButton.textContent = 'Join room';
  }
}

function enterRoom(): void {
  ui.welcome.hidden = true;
  ui.room.hidden = false;
  ui.roomBar.hidden = false;
  ui.detailsButton.hidden = false;
  ui.roomLabel.textContent = roomId;
  ui.myName.textContent = myName;
  paintAvatar(ui.myAvatar, mesh!.peerId, myName);
  setStatus('Connected', 'ok');
  const url = new URL(window.location.href);
  url.searchParams.set('room', roomId);
  window.history.replaceState(null, '', url);
  addNotice(`You joined #${roomId}. Invite someone to start chatting.`);
  log(`Joined room “${roomId}” as ${myName}.`);
  renderPeers();
  ui.messageInput.focus();
}

byId('leave-button').addEventListener('click', () => void leaveRoom());

async function leaveRoom(): Promise<void> {
  closeDrawer();
  const current = mesh;
  mesh = undefined;
  await current?.close();
  names.clear();
  greeted.clear();
  reconnecting.clear();
  fileCards.clear();
  target = 'all';
  ui.timeline.replaceChildren();
  ui.log.replaceChildren();
  ui.room.hidden = true;
  ui.roomBar.hidden = true;
  ui.detailsButton.hidden = true;
  ui.welcome.hidden = false;
  setStatus('Not connected', 'idle');
}

window.addEventListener('beforeunload', () => void mesh?.close());

// ---------------------------------------------------------------------------
// P2Ply events

function wireEvents(current: P2Ply<PlaygroundMessage>): void {
  current.on('signaling:state', ({ state }) => {
    if (current !== mesh) return;
    if (state === 'connected') setStatus('Connected', 'ok');
    else if (state === 'reconnecting' || state === 'connecting')
      setStatus('Reconnecting to server…', 'busy');
    else setStatus('Server disconnected', 'error');
    log(`Signaling ${state}.`);
  });

  current.on('peer:join', ({ peerId }) => {
    log(`${shortId(peerId)} is in the room.`);
    renderPeers();
  });

  current.on('peer:leave', ({ peerId }) => {
    if (greeted.has(peerId) && !reconnecting.has(peerId)) addNotice(`${nameOf(peerId)} left.`);
    log(`${nameOf(peerId)} left the room.`);
    renderPeers();
  });

  current.on('peer:state', ({ peerId, state }) => {
    log(`${nameOf(peerId)}: ${state}.`);
    if (state === 'reconnecting' && !reconnecting.has(peerId)) {
      reconnecting.add(peerId);
      addNotice(`Connection to ${nameOf(peerId)} dropped. Reconnecting…`, 'warn');
      for (const card of fileCards.values())
        if (card.peerId === peerId && card.active) card.pause();
    }
    if (state === 'connected') {
      if (reconnecting.delete(peerId)) {
        addNotice(`Reconnected to ${nameOf(peerId)}.`, 'ok');
        for (const card of fileCards.values())
          if (card.peerId === peerId && card.active) card.resume();
      }
      void current.send(peerId, { kind: 'hello', name: myName }).catch(() => {});
    }
    if (
      (state === 'failed' || state === 'closed') &&
      mesh?.getPeer(peerId)?.state !== 'reconnecting'
    )
      reconnecting.delete(peerId);
    renderPeers();
  });

  current.on('message', ({ peerId, data }) => {
    names.set(peerId, data.name || shortId(peerId));
    if (data.kind === 'hello') {
      if (!greeted.has(peerId)) {
        greeted.add(peerId);
        addNotice(`${data.name} joined the chat.`);
      }
      renderPeers();
      return;
    }
    addMessage({ peerId, name: data.name, text: data.text, own: false, direct: data.direct });
  });

  current.on('file:offer', ({ peerId, transferId, metadata, accept, reject }) => {
    const card = new FileCard(transferId, peerId, metadata.name, metadata.size, 'receive');
    card.offer(
      () => void accept().catch(showError),
      () => void reject('Declined.').catch(() => {}),
    );
    fileCards.set(transferId, card);
  });

  current.on('file:progress', ({ transferId, transferredBytes, totalBytes }) => {
    fileCards.get(transferId)?.progress(transferredBytes, totalBytes);
  });

  current.on('file:complete', ({ transferId, result }) => {
    fileCards.get(transferId)?.complete(result.sha256, result.blob);
    log(`File “${result.name}” verified (SHA-256 ${result.sha256.slice(0, 12)}…).`);
  });

  current.on('file:error', ({ transferId, error }) => {
    fileCards.get(transferId)?.fail(error.message);
  });

  current.on('error', (error) => {
    log(error.message, 'error');
    if (error.code !== 'TRANSFER_CANCELLED') toast(error.message, 'error');
  });
}

ui.composer.addEventListener('submit', (event) => {
  event.preventDefault();
  const text = ui.messageInput.value.trim();
  if (!text || !mesh) return;
  const recipients = recipientsFor(target);
  if (recipients.length === 0) {
    toast('Nobody is connected yet. Share the invite link to bring someone in.');
    return;
  }
  ui.messageInput.value = '';
  const direct = target !== 'all';
  addMessage({ peerId: mesh.peerId, name: myName, text, own: true, direct, to: target });
  const message: PlaygroundMessage = { kind: 'chat', name: myName, text, direct };
  for (const peerId of recipients) void mesh.send(peerId, message).catch(showError);
});

byId('attach-button').addEventListener('click', () => ui.fileInput.click());
ui.fileInput.addEventListener('change', () => {
  sendFiles([...(ui.fileInput.files ?? [])]);
  ui.fileInput.value = '';
});

function sendFiles(files: File[]): void {
  if (!mesh || files.length === 0) return;
  const recipients = recipientsFor(target).filter(
    (peerId) => mesh?.getPeer(peerId)?.state === 'connected',
  );
  if (recipients.length === 0) {
    toast('Connect to someone first, then send a file.');
    return;
  }
  for (const file of files) {
    for (const peerId of recipients) void sendFile(peerId, file);
  }
}

async function sendFile(peerId: string, file: File): Promise<void> {
  if (!mesh) return;
  try {
    const handle = await mesh.sendFile(peerId, file);
    const card = new FileCard(handle.transferId, peerId, file.name, file.size, 'send');
    card.waiting(() => void handle.cancel());
    fileCards.set(handle.transferId, card);
    handle.completion.then((result) => card.complete(result.sha256)).catch(() => {});
  } catch (error) {
    showError(error);
  }
}

function recipientsFor(value: string): string[] {
  if (!mesh) return [];
  const reachable = (peer: PeerSummary) =>
    peer.state === 'connected' || peer.state === 'reconnecting';
  if (value === 'all')
    return mesh
      .getPeers()
      .filter(reachable)
      .map((peer) => peer.peerId);
  const peer = mesh.getPeer(value);
  return peer && reachable(peer) ? [value] : [];
}

ui.targetSelect.addEventListener('change', () => {
  target = ui.targetSelect.value;
  renderPeers();
});

const chat = ui.timeline.parentElement!;
let dragDepth = 0;
chat.addEventListener('dragenter', (event) => {
  if (!event.dataTransfer?.types.includes('Files')) return;
  dragDepth += 1;
  ui.dropzone.hidden = false;
});
chat.addEventListener('dragleave', () => {
  dragDepth = Math.max(0, dragDepth - 1);
  if (dragDepth === 0) ui.dropzone.hidden = true;
});
chat.addEventListener('dragover', (event) => event.preventDefault());
chat.addEventListener('drop', (event) => {
  event.preventDefault();
  dragDepth = 0;
  ui.dropzone.hidden = true;
  sendFiles([...(event.dataTransfer?.files ?? [])]);
});

function renderPeers(): void {
  if (!mesh) return;
  const peers = mesh.getPeers();
  if (target !== 'all' && !peers.some((peer) => peer.peerId === target)) target = 'all';

  ui.peerCount.textContent = String(peers.length);
  ui.peersEmpty.hidden = peers.length > 0;
  ui.peerList.replaceChildren(
    ...peers.map((peer) => {
      const item = document.createElement('li');
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'peer';
      button.setAttribute('aria-pressed', String(target === peer.peerId));
      button.title =
        target === peer.peerId ? 'Send to everyone' : `Send only to ${nameOf(peer.peerId)}`;
      const avatar = document.createElement('span');
      paintAvatar(avatar, peer.peerId, nameOf(peer.peerId), peer.state);
      const text = document.createElement('span');
      text.className = 'peer-text';
      const name = document.createElement('strong');
      name.textContent = nameOf(peer.peerId);
      const state = document.createElement('small');
      state.textContent = describeState(peer.state);
      text.append(name, state);
      button.append(avatar, text);
      button.addEventListener('click', () => {
        target = target === peer.peerId ? 'all' : peer.peerId;
        renderPeers();
        ui.messageInput.focus();
      });
      item.append(button);
      return item;
    }),
  );

  const options = [new Option('To everyone', 'all')];
  for (const peer of peers) options.push(new Option(`To ${nameOf(peer.peerId)} only`, peer.peerId));
  ui.targetSelect.replaceChildren(...options);
  ui.targetSelect.value = target;
}

function describeState(state: PeerSummary['state']): string {
  switch (state) {
    case 'connected':
      return 'Connected directly';
    case 'connecting':
      return 'Connecting…';
    case 'reconnecting':
      return 'Reconnecting…';
    case 'discovered':
      return 'In the room';
    case 'failed':
      return 'Could not connect';
    default:
      return 'Disconnected';
  }
}

function addNotice(text: string, tone?: 'warn' | 'ok'): void {
  const notice = document.createElement('div');
  notice.className = 'notice';
  if (tone) notice.dataset.tone = tone;
  notice.textContent = text;
  appendToTimeline(notice);
}

function addMessage(message: {
  peerId: string;
  name: string;
  text: string;
  own: boolean;
  direct: boolean;
  to?: string;
}): void {
  const row = messageRow(message.peerId, message.name, message.own);
  const meta = row.querySelector('.msg-meta')!;
  if (message.direct)
    meta.textContent += message.own ? ` · to ${nameOf(message.to ?? '')} only` : ' · only to you';
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  bubble.textContent = message.text;
  row.querySelector('.msg-body')!.append(bubble);
  appendToTimeline(row);
}

function messageRow(peerId: string, name: string, own: boolean): HTMLDivElement {
  const row = document.createElement('div');
  row.className = `msg${own ? ' own' : ''}`;
  const avatar = document.createElement('span');
  paintAvatar(avatar, peerId, name);
  const body = document.createElement('div');
  body.className = 'msg-body';
  const meta = document.createElement('div');
  meta.className = 'msg-meta';
  meta.textContent = `${own ? 'You' : name} · ${clock()}`;
  body.append(meta);
  row.append(avatar, body);
  return row;
}

function appendToTimeline(element: HTMLElement): void {
  const timeline = ui.timeline;
  const nearBottom = timeline.scrollHeight - timeline.scrollTop - timeline.clientHeight < 80;
  timeline.append(element);
  if (nearBottom) timeline.scrollTop = timeline.scrollHeight;
}
class FileCard {
  readonly element: HTMLDivElement;
  active = true;
  private readonly fill: HTMLSpanElement;
  private readonly status: HTMLDivElement;
  private readonly actions: HTMLDivElement;
  private lastStatus = '';

  constructor(
    readonly transferId: string,
    readonly peerId: string,
    name: string,
    private readonly size: number,
    private readonly direction: 'send' | 'receive',
  ) {
    const own = direction === 'send';
    const row = messageRow(own ? mesh!.peerId : peerId, own ? myName : nameOf(peerId), own);
    if (own) row.querySelector('.msg-meta')!.textContent += ` · to ${nameOf(peerId)}`;
    this.element = document.createElement('div');
    this.element.className = 'file-card';
    const top = document.createElement('div');
    top.className = 'file-top';
    top.innerHTML =
      '<span class="file-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/></svg></span>';
    const label = document.createElement('div');
    label.className = 'file-name';
    const title = document.createElement('strong');
    title.textContent = name;
    const detail = document.createElement('small');
    detail.textContent = formatBytes(size);
    label.append(title, detail);
    top.append(label);
    const track = document.createElement('div');
    track.className = 'progress';
    this.fill = document.createElement('span');
    track.append(this.fill);
    this.status = document.createElement('div');
    this.status.className = 'file-status';
    this.actions = document.createElement('div');
    this.actions.className = 'file-actions';
    this.element.append(top, track, this.status, this.actions);
    row.querySelector('.msg-body')!.append(this.element);
    appendToTimeline(row);
  }

  offer(accept: () => void, decline: () => void): void {
    this.setStatus(`${nameOf(this.peerId)} wants to send you this file.`);
    this.setActions(
      button('Accept', 'btn btn-primary btn-sm', () => {
        accept();
        this.setStatus('Starting…');
        this.setActions();
      }),
      button('Decline', 'btn btn-ghost btn-sm', () => {
        decline();
        this.fail('You declined this file.');
      }),
    );
  }

  waiting(cancel: () => void): void {
    this.setStatus(`Waiting for ${nameOf(this.peerId)} to accept…`);
    this.setActions(button('Cancel', 'btn btn-danger btn-sm', cancel));
  }

  progress(done: number, total: number): void {
    if (!this.active) return;
    const percent = total === 0 ? 100 : Math.floor((done / total) * 100);
    this.fill.style.width = `${percent}%`;
    this.element.dataset.state = 'active';
    this.setStatus(
      `${this.direction === 'send' ? 'Sending' : 'Receiving'} · ${percent}% · ${formatBytes(done)} of ${formatBytes(total)}`,
    );
  }

  pause(): void {
    this.element.dataset.state = 'paused';
    this.setStatus('Paused while reconnecting. It will continue where it stopped.');
  }

  resume(): void {
    this.element.dataset.state = 'active';
    this.setStatus('Resuming…');
  }

  complete(sha256: string, blob?: Blob): void {
    if (!this.active) return;
    this.active = false;
    this.fill.style.width = '100%';
    this.element.dataset.state = 'done';
    this.setStatus(`✓ Verified · SHA-256 ${sha256.slice(0, 12)}…`);
    if (blob) {
      const link = document.createElement('a');
      link.className = 'btn btn-soft btn-sm';
      link.href = URL.createObjectURL(blob);
      link.download = (
        this.element.querySelector('.file-name strong')?.textContent ?? 'file'
      ).trim();
      link.textContent = 'Download';
      this.setActions(link);
    } else {
      this.setActions();
    }
  }

  fail(reason: string): void {
    if (!this.active) return;
    this.active = false;
    this.element.dataset.state = 'failed';
    this.setStatus(reason);
    this.setActions();
  }

  private setStatus(text: string): void {
    if (text === this.lastStatus) return;
    this.lastStatus = text;
    this.status.textContent = text;
  }

  private setActions(...elements: HTMLElement[]): void {
    this.actions.replaceChildren(...elements);
  }
}

ui.detailsButton.addEventListener('click', () => (ui.drawer.hidden ? openDrawer() : closeDrawer()));
byId('drawer-close').addEventListener('click', closeDrawer);

function openDrawer(): void {
  ui.drawer.hidden = false;
  ui.room.classList.add('with-drawer');
  ui.detailsButton.setAttribute('aria-expanded', 'true');
  void refreshDetails();
  drawerTimer = setInterval(() => void refreshDetails(), 2_000);
}

function closeDrawer(): void {
  ui.drawer.hidden = true;
  ui.room.classList.remove('with-drawer');
  ui.detailsButton.setAttribute('aria-expanded', 'false');
  clearInterval(drawerTimer);
}

async function refreshDetails(): Promise<void> {
  if (!mesh) return;
  const diagnostics = await mesh.getDiagnostics();
  ui.selfFacts.replaceChildren(
    ...facts([
      ['Your name', myName],
      ['Your peer ID', shortId(mesh.peerId)],
      ['Room', roomId],
      ['Signaling server', hostOf(ui.urlInput.value)],
      ['Server connection', diagnostics.signalingState],
    ]),
  );
  if (diagnostics.peers.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'empty';
    empty.textContent = 'No peer connections yet.';
    ui.peerDetails.replaceChildren(empty);
    return;
  }
  ui.peerDetails.replaceChildren(...diagnostics.peers.map(peerCard));
}

function peerCard(peer: PeerDiagnostics): HTMLElement {
  const card = document.createElement('div');
  card.className = 'peer-card';
  const head = document.createElement('div');
  head.className = 'peer-card-head';
  const avatar = document.createElement('span');
  paintAvatar(avatar, peer.peerId, nameOf(peer.peerId));
  const name = document.createElement('strong');
  name.textContent = nameOf(peer.peerId);
  const badge = document.createElement('span');
  badge.className = 'badge';
  badge.textContent = describeState(peer.state);
  badge.dataset.tone =
    peer.state === 'connected'
      ? 'ok'
      : peer.state === 'failed'
        ? 'error'
        : peer.state === 'closed'
          ? ''
          : 'warn';
  head.append(avatar, name, badge);
  const list = document.createElement('dl');
  list.className = 'facts';
  list.append(
    ...facts([
      ['Route', describeRoute(peer.selectedCandidateType)],
      [
        'Round trip',
        peer.roundTripTimeMs === undefined ? '—' : `${Math.round(peer.roundTripTimeMs)} ms`,
      ],
      ['Sent', peer.bytesSent === undefined ? '—' : formatBytes(peer.bytesSent)],
      ['Received', peer.bytesReceived === undefined ? '—' : formatBytes(peer.bytesReceived)],
      ['Active transfers', String(peer.activeTransfers)],
      ['Reconnect attempts', String(peer.reconnectAttempts)],
    ]),
  );
  const rebuild = button('Rebuild connection', 'btn btn-soft btn-sm', () => {
    toast(`Rebuilding the connection to ${nameOf(peer.peerId)}…`);
    void mesh
      ?.reconnectPeer(peer.peerId)
      .then(() => toast(`New connection to ${nameOf(peer.peerId)} is up.`))
      .catch(showError);
  });
  rebuild.title = 'Closes and reopens the connection. Try it during a file transfer: it resumes.';
  card.append(head, list, rebuild);
  return card;
}

function describeRoute(type: PeerDiagnostics['selectedCandidateType']): string {
  switch (type) {
    case 'host':
      return 'Direct · same network';
    case 'srflx':
    case 'prflx':
      return 'Direct · through NAT';
    case 'relay':
      return 'Relayed via TURN';
    default:
      return '—';
  }
}

function facts(rows: Array<[string, string]>): HTMLElement[] {
  return rows.flatMap(([term, value]) => {
    const dt = document.createElement('dt');
    dt.textContent = term;
    const dd = document.createElement('dd');
    dd.textContent = value;
    dd.title = value;
    return [dt, dd];
  });
}

function log(text: string, tone?: 'error'): void {
  const item = document.createElement('li');
  if (tone) item.dataset.tone = tone;
  const time = document.createElement('time');
  time.textContent = clock(true);
  const body = document.createElement('span');
  body.textContent = text;
  item.append(time, body);
  ui.log.prepend(item);
  while (ui.log.children.length > 200) ui.log.lastElementChild?.remove();
}

for (const id of ['copy-invite', 'copy-invite-empty'])
  byId(id).addEventListener('click', () => void copyInvite());

async function copyInvite(): Promise<void> {
  const url = new URL(window.location.pathname, window.location.origin);
  url.searchParams.set('room', roomId);
  if (signalingFromUrl) url.searchParams.set('signaling', signalingFromUrl);
  try {
    await navigator.clipboard.writeText(url.toString());
    toast('Invite link copied. Open it in another tab or send it to a friend.');
  } catch {
    window.prompt('Copy this invite link:', url.toString());
  }
}

function setStatus(text: string, tone: 'idle' | 'ok' | 'busy' | 'error'): void {
  ui.statusLabel.textContent = text;
  ui.status.dataset.tone = tone;
  ui.status.title = text;
}

function toast(text: string, tone?: 'error'): void {
  const element = document.createElement('div');
  element.className = 'toast';
  if (tone) element.dataset.tone = tone;
  element.textContent = text;
  ui.toasts.append(element);
  while (ui.toasts.children.length > 3) ui.toasts.firstElementChild?.remove();
  setTimeout(() => element.remove(), 4_500);
}

function showError(error: unknown): void {
  toast(errorMessage(error), 'error');
}

function nameOf(peerId: string): string {
  return names.get(peerId) ?? `Peer ${shortId(peerId)}`;
}

function shortId(peerId: string): string {
  return peerId.slice(0, 4);
}

function paintAvatar(
  element: HTMLElement,
  peerId: string,
  name: string,
  state?: PeerSummary['state'],
): void {
  element.className = 'avatar';
  element.textContent = initials(name);
  element.style.setProperty('--hue', String(hue(peerId)));
  if (state) element.dataset.state = state;
}

function initials(name: string): string {
  const words = name.trim().split(/\s+/).filter(Boolean);
  const letters = words.length > 1 ? words[0]![0]! + words[1]![0]! : (words[0] ?? '?').slice(0, 2);
  return letters.toUpperCase();
}

function hue(value: string): number {
  let hash = 0;
  for (const character of value) hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  return hash % 360;
}

function button(label: string, className: string, onClick: () => void): HTMLButtonElement {
  const element = document.createElement('button');
  element.type = 'button';
  element.className = className;
  element.textContent = label;
  element.addEventListener('click', onClick);
  return element;
}

function clock(seconds = false): string {
  return new Date().toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    ...(seconds ? { second: '2-digit' } : {}),
  });
}

function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
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
  return `${value.toFixed(value < 10 ? 1 : 0)} ${unit}`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function pick<T>(items: readonly T[]): T {
  return items[Math.floor(Math.random() * items.length)]!;
}

function randomName(): string {
  return `${pick(ADJECTIVES)} ${pick(ANIMALS)}`;
}

function randomRoom(): string {
  return `${pick(ADJECTIVES)}-${pick(ANIMALS)}-${Math.floor(Math.random() * 90 + 10)}`.toLowerCase();
}

function loadName(): string | undefined {
  try {
    return localStorage.getItem('p2ply-name') ?? undefined;
  } catch {
    return undefined;
  }
}

function saveName(name: string): void {
  try {
    localStorage.setItem('p2ply-name', name);
  } catch {
    // Private mode: the name just isn't remembered.
  }
}
