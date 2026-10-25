import { describe, expect, it } from 'vitest';
import { PeerLink } from '../../src/connection/peer-link.js';
import type {
  DataChannelLike,
  RTCPeerConnectionLike,
  RTCFactory,
} from '../../src/adapters/webrtc.js';
import type { PeerLinkOptions, PeerLinkSignal } from '../../src/connection/peer-link.js';

class FakeChannel implements DataChannelLike {
  readonly label = 'p2ply/1';
  readyState: RTCDataChannelState = 'connecting';
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  binaryType: BinaryType = 'arraybuffer';
  onopen: DataChannelLike['onopen'] = null;
  onclose: DataChannelLike['onclose'] = null;
  onerror: DataChannelLike['onerror'] = null;
  onmessage: DataChannelLike['onmessage'] = null;
  onbufferedamountlow: DataChannelLike['onbufferedamountlow'] = null;
  sent: Array<string | ArrayBuffer | ArrayBufferView | Blob> = [];

  send(data: string | ArrayBuffer | ArrayBufferView | Blob): void {
    this.sent.push(data);
    this.bufferedAmount +=
      typeof data === 'string'
        ? new TextEncoder().encode(data).byteLength
        : data instanceof Blob
          ? data.size
          : data.byteLength;
  }

  close(): void {
    this.readyState = 'closed';
  }
}

class FakePeerConnection implements RTCPeerConnectionLike {
  signalingState: RTCSignalingState = 'stable';
  connectionState: RTCPeerConnectionState = 'new';
  iceConnectionState: RTCIceConnectionState = 'new';
  localDescription: RTCSessionDescriptionInit | null = null;
  remoteDescription: RTCSessionDescriptionInit | null = null;
  onicecandidate: RTCPeerConnectionLike['onicecandidate'] = null;
  ondatachannel: RTCPeerConnectionLike['ondatachannel'] = null;
  onconnectionstatechange: RTCPeerConnectionLike['onconnectionstatechange'] = null;
  oniceconnectionstatechange: RTCPeerConnectionLike['oniceconnectionstatechange'] = null;
  channel = new FakeChannel();
  addedCandidates: RTCIceCandidateInit[] = [];

  createDataChannel(): DataChannelLike {
    return this.channel;
  }
  async createOffer(): Promise<RTCSessionDescriptionInit> {
    return { type: 'offer', sdp: 'fake-offer' };
  }
  async createAnswer(): Promise<RTCSessionDescriptionInit> {
    return { type: 'answer', sdp: 'fake-answer' };
  }
  async setLocalDescription(description?: RTCSessionDescriptionInit): Promise<void> {
    this.localDescription = description ?? null;
    if (description?.type === 'offer') this.signalingState = 'have-local-offer';
    else this.signalingState = 'stable';
  }
  async setRemoteDescription(description: RTCSessionDescriptionInit): Promise<void> {
    this.remoteDescription = description;
    this.signalingState = description.type === 'offer' ? 'have-remote-offer' : 'stable';
  }
  async addIceCandidate(candidate?: RTCIceCandidateInit | null): Promise<void> {
    if (candidate) this.addedCandidates.push(candidate);
  }
  stats: ReadonlyMap<string, unknown> = new Map();
  async getStats(): Promise<ReadonlyMap<string, unknown>> {
    return this.stats;
  }
  close(): void {
    this.connectionState = 'closed';
  }
}

function createTestLink(overrides: Partial<PeerLinkOptions> = {}): {
  link: PeerLink;
  pc: FakePeerConnection;
} {
  const pc = new FakePeerConnection();
  const options: PeerLinkOptions = {
    localPeerId: '123e4567-e89b-42d3-a456-426614174000',
    peerId: '123e4567-e89b-42d3-a456-426614174001',
    roomId: 'test',
    sessionId: '123e4567-e89b-42d3-a456-426614174002',
    initiator: true,
    rtcConfiguration: {},
    rtcFactory: (() => pc) as RTCFactory,
    negotiationTimeoutMs: 10_000,
    disconnectGraceMs: 1_000,
    maxMessageBytes: 256,
    maxBufferedBytes: 128,
    maxQueuedMessages: 1,
    maxQueuedBytes: 256,
    logger: { log: () => {} },
    sendSignal: () => {},
    onData: () => {},
    onState: () => {},
    ...overrides,
  };
  return { link: new PeerLink(options), pc };
}

async function openLocalChannel(link: PeerLink, pc: FakePeerConnection): Promise<FakeChannel> {
  await link.startOffer();
  pc.channel.readyState = 'open';
  pc.channel.onopen?.(new Event('open'));
  return pc.channel;
}

describe('PeerLink bounded writes and stale ICE handling', () => {
  it('waits for DataChannel backpressure to clear and rejects overflow without dropping silently', async () => {
    const { link, pc } = createTestLink();
    const channel = await openLocalChannel(link, pc);
    channel.bufferedAmount = 128;
    const first = link.sendRaw('a');
    const firstResult = first.catch((error: unknown) => error);
    expect(link.queuedMessageCount).toBe(1);
    await expect(link.sendRaw('b')).rejects.toMatchObject({ code: 'QUEUE_OVERFLOW' });
    channel.bufferedAmount = 0;
    channel.onbufferedamountlow?.(new Event('bufferedamountlow'));
    expect(await firstResult).toBeTypeOf('number');
    expect(channel.sent).toEqual(['a']);
    link.close();
  });

  it('ignores stale and duplicate ICE candidates while retaining valid candidates', async () => {
    const { link, pc } = createTestLink();
    await link.acceptOffer('123e4567-e89b-42d3-a456-426614174003', {
      type: 'offer',
      sdp: 'remote',
    });
    const candidate = {
      candidate: 'candidate:1 1 udp 1 127.0.0.1 1234 typ host',
      sdpMid: '0',
      sdpMLineIndex: 0,
    };
    await link.addRemoteCandidate('123e4567-e89b-42d3-a456-426614174002', candidate);
    await link.addRemoteCandidate('123e4567-e89b-42d3-a456-426614174003', candidate);
    await link.addRemoteCandidate('123e4567-e89b-42d3-a456-426614174003', candidate);
    expect(pc.addedCandidates).toEqual([candidate]);
    link.close();
  });

  it('answers an offer collision on a fresh peer connection when polite', async () => {
    const created: FakePeerConnection[] = [];
    const signals: PeerLinkSignal[] = [];
    const { link } = createTestLink({
      localPeerId: '123e4567-e89b-42d3-a456-426614174009',
      rtcFactory: (() => {
        const pc = new FakePeerConnection();
        created.push(pc);
        return pc;
      }) as RTCFactory,
      sendSignal: (signal) => signals.push(signal),
    });
    await link.startOffer();
    expect(created[0]!.signalingState).toBe('have-local-offer');

    const remoteSession = '123e4567-e89b-42d3-a456-426614174004';
    expect(await link.acceptOffer(remoteSession, { type: 'offer', sdp: 'remote' })).toBe(true);
    expect(created).toHaveLength(2);
    expect(created[0]!.connectionState).toBe('closed');
    expect(created[1]!.remoteDescription).toEqual({ type: 'offer', sdp: 'remote' });
    expect(link.sessionId).toBe(remoteSession);
    expect(signals.at(-1)).toMatchObject({ type: 'answer', sessionId: remoteSession });

    created[0]!.onicecandidate?.({
      candidate: { candidate: 'candidate:stale 1 udp 1 127.0.0.1 9 typ host', sdpMid: '0' },
    } as unknown as RTCPeerConnectionIceEvent);
    expect(signals.filter((signal) => signal.type === 'ice-candidate')).toHaveLength(0);
    link.close();
  });

  it('reads diagnostics from a Map-shaped RTCStatsReport like browsers return', async () => {
    const { link, pc } = createTestLink();
    const reports = [
      { id: 'T1', type: 'transport', selectedCandidatePairId: 'CP1' },
      { id: 'CP1', type: 'candidate-pair', currentRoundTripTime: 0.042, localCandidateId: 'L1' },
      { id: 'L1', type: 'local-candidate', candidateType: 'srflx' },
      { id: 'D1', type: 'data-channel', bytesSent: 100, bytesReceived: 40 },
    ];
    pc.stats = new Map(reports.map((report) => [report.id, report]));
    expect(await link.getStats()).toEqual({
      bytesSent: 100,
      bytesReceived: 40,
      roundTripTimeMs: 42,
      selectedCandidateType: 'srflx',
    });
    link.close();
  });

  it('drops an invalid sdpMLineIndex reported by a native adapter before signaling', () => {
    const signals: PeerLinkSignal[] = [];
    const { link, pc } = createTestLink({ sendSignal: (signal) => signals.push(signal) });
    const candidate = 'candidate:1 1 udp 1 127.0.0.1 1234 typ host';
    pc.onicecandidate?.({
      candidate: { candidate, sdpMid: '0', sdpMLineIndex: 4.2e-312 },
    } as unknown as RTCPeerConnectionIceEvent);
    pc.onicecandidate?.({
      candidate: { candidate, sdpMid: '0', sdpMLineIndex: 0 },
    } as unknown as RTCPeerConnectionIceEvent);
    expect(signals.map((signal) => signal.candidate)).toEqual([
      { candidate, sdpMid: '0' },
      { candidate, sdpMid: '0', sdpMLineIndex: 0 },
    ]);
    link.close();
  });
});
