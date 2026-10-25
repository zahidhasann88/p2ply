export interface IceCandidateLike {
  candidate: string;
  sdpMid: string | null;
  sdpMLineIndex: number | null;
  usernameFragment?: string | null;
  toJSON?(): RTCIceCandidateInit;
}

export interface DataChannelLike {
  readonly label: string;
  readonly readyState: RTCDataChannelState;
  readonly bufferedAmount: number;
  bufferedAmountLowThreshold: number;
  binaryType: BinaryType;
  onopen: ((event: Event) => void) | null;
  onclose: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent<unknown>) => void) | null;
  onbufferedamountlow: ((event: Event) => void) | null;
  send(data: string | ArrayBuffer | ArrayBufferView | Blob): void;
  close(): void;
}

export interface RTCPeerConnectionLike {
  readonly signalingState: RTCSignalingState;
  readonly connectionState: RTCPeerConnectionState;
  readonly iceConnectionState: RTCIceConnectionState;
  readonly localDescription: RTCSessionDescriptionInit | null;
  readonly remoteDescription: RTCSessionDescriptionInit | null;
  onicecandidate: ((event: RTCPeerConnectionIceEvent) => void) | null;
  ondatachannel: ((event: RTCDataChannelEvent) => void) | null;
  onconnectionstatechange: ((event: Event) => void) | null;
  oniceconnectionstatechange: ((event: Event) => void) | null;
  createDataChannel(label: string, options?: RTCDataChannelInit): DataChannelLike;
  createOffer(options?: RTCOfferOptions): Promise<RTCSessionDescriptionInit>;
  createAnswer(options?: RTCAnswerOptions): Promise<RTCSessionDescriptionInit>;
  setLocalDescription(description?: RTCSessionDescriptionInit): Promise<void>;
  setRemoteDescription(description: RTCSessionDescriptionInit): Promise<void>;
  addIceCandidate(candidate?: RTCIceCandidateInit | null): Promise<void>;
  getStats(): Promise<Iterable<unknown>>;
  close(): void;
}

export type RTCFactory = (configuration: RTCConfiguration) => RTCPeerConnectionLike;

export const browserRTCFactory: RTCFactory = (configuration) => {
  if (typeof globalThis.RTCPeerConnection !== 'function') {
    throw new Error(
      'This environment does not provide RTCPeerConnection. Use the Node adapter or an RTC polyfill.',
    );
  }
  return new globalThis.RTCPeerConnection(configuration) as unknown as RTCPeerConnectionLike;
};
