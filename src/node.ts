import { WebSocket as NodeWebSocket } from 'ws';
import { P2Ply } from './p2ply.js';
import { P2PlyError } from './errors.js';
import type { P2PlyOptions } from './types.js';
import type { RTCFactory } from './adapters/webrtc.js';
import type { WebSocketFactory, WebSocketLike } from './signaling/socket.js';

export class NodeP2Ply<TMessage = unknown, TResponse = unknown> extends P2Ply<TMessage, TResponse> {
  private constructor(
    options: P2PlyOptions<TMessage>,
    rtcFactory: RTCFactory,
    webSocketFactory: WebSocketFactory,
  ) {
    super({
      ...options,
      rtcFactory: options.rtcFactory ?? rtcFactory,
      webSocketFactory: options.webSocketFactory ?? webSocketFactory,
    });
  }

  static async create<TMessage = unknown, TResponse = unknown>(
    options: P2PlyOptions<TMessage>,
  ): Promise<NodeP2Ply<TMessage, TResponse>> {
    let rtcFactory = options.rtcFactory;
    if (!rtcFactory) {
      try {
        const wrtc = await import('@roamhq/wrtc');
        const runtime = (wrtc as unknown as { default?: typeof wrtc }).default ?? wrtc;
        rtcFactory = (configuration) =>
          new runtime.RTCPeerConnection(configuration) as unknown as ReturnType<RTCFactory>;
      } catch (error) {
        throw new P2PlyError(
          'CONFIGURATION_INVALID',
          'Node WebRTC requires the optional peer dependency @roamhq/wrtc. Install it with `pnpm add @roamhq/wrtc` or provide rtcFactory.',
          { cause: error },
        );
      }
    }
    const webSocketFactory: WebSocketFactory =
      options.webSocketFactory ?? ((url) => new NodeWebSocket(url) as unknown as WebSocketLike);
    return new NodeP2Ply<TMessage, TResponse>(options, rtcFactory, webSocketFactory);
  }
}

export async function createNodeP2Ply<TMessage = unknown, TResponse = unknown>(
  options: P2PlyOptions<TMessage>,
): Promise<NodeP2Ply<TMessage, TResponse>> {
  return NodeP2Ply.create<TMessage, TResponse>(options);
}
