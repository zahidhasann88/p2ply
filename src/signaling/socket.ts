export type WebSocketLike = Pick<
  WebSocket,
  'readyState' | 'send' | 'close' | 'addEventListener' | 'removeEventListener'
>;
export type WebSocketFactory = (url: string) => WebSocketLike;

export const browserWebSocketFactory: WebSocketFactory = (url) => {
  if (typeof globalThis.WebSocket !== 'function') {
    throw new Error(
      'This environment does not provide WebSocket. Use the Node adapter or provide a factory.',
    );
  }
  return new globalThis.WebSocket(url);
};
