/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Default signaling server, e.g. wss://signal.example.com */
  readonly VITE_SIGNALING_URL?: string;
  /** Comma-separated STUN URLs, e.g. stun:stun.l.google.com:19302 */
  readonly VITE_STUN_URLS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
