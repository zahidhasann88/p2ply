export interface TransferMetadata {
  name: string;
  size: number;
  mimeType: string;
  lastModified: number;
  chunkSize: number;
  totalChunks: number;
}

export interface FileTransferResult {
  transferId: string;
  peerId: string;
  name: string;
  size: number;
  mimeType: string;
  sha256: string;
  blob?: Blob;
  sinkResult?: unknown;
}

export interface FileSink {
  write(chunk: Uint8Array): Promise<void> | void;
  close(metadata: TransferMetadata & { sha256: string }): Promise<unknown> | unknown;
  abort(reason: string): Promise<void> | void;
}

export type FileSinkFactory = (context: {
  peerId: string;
  transferId: string;
  metadata: TransferMetadata;
}) => FileSink | Promise<FileSink>;

export interface FileInput extends Blob {
  readonly name?: string;
  readonly lastModified?: number;
}
