import type { WebSocket } from 'ws';

export interface RegisteredPeer {
  peerId: string;
  socket: WebSocket;
  authContext: unknown;
  rooms: Set<string>;
  connectedAt: number;
}

export interface JoinResult {
  joined: boolean;
  peerIds: string[];
}

export interface LeaveResult {
  left: boolean;
  peerIds: string[];
}

export interface PeerRegistry {
  readonly peerCount: number;
  readonly roomCount: number;
  get(peerId: string): RegisteredPeer | undefined;
  add(peer: RegisteredPeer): void;
  remove(peerId: string): RegisteredPeer | undefined;
  join(peerId: string, roomId: string): JoinResult;
  leave(peerId: string, roomId: string): LeaveResult;
  peersInRoom(roomId: string): string[];
}

export class InMemoryPeerRegistry implements PeerRegistry {
  private readonly peers = new Map<string, RegisteredPeer>();
  private readonly rooms = new Map<string, Set<string>>();

  get peerCount(): number {
    return this.peers.size;
  }

  get roomCount(): number {
    return this.rooms.size;
  }

  get(peerId: string): RegisteredPeer | undefined {
    return this.peers.get(peerId);
  }

  add(peer: RegisteredPeer): void {
    if (this.peers.has(peer.peerId)) throw new Error('Peer id is already registered.');
    this.peers.set(peer.peerId, peer);
  }

  remove(peerId: string): RegisteredPeer | undefined {
    const peer = this.peers.get(peerId);
    if (!peer) return undefined;
    this.peers.delete(peerId);
    for (const roomId of peer.rooms) {
      const members = this.rooms.get(roomId);
      members?.delete(peerId);
      if (members?.size === 0) this.rooms.delete(roomId);
    }
    peer.rooms.clear();
    return peer;
  }

  join(peerId: string, roomId: string): JoinResult {
    const peer = this.peers.get(peerId);
    if (!peer) throw new Error('Peer is not registered.');
    let members = this.rooms.get(roomId);
    if (!members) {
      members = new Set();
      this.rooms.set(roomId, members);
    }
    const joined = !members.has(peerId);
    const peerIds = [...members].filter((id) => id !== peerId);
    members.add(peerId);
    peer.rooms.add(roomId);
    return { joined, peerIds };
  }

  leave(peerId: string, roomId: string): LeaveResult {
    const peer = this.peers.get(peerId);
    if (!peer || !peer.rooms.has(roomId)) return { left: false, peerIds: [] };
    const members = this.rooms.get(roomId);
    members?.delete(peerId);
    peer.rooms.delete(roomId);
    const peerIds = [...(members ?? [])];
    if (members?.size === 0) this.rooms.delete(roomId);
    return { left: true, peerIds };
  }

  peersInRoom(roomId: string): string[] {
    return [...(this.rooms.get(roomId) ?? [])];
  }
}
