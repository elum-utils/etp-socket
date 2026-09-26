export type EventDefinition<Request = unknown, Response = unknown> = {
  request: Request;
  response: Response;
};

export type ClientEvents = Record<string, EventDefinition>;

export type ServerEvents = Record<string, unknown>;

export const SocketState = {
  Closed: "closed",
  Connecting: "connecting",
  Authenticating: "authenticating",
  Open: "open",
  Reconnecting: "reconnecting",
  Unauthorized: "unauthorized",
  Terminated: "terminated",
} as const;

export type SocketState = (typeof SocketState)[keyof typeof SocketState];

export type SocketErrorCode =
  | "auth"
  | "closed"
  | "connection"
  | "protocol"
  | "timeout"
  | "terminated";

export class SocketError extends Error {
  readonly code: SocketErrorCode;

  constructor(code: SocketErrorCode, message: string) {
    super(message);
    this.name = "SocketError";
    this.code = code;
  }
}

export type ReconnectionOptions = {
  enabled?: boolean;
  attempts?: number;
  delay?: number;
  maxDelay?: number;
};

export type ProtocolOptions = {
  chunkSize?: number;
  maxTransferBytes?: number;
  maxConcurrentTransfers?: number;
  maxInFlightChunks?: number;
  heartbeatInterval?: number;
  heartbeatTimeout?: number;
  ackTimeout?: number;
  retryLimit?: number;
  maxRequestsPerSecond?: number;
  maxFramesPerSecond?: number;
  maxBytesPerSecond?: number;
  checksum?: boolean;
  resumeToken?: Uint8Array;
};

export type SocketOptions = {
  url: string;
  auth?: () => string | Promise<string>;
  autoConnect?: boolean;
  reconnection?: boolean | ReconnectionOptions;
  timeout?: number;
  protocol?: ProtocolOptions;
};

export type WorkerConfig = {
  url: string;
  timeout: number;
  reconnection: Required<ReconnectionOptions>;
  protocol: Required<ProtocolOptions>;
};

export type MainMessage =
  | { type: "configure"; config: WorkerConfig }
  | { type: "connect" | "disconnect" | "terminate" }
  | { type: "auth"; epoch: number; token?: string; error?: string }
  | { type: "emit"; callID: number; event: string; data: unknown }
  | { type: "respond"; requestID: bigint; event: string; data: unknown }
  | { type: "cancel"; callID: number };

export type WorkerMessage =
  | { type: "auth"; epoch: number }
  | { type: "status"; state: SocketState }
  | { type: "disconnect"; reason: DisconnectReason }
  | { type: "event"; event: string; data: unknown; requestID?: bigint }
  | { type: "text"; text: string }
  | { type: "identity"; identity: SocketIdentity }
  | { type: "response"; callID: number; data?: unknown; error?: { code: SocketErrorCode; message: string } }
  | { type: "error"; error: { code: SocketErrorCode; message: string } }
  | { type: "progress"; progress: TransferProgress }
  | { type: "protocol"; event: ProtocolEvent };

export type EmitCallback<Response> = (error: SocketError | null, response?: Response) => void;

export type EmitOptions = {
  signal?: AbortSignal;
  transfer?: boolean;
};

export type TransferProgress = {
  transferID: bigint;
  totalBytes: number;
  sentBytes: number;
  acknowledgedBytes: number;
  receivedBytes?: number;
  direction?: "send" | "receive";
  state: "sending" | "receiving" | "canceling" | "canceled" | "completed" | "failed";
};

export type ProtocolEvent = {
  code: string;
  message: string;
  frameType?: number;
  transferID?: bigint;
  chunkID?: number;
};

export type SocketIdentity = { userID: string };
export type EventResponder<Response = unknown> = (response: Response) => void;

export type DisconnectReason = "auth" | "client" | "connection" | "protocol" | "terminated";

export type Socket<Outgoing extends ClientEvents, Incoming extends ServerEvents> = {
  readonly state: SocketState;
  readonly identity: SocketIdentity | undefined;
  connect(): void;
  disconnect(): void;
  close(): void;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"]): Promise<Outgoing[Event]["response"]>;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], options: EmitOptions): Promise<Outgoing[Event]["response"]>;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], callback: EmitCallback<Outgoing[Event]["response"]>): void;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], options: EmitOptions, callback: EmitCallback<Outgoing[Event]["response"]>): void;
  on<Event extends keyof Incoming & string>(event: Event, listener: (data: Incoming[Event], respond: EventResponder) => void): () => void;
  once<Event extends keyof Incoming & string>(event: Event, listener: (data: Incoming[Event], respond: EventResponder) => void): () => void;
  off<Event extends keyof Incoming & string>(event: Event, listener?: (data: Incoming[Event], respond: EventResponder) => void): void;
  onAny(listener: (event: keyof Incoming & string, data: Incoming[keyof Incoming]) => void): () => void;
  onConnect(listener: () => void): () => void;
  onDisconnect(listener: (reason: DisconnectReason) => void): () => void;
  onError(listener: (error: SocketError) => void): () => void;
  onProgress(listener: (progress: TransferProgress) => void): () => void;
  onProtocolEvent(listener: (event: ProtocolEvent) => void): () => void;
  onText(listener: (text: string) => void): () => void;
};
