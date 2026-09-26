import * as p from "./protocol";
import {
  SocketState,
  type DisconnectReason,
  type MainMessage,
  type SocketErrorCode,
  type TransferProgress,
  type WorkerConfig,
  type WorkerMessage,
} from "./types";

type Pending = {
  callID: number;
  timeout?: ReturnType<typeof setTimeout>;
};

type TokenState = { tokens: number; updatedAt: number };

type OutgoingTransfer = {
  callID: number;
  requestID: bigint;
  transferID: bigint;
  bytes: Uint8Array;
  chunkSize: number;
  maxInFlightChunks: number;
  nextChunk: number;
  nextOffset: number;
  inFlight: Map<number, { payload: Uint8Array; sentAt: number; retries: number; queued: boolean }>;
  windowBytes: bigint;
  windowChunks: number;
  ended: boolean;
  acknowledgedBytes: number;
  commitSentAt: number;
  commitRetries: number;
  commitQueued: boolean;
};

type WriteQueue = {
  frames: ArrayBuffer[];
  head: number;
};

type IncomingTransfer = {
  requestID: bigint;
  transferID: bigint;
  totalSize: number;
  chunkSize: number;
  chunkCount: number;
  event: string;
  field: string;
  fields: p.Field[];
  parts: p.TransferPart[];
  contentType: number;
  flags: number;
  checksum: Uint8Array;
  chunks: Uint8Array[];
  receivedBytes: number;
  nextChunk: number;
  controlBatch: number;
  ackPending: number;
  windowPending: number;
  ackFirstChunk: number;
};

type BinaryPart = Blob | ArrayBuffer | Uint8Array;
type MultipartPayload = { __etpMultipart: true; fields: Array<{ key: string; value: string }>; parts: Array<{ field: string; index: number; name: string; blob: BinaryPart }> };

type WorkerScope = {
  onmessage: ((event: MessageEvent<MainMessage>) => void) | null;
  postMessage(message: WorkerMessage): void;
  close(): void;
};

const scope = self as unknown as WorkerScope;
const encoder = new TextEncoder();
const BrowserCapabilities = p.Capability.Transfers | p.Capability.Cancel | p.Capability.Ack | p.Capability.Nack | p.Capability.Heartbeat | p.Capability.TransferSHA256 | p.Capability.FlowControl | p.Capability.RequestResponse | p.Capability.GracefulClose | p.Capability.TransferResume | p.Capability.TransferCommit | p.Capability.RateLimits;

let config: WorkerConfig | undefined;
let socket: WebSocket | undefined;
let heartbeat: ReturnType<typeof setInterval> | undefined;
let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
let handshakeTimer: ReturnType<typeof setTimeout> | undefined;
let nextRequestID = 1000n;
let authEpoch = 0;
let reconnectAttempt = 0;
let desiredConnection = false;
let terminated = false;
let unauthorized = false;
let protocolFailure = false;
let lastWriteAt = 0;
let lastReadAt = 0;
let phase: "closed" | "auth" | "hello" | "open" | "draining" | "closing" = "closed";
const pending = new Map<bigint, Pending>();
const outgoingTransfers = new Map<bigint, OutgoingTransfer>();
const incomingTransfers = new Map<bigint, IncomingTransfer>();
const completedIncoming = new Map<bigint, { receivedBytes: bigint; nextChunk: number }>();
let nextTransferID = 7000n;
let remoteHello: p.Hello | undefined;
let requestTokens: TokenState | undefined;
let frameTokens: TokenState | undefined;
let byteTokens: TokenState | undefined;
let resumePending = false;
let incomingRequestTokens: TokenState | undefined;
let incomingFrameTokens: TokenState | undefined;
let incomingByteTokens: TokenState | undefined;
const controlWrites: WriteQueue = { frames: [], head: 0 };
const realtimeWrites: WriteQueue = { frames: [], head: 0 };
const bulkWrites: WriteQueue = { frames: [], head: 0 };
const writeChannel = new MessageChannel();
let writeScheduled = false;
let writeCreditWake = false;
let writeTimer: ReturnType<typeof setTimeout> | undefined;
let queuedWriteFrames = 0;
let queuedWriteBytes = 0;
const MaxQueuedWriteFrames = 4_096;
const MaxQueuedWriteBytes = 64 << 20;
const MaxBulkBurstFrames = 8;
const ProgressIntervalMillis = 32;
const OutboundByteRateSafety = 0.99;
const OutboundByteReserveChunks = 4;
const pendingProgress = new Map<bigint, TransferProgress>();
const progressPublishedAt = new Map<bigint, number>();
let progressTimer: ReturnType<typeof setTimeout> | undefined;
writeChannel.port1.onmessage = () => {
  writeScheduled = false;
  drainWrites();
};

scope.onmessage = ({ data }: MessageEvent<MainMessage>) => {
  switch (data.type) {
    case "configure":
      config = data.config;
      resetIncomingRateLimits();
      return;
    case "connect":
      if (terminated) {
        emitError("terminated", "socket worker is terminated");
        return;
      }
      desiredConnection = true;
      unauthorized = false;
      protocolFailure = false;
      reconnectAttempt = 0;
      requestAuth(false);
      return;
    case "disconnect":
      desiredConnection = false;
      clearReconnectTimer();
      closeSocket("client");
      return;
    case "terminate":
      terminated = true;
      desiredConnection = false;
      clearReconnectTimer();
      stopHeartbeat();
      clearHandshakeTimer();
      clearWriteScheduler();
      clearProgressScheduler();
      rejectPending("terminated", "socket worker is terminated");
      if (socket) {
        socket.onclose = null;
        socket.close();
        socket = undefined;
      }
      post({ type: "status", state: SocketState.Terminated });
      post({ type: "disconnect", reason: "terminated" });
      scope.close();
      return;
    case "auth":
      if (data.epoch !== authEpoch || !desiredConnection || terminated) {
        return;
      }
      if (data.error) {
        emitError("auth", data.error);
        scheduleReconnect();
        return;
      }
      openSocket(data.token ?? "");
      return;
    case "emit":
      void sendRequest(data.callID, data.event, data.data);
      return;
    case "respond":
      void sendInboundResponse(data.requestID, data.event, data.data);
      return;
    case "cancel":
      cancelCall(data.callID);
      return;
  }
};

function requestAuth(reconnecting: boolean): void {
  if (!config || socket || reconnectTimer || !desiredConnection) {
    return;
  }
  post({ type: "status", state: reconnecting ? SocketState.Reconnecting : SocketState.Connecting });
  authEpoch += 1;
  post({ type: "auth", epoch: authEpoch });
}

function openSocket(token: string): void {
  if (!config || socket) {
    return;
  }
  clearWriteScheduler();
  const current = new WebSocket(config.url);
  socket = current;
  current.binaryType = "arraybuffer";
  current.onopen = () => {
    if (socket !== current) {
      return;
    }
    phase = "auth";
    post({ type: "status", state: SocketState.Authenticating });
    sendRaw(p.encodeAuth(token));
    startHandshakeTimer();
  };
  current.onmessage = ({ data }: MessageEvent<ArrayBuffer>) => handleMessage(current, data);
  current.onerror = () => emitError("connection", "WebSocket transport error");
  current.onclose = () => handleClose(current);
}

function handleMessage(current: WebSocket, data: ArrayBuffer): void {
  if (socket !== current) {
    return;
  }
  try {
    const frame = p.decodeFrame(data);
    enforceIncomingRate(frame, data.byteLength);
    lastReadAt = Date.now();
    enforceFrameCapability(frame.type, frame.transferID);
    switch (frame.type) {
      case p.FrameType.AuthAccept:
        requirePhase("auth", "unexpected auth acceptance");
        post({ type: "identity", identity: { userID: p.decodeAuthAccept(frame) } });
        phase = "hello";
        sendRaw(p.encodeHello({ capabilities: BrowserCapabilities, maxChunkSize: config!.protocol.chunkSize, maxTransferBytes: BigInt(config!.protocol.maxTransferBytes), maxInFlightChunks: config!.protocol.maxInFlightChunks, heartbeatMillis: config!.protocol.heartbeatInterval, rateLimits: { maxRequestsPerSecond: config!.protocol.maxRequestsPerSecond, requestBurst: config!.protocol.maxRequestsPerSecond, maxFramesPerSecond: config!.protocol.maxFramesPerSecond, frameBurst: config!.protocol.maxFramesPerSecond, maxBytesPerSecond: BigInt(config!.protocol.maxBytesPerSecond), byteBurst: BigInt(config!.protocol.maxBytesPerSecond), availableRequests: config!.protocol.maxRequestsPerSecond, availableFrames: config!.protocol.maxFramesPerSecond, availableBytes: BigInt(config!.protocol.maxBytesPerSecond) } }));
        return;
      case p.FrameType.AuthReject:
        requirePhase("auth", "unexpected auth rejection");
        unauthorized = true;
        desiredConnection = false;
        post({ type: "status", state: SocketState.Unauthorized });
        emitError("auth", p.decodeAuthReject(frame) || "authentication rejected");
        current.close();
        return;
      case p.FrameType.HelloAck:
        requirePhase("hello", "unexpected hello acknowledgment");
        remoteHello = p.decodeHello(frame);
        if (remoteHello.role !== "server") {
          throw new p.ProtocolError("unexpected ETP hello role");
        }
        if (remoteHello.capabilities & ~p.AllCapabilities) {
          throw new p.ProtocolError("remote advertised unknown ETP capabilities");
        }
        if (!(remoteHello.capabilities & p.Capability.RateLimits)) throw new p.ProtocolError("server does not support required ETP rate-limit negotiation");
        resetOutboundRateLimits();
        clearHandshakeTimer();
        phase = "open";
        reconnectAttempt = 0;
        lastWriteAt = Date.now();
        lastReadAt = Date.now();
        post({ type: "status", state: SocketState.Open });
        startHeartbeat();
        resumeOutgoingTransfers();
        return;
      case p.FrameType.Ping:
        requireTransferPhase("ping before handshake completion");
        sendRaw(p.encodePong());
        return;
      case p.FrameType.Pong:
        requireTransferPhase("pong before handshake completion");
        return;
      case p.FrameType.Response:
        requirePhase("open", "response before handshake completion");
        resolveResponse(frame.requestID, materializeEventMessage(p.decodeEvent(frame)));
        return;
      case p.FrameType.Request: {
        requirePhase("open", "request before handshake completion");
        const event = p.decodeEvent(frame);
        post({ type: "event", event: event.event, data: materializeEventMessage(event), requestID: frame.requestID });
        return;
      }
      case p.FrameType.Error:
        requireTransferPhase("error before handshake completion");
        const remoteError = p.decodeError(frame);
        post({ type: "protocol", event: { code: `remote_error_${remoteError.code}`, message: remoteError.message, frameType: remoteError.frameType, transferID: remoteError.transferID } });
        rejectResponse(remoteError.requestID || frame.requestID, remoteError.code === 2 ? "auth" : "protocol", remoteError.message || "server returned an ETP error");
        if (remoteError.code === 1 || remoteError.code === 2) throw new p.ProtocolError(`fatal remote ETP error ${remoteError.code}`);
        return;
      case p.FrameType.Ack:
        requireTransferPhase("ack before handshake completion");
        handleTransferAck(p.decodeAck(frame));
        return;
      case p.FrameType.Nack:
        requireTransferPhase("nack before handshake completion");
        handleTransferNack(p.decodeNack(frame));
        return;
      case p.FrameType.Window:
        requireTransferPhase("window before handshake completion");
        handleTransferWindow(p.decodeWindow(frame));
        return;
      case p.FrameType.TransferState:
        requireTransferPhase("transfer state before handshake completion");
        handleTransferState(p.decodeTransferState(frame));
        return;
      case p.FrameType.TransferBegin:
        requirePhase("open", "transfer begin before handshake completion");
        handleIncomingTransferBegin(frame);
        return;
      case p.FrameType.Data:
        requireTransferPhase("transfer data before handshake completion");
        if (frame.transferID === 0n) {
          post({ type: "text", text: p.decodeText(frame) });
          return;
        }
        handleIncomingTransferData(frame);
        return;
      case p.FrameType.TransferEnd:
        requireTransferPhase("transfer end before handshake completion");
        void handleIncomingTransferEnd(frame).catch((error) => failProtocol(current, error));
        return;
      case p.FrameType.TransferResume:
        requirePhase("open", "transfer resume before handshake completion");
        handleIncomingTransferResume(frame);
        return;
      case p.FrameType.Cancel: {
        requireTransferPhase("cancel before handshake completion");
        const cancel = p.decodeCancel(frame);
        const outgoing = outgoingTransfers.get(cancel.transferID), incoming = incomingTransfers.get(cancel.transferID), completed = completedIncoming.has(cancel.transferID);
        if (outgoing) { outgoingTransfers.delete(cancel.transferID); emitProgress(outgoing, "canceled"); rejectResponse(outgoing.requestID, "closed", "transfer was canceled by the server"); }
        if (incoming) { incomingTransfers.delete(cancel.transferID); emitIncomingProgress(incoming, "canceled"); }
        sendRaw(p.encodeCancelAck(cancel.transferID, completed ? 3 : outgoing || incoming ? 1 : 2));
        return;
      }
      case p.FrameType.CancelAck: {
        requireTransferPhase("cancel acknowledgment before handshake completion");
        const ack = p.decodeCancelAck(frame);
        post({ type: "protocol", event: { code: "cancel_ack", message: `transfer cancellation status ${ack.status}`, frameType: frame.type, transferID: ack.transferID } });
        return;
      }
      case p.FrameType.Close:
        if (phase !== "open" && phase !== "draining" && phase !== "closing") throw new p.ProtocolError("close before handshake completion");
        handlePeerClose(current, p.decodeClose(frame));
        return;
      case p.FrameType.GoAway:
        requirePhase("open", "goaway before handshake completion");
        const goAway = p.decodeGoAway(frame);
        if (goAway.flags & p.CloseFlag.Drain) {
          if (!(remoteHello?.capabilities && (remoteHello.capabilities & p.Capability.GracefulClose))) throw new p.ProtocolError("graceful close was not negotiated");
          phase = "draining";
        } else {
          phase = "closing";
          current.close();
        }
        return;
      case p.FrameType.CloseAck:
        current.close();
        return;
      default:
        throw new p.ProtocolError(`unsupported ETP frame type ${frame.type}`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "invalid ETP frame";
    protocolFailure = true;
    desiredConnection = false;
    emitError("protocol", message);
    current.close();
  }
}

async function sendRequest(callID: number, event: string, data: unknown): Promise<void> {
  if (!socket || socket.readyState !== WebSocket.OPEN || !isOpen()) {
    postResponse(callID, undefined, { code: "closed", message: "socket is not connected" });
    return;
  }
  const requestID = ++nextRequestID;
  pending.set(requestID, { callID });
  try {
    if (isMultipartPayload(data)) {
      const [bytes, parts] = await readMultipart(data);
      if (!pending.has(requestID)) return;
      await startTransfer(callID, requestID, event, bytes, "", parts, data.fields);
      return;
    }
    if (data instanceof Blob) {
      const bytes = new Uint8Array(await data.arrayBuffer());
      if (!pending.has(requestID)) return;
      await startTransfer(callID, requestID, event, bytes, typeof File !== "undefined" && data instanceof File ? data.name : "");
      return;
    }
    if (data instanceof ArrayBuffer) {
      await startTransfer(callID, requestID, event, new Uint8Array(data));
      return;
    }
    if (data instanceof Uint8Array) {
      await startTransfer(callID, requestID, event, data);
      return;
    }
    const body = encoder.encode(JSON.stringify(data) ?? "null");
    const inlineLimit = Math.min(64 << 10, (remoteHello?.maxFrameBytes ?? 8 << 20) - 40);
    if (body.length <= inlineLimit) {
      sendRaw(p.encodeRequest(requestID, event, data));
    } else {
      await startTransfer(callID, requestID, event, body);
    }
  } catch (error) {
    clearPendingTimeout(requestID);
    pending.delete(requestID);
    postResponse(callID, undefined, {
      code: "protocol",
      message: error instanceof Error ? error.message : "request encoding failed",
    });
  }
}

async function sendInboundResponse(requestID: bigint, event: string, data: unknown): Promise<void> {
  if (!socket || socket.readyState !== WebSocket.OPEN || !isOpen()) return;
  try {
    if (isMultipartPayload(data)) {
      const [bytes, parts] = await readMultipart(data);
      await startTransfer(0, requestID, event, bytes, "", parts, data.fields, false);
      return;
    }
    const binary = data instanceof Blob ? new Uint8Array(await data.arrayBuffer()) : data instanceof ArrayBuffer ? new Uint8Array(data) : data instanceof Uint8Array ? data : undefined;
    if (binary) { await startTransfer(0, requestID, event, binary, "", [], [], false); return; }
    const body = encoder.encode(JSON.stringify(data) ?? "null");
    const inlineLimit = Math.min(64 << 10, (remoteHello?.maxFrameBytes ?? 8 << 20) - 40);
    if (body.length <= inlineLimit) sendRaw(p.encodeResponse(requestID, event, data));
    else await startTransfer(0, requestID, event, body, "", [], [], false);
  } catch (error) {
    emitError("protocol", error instanceof Error ? error.message : "response encoding failed");
  }
}

async function startTransfer(callID: number, requestID: bigint, event: string, bytes: Uint8Array, name = "", parts: p.TransferPart[] = [], fields: p.Field[] = [], requirePending = true): Promise<void> {
  if (!remoteHello || !(remoteHello.capabilities & p.Capability.Transfers) || !(remoteHello.capabilities & p.Capability.Ack) || !(remoteHello.capabilities & p.Capability.TransferCommit)) throw new p.ProtocolError("remote peer does not support ETP transfers");
  if (BigInt(bytes.length) > remoteHello.maxTransferBytes) throw new p.ProtocolError("message exceeds remote ETP transfer limit");
  const transferID = ++nextTransferID, chunkSize = Math.min(config?.protocol.chunkSize ?? p.DefaultChunkSize, remoteHello.maxChunkSize), chunkCount = Math.ceil(bytes.length / chunkSize);
  const checksumEnabled = Boolean(config?.protocol.checksum && remoteHello.capabilities & p.Capability.TransferSHA256);
  const checksum = checksumEnabled
    ? new Uint8Array(await crypto.subtle.digest("SHA-256", bytes.buffer instanceof ArrayBuffer && bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength ? bytes.buffer : bytes.slice().buffer))
    : new Uint8Array(32);
  if (requirePending && !pending.has(requestID)) return;
  const transfer: OutgoingTransfer = { callID, requestID, transferID, bytes, chunkSize, maxInFlightChunks: Math.min(config?.protocol.maxInFlightChunks ?? 16, remoteHello.maxInFlightChunks), nextChunk: 0, nextOffset: 0, inFlight: new Map(), windowBytes: 0n, windowChunks: 0, ended: false, acknowledgedBytes: 0, commitSentAt: 0, commitRetries: 0, commitQueued: false };
  outgoingTransfers.set(transferID, transfer);
  try {
    sendRaw(p.encodeTransferBegin(transferID, requestID, { totalSize: BigInt(bytes.length), chunkSize, chunkCount, contentType: 1, flags: checksumEnabled ? p.TransferFlag.ChecksumSHA256 : 0, checksum, name, event, field: "", index: 0, parts, fields }));
  } catch (error) {
    outgoingTransfers.delete(transferID);
    throw error;
  }
  if (!(remoteHello.capabilities & p.Capability.FlowControl)) {
    transfer.windowBytes = BigInt(bytes.length);
    transfer.windowChunks = remoteHello.maxInFlightChunks;
    pumpTransfer(transfer);
  }
}

function isMultipartPayload(data: unknown): data is MultipartPayload { return Boolean(data && typeof data === "object" && (data as { __etpMultipart?: unknown }).__etpMultipart === true); }

async function readMultipart(data: MultipartPayload): Promise<[Uint8Array, p.TransferPart[]]> {
  const parts = data.parts.map((part) => ({ field: part.field, index: part.index, name: part.name, totalSize: BigInt(binaryPartSize(part.blob)), contentType: 1 }));
  if (data.parts.length === 1) return [await binaryPartBytes(data.parts[0].blob), parts];
  const bytes = new Uint8Array(data.parts.reduce((size, part) => size + binaryPartSize(part.blob), 0));
  let offset = 0;
  for (const part of data.parts) { const chunk = await binaryPartBytes(part.blob); bytes.set(chunk, offset); offset += chunk.length; }
  return [bytes, parts];
}

function binaryPartSize(part: BinaryPart): number {
  return part instanceof Blob ? part.size : part.byteLength;
}

async function binaryPartBytes(part: BinaryPart): Promise<Uint8Array> {
  if (part instanceof Blob) return new Uint8Array(await part.arrayBuffer());
  if (part instanceof ArrayBuffer) return new Uint8Array(part);
  return part;
}

function handleTransferWindow(window: p.Window): void {
  if (window.flags !== p.WindowFlag.Transfer) throw new p.ProtocolError("invalid transfer window");
  const transfer = outgoingTransfers.get(window.transferID);
  if (!transfer) return;
  transfer.windowBytes = window.windowBytes;
  transfer.windowChunks = window.windowChunks;
  pumpTransfer(transfer);
}

function handleTransferAck(ack: p.Ack): void {
  const transfer = outgoingTransfers.get(ack.transferID);
  if (!transfer) return;
  let matched = false, acknowledged = transfer.acknowledgedBytes;
  for (let chunk = ack.chunkFrom; chunk <= ack.chunkTo; chunk += 1) {
    const sent = transfer.inFlight.get(chunk);
    if (!sent) continue;
    matched = true; acknowledged += sent.payload.length; transfer.inFlight.delete(chunk);
  }
  if ((!matched && ack.receivedBytes > BigInt(transfer.acknowledgedBytes)) || (matched && ack.receivedBytes !== BigInt(acknowledged))) throw new p.ProtocolError("ack byte counter does not match committed chunks");
  transfer.acknowledgedBytes = Number(ack.receivedBytes);
  emitProgress(transfer, "sending");
  writeCreditWake = true;
  pumpTransfer(transfer);
  scheduleWriteDrain(0);
}

function handleTransferNack(nack: p.Nack): void {
  const transfer = outgoingTransfers.get(nack.transferID);
  if (!transfer) return;
  post({ type: "protocol", event: { code: "nack_received", message: `NACK ${nack.chunkFrom}-${nack.chunkTo}`, frameType: p.FrameType.Nack, transferID: nack.transferID, chunkID: nack.chunkFrom } });
  if (nack.reasonCode === 7) { transfer.windowBytes = 0n; transfer.windowChunks = 0; return; }
  if (nack.reasonCode !== 1) { outgoingTransfers.delete(nack.transferID); emitProgress(transfer, "failed"); rejectResponse(transfer.requestID, "protocol", `remote rejected transfer: ${nackReason(nack.reasonCode)} (reason ${nack.reasonCode})`); return; }
  const total = Math.ceil(transfer.bytes.length / transfer.chunkSize);
  for (let chunk = nack.chunkFrom; chunk <= nack.chunkTo; chunk += 1) {
    const sent = transfer.inFlight.get(chunk);
    if (sent && !sent.queued) { sent.queued = true; sent.retries += 1; sendRaw(p.encodeData(transfer.transferID, transfer.requestID, chunk, sent.payload, chunk === 0, chunk + 1 === total)); }
  }
}

function handleTransferState(state: p.TransferState): void {
  const transfer = outgoingTransfers.get(state.transferID);
  if (!transfer) return;
  if (state.flags & p.TransferStateFlag.ResumeAccepted) {
    const expected = Math.min(transfer.bytes.length, state.nextChunk * transfer.chunkSize);
    if (state.receivedBytes !== BigInt(expected)) throw new p.ProtocolError("invalid transfer resume counters");
    transfer.acknowledgedBytes = Number(state.receivedBytes);
    transfer.nextOffset = transfer.acknowledgedBytes;
    transfer.nextChunk = state.nextChunk;
    transfer.inFlight.clear(); transfer.ended = false; transfer.commitSentAt = 0; transfer.commitRetries = 0; transfer.commitQueued = false;
    emitProgress(transfer, "sending");
    return;
  }
  if (state.flags & p.TransferStateFlag.Completed) {
    const chunkCount = Math.ceil(transfer.bytes.length / transfer.chunkSize);
    if (state.receivedBytes !== BigInt(transfer.bytes.length) || state.nextChunk !== chunkCount) throw new p.ProtocolError("invalid transfer completion counters");
  }
  if (state.flags & p.TransferStateFlag.ResumeRejected) {
    outgoingTransfers.delete(state.transferID);
    emitProgress(transfer, "failed");
    rejectResponse(transfer.requestID, "protocol", `transfer resume rejected with reason ${state.reasonCode}`);
    return;
  }
  outgoingTransfers.delete(state.transferID);
  if (state.flags & p.TransferStateFlag.Failed) rejectResponse(transfer.requestID, "protocol", `transfer failed with reason ${state.reasonCode}`);
  emitProgress(transfer, state.flags & p.TransferStateFlag.Failed ? "failed" : "completed");
}

function nackReason(reason: number): string {
  return ["unknown", "missing chunk", "invalid chunk", "unknown transfer", "canceled transfer", "receiver write failed", "protocol error", "flow control exceeded"][reason] ?? "unknown error";
}

function resumeOutgoingTransfers(): void {
  if (!resumePending) return;
  resumePending = false;
  if (!remoteHello || !(remoteHello.capabilities & p.Capability.TransferResume)) {
    for (const transfer of outgoingTransfers.values()) rejectResponse(transfer.requestID, "protocol", "remote no longer supports transfer resume");
    outgoingTransfers.clear();
    return;
  }
  for (const transfer of outgoingTransfers.values()) {
    sendRaw(p.encodeTransferResume(transfer.requestID, { transferID: transfer.transferID, receivedBytes: BigInt(transfer.acknowledgedBytes), nextChunk: Math.ceil(transfer.acknowledgedBytes / transfer.chunkSize), token: config?.protocol.resumeToken ?? new Uint8Array() }));
  }
}

function cancelTransferForRequest(requestID: bigint, reasonCode: number): void {
  for (const transfer of outgoingTransfers.values()) {
    if (transfer.requestID === requestID) {
      emitProgress(transfer, "canceling");
      outgoingTransfers.delete(transfer.transferID);
      if (socket?.readyState === WebSocket.OPEN) sendRaw(p.encodeCancel(transfer.transferID, reasonCode, 1));
      return;
    }
  }
}

function cancelCall(callID: number): void {
  for (const [requestID, request] of pending) {
    if (request.callID !== callID) continue;
    if (request.timeout !== undefined) clearTimeout(request.timeout);
    pending.delete(requestID);
    cancelTransferForRequest(requestID, 1);
    postResponse(callID, undefined, { code: "closed", message: "request was canceled" });
    return;
  }
}

function pumpTransfer(transfer: OutgoingTransfer): void {
  while (!transfer.ended && transfer.nextOffset < transfer.bytes.length && transfer.inFlight.size < transfer.maxInFlightChunks && transfer.windowChunks > 0 && transfer.windowBytes > 0n) {
    const size = Math.min(transfer.chunkSize, transfer.bytes.length - transfer.nextOffset);
    if (BigInt(size) > transfer.windowBytes) return;
    const chunkID = transfer.nextChunk, payload = transfer.bytes.slice(transfer.nextOffset, transfer.nextOffset + size);
    const last = transfer.nextOffset + size === transfer.bytes.length;
    const ackBatch = Math.max(1, Math.floor(transfer.maxInFlightChunks / 2));
    transfer.inFlight.set(chunkID, { payload, sentAt: 0, retries: 0, queued: true });
    sendRaw(p.encodeData(transfer.transferID, transfer.requestID, chunkID, payload, chunkID === 0, last, (chunkID + 1) % ackBatch === 0 || last));
    transfer.nextChunk += 1; transfer.nextOffset += size; transfer.windowChunks -= 1; transfer.windowBytes -= BigInt(size);
    emitProgress(transfer, "sending");
  }
  if (!transfer.ended && transfer.nextOffset === transfer.bytes.length && transfer.inFlight.size === 0) {
    transfer.ended = true;
    transfer.commitQueued = true;
    sendRaw(p.encodeTransferEnd(transfer.transferID, transfer.requestID));
  }
}

function emitProgress(transfer: OutgoingTransfer, state: TransferProgress["state"]): void {
  queueProgress({ transferID: transfer.transferID, totalBytes: transfer.bytes.length, sentBytes: transfer.nextOffset, acknowledgedBytes: transfer.acknowledgedBytes, state });
}

function handleIncomingTransferBegin(frame: p.Frame): void {
  const begin = p.decodeTransferBegin(frame);
  const partsSize = begin.parts.reduce((size, part) => size + part.totalSize, 0n);
  if (incomingTransfers.size >= (config?.protocol.maxConcurrentTransfers ?? 16) || (frame.requestID === 0n) !== (begin.event.length === 0) || begin.totalSize > BigInt(config?.protocol.maxTransferBytes ?? 64 << 20) || begin.totalSize > BigInt(Number.MAX_SAFE_INTEGER) || begin.chunkSize === 0 || begin.chunkSize > (config?.protocol.chunkSize ?? p.DefaultChunkSize) || begin.chunkCount !== Math.ceil(Number(begin.totalSize) / begin.chunkSize) || begin.flags & ~p.TransferFlag.ChecksumSHA256 || (begin.parts.length > 0 && partsSize !== begin.totalSize)) {
    sendRaw(p.encodeNack({ transferID: frame.transferID, chunkFrom: 0, chunkTo: 0, reasonCode: 6, flags: 0 }));
    return;
  }
  if (incomingTransfers.has(frame.transferID)) throw new p.ProtocolError("duplicate incoming transfer");
  const receiveWindow = config?.protocol.maxInFlightChunks ?? 16;
  const transfer: IncomingTransfer = { requestID: frame.requestID, transferID: frame.transferID, totalSize: Number(begin.totalSize), chunkSize: begin.chunkSize, chunkCount: begin.chunkCount, event: begin.event, field: begin.field, fields: begin.fields, parts: begin.parts, contentType: begin.contentType, flags: begin.flags, checksum: begin.checksum, chunks: [], receivedBytes: 0, nextChunk: 0, controlBatch: Math.max(1, Math.floor(receiveWindow / 2)), ackPending: 0, windowPending: 0, ackFirstChunk: 0 };
  incomingTransfers.set(frame.transferID, transfer);
  emitIncomingProgress(transfer, "receiving");
  sendRaw(p.encodeWindow({ transferID: frame.transferID, windowBytes: BigInt(Math.min(transfer.totalSize, receiveWindow * transfer.chunkSize)), windowChunks: receiveWindow, flags: p.WindowFlag.Transfer }));
}

function handleIncomingTransferData(frame: p.Frame): void {
  const transfer = incomingTransfers.get(frame.transferID);
  if (!transfer) {
    sendRaw(p.encodeNack({ transferID: frame.transferID, chunkFrom: frame.chunkID, chunkTo: frame.chunkID, reasonCode: 3, flags: 0 }));
    return;
  }
  if (frame.chunkID < transfer.nextChunk) {
    sendRaw(p.encodeAck({ transferID: frame.transferID, chunkFrom: 0, chunkTo: transfer.nextChunk - 1, receivedBytes: BigInt(transfer.receivedBytes) }));
    return;
  }
  if (frame.chunkID > transfer.nextChunk || frame.chunkID >= transfer.chunkCount || !frame.payload.length) {
    sendRaw(p.encodeNack({ transferID: frame.transferID, chunkFrom: transfer.nextChunk, chunkTo: frame.chunkID, reasonCode: 1, flags: 0 }));
    return;
  }
  const expected = frame.chunkID + 1 === transfer.chunkCount ? transfer.totalSize - frame.chunkID * transfer.chunkSize : transfer.chunkSize;
  const allowedFlags = 1 | 2 | 4;
  const first = Boolean(frame.flags & 1), last = Boolean(frame.flags & 2);
  if (frame.flags & ~allowedFlags || first !== (frame.chunkID === 0) || last !== (frame.chunkID + 1 === transfer.chunkCount) || frame.payload.length !== expected) {
    sendRaw(p.encodeNack({ transferID: frame.transferID, chunkFrom: frame.chunkID, chunkTo: frame.chunkID, reasonCode: 2, flags: 0 }));
    return;
  }
  transfer.chunks.push(frame.payload.slice());
  transfer.receivedBytes += frame.payload.length;
  transfer.nextChunk += 1;
  if (transfer.ackPending === 0) transfer.ackFirstChunk = frame.chunkID;
  transfer.ackPending += 1;
  transfer.windowPending += 1;
  emitIncomingProgress(transfer, "receiving");
  const flushAck = transfer.ackPending >= transfer.controlBatch || Boolean(frame.flags & p.FrameFlag.AckRequest) || last;
  const flushWindow = transfer.windowPending >= transfer.controlBatch || last;
  if (flushAck) {
    sendRaw(p.encodeAck({ transferID: frame.transferID, chunkFrom: transfer.ackFirstChunk, chunkTo: frame.chunkID, receivedBytes: BigInt(transfer.receivedBytes) }));
    transfer.ackPending = 0;
  }
  if (flushWindow) {
    transfer.windowPending = 0;
  }
  if (flushWindow && !last) {
    const windowChunks = config?.protocol.maxInFlightChunks ?? 16;
    const remaining = transfer.totalSize - transfer.receivedBytes;
    sendRaw(p.encodeWindow({ transferID: frame.transferID, windowBytes: BigInt(Math.min(remaining, windowChunks * transfer.chunkSize)), windowChunks, flags: p.WindowFlag.Transfer }));
  }
}

async function handleIncomingTransferEnd(frame: p.Frame): Promise<void> {
  const transfer = incomingTransfers.get(frame.transferID);
  if (!transfer) {
    const completed = completedIncoming.get(frame.transferID);
    if (completed) { sendRaw(p.encodeTransferState({ transferID: frame.transferID, receivedBytes: completed.receivedBytes, nextChunk: completed.nextChunk, flags: p.TransferStateFlag.Completed, reasonCode: 0 })); return; }
    throw new p.ProtocolError("transfer end for unknown transfer");
  }
  incomingTransfers.delete(frame.transferID);
  if (transfer.receivedBytes !== transfer.totalSize || transfer.nextChunk !== transfer.chunkCount) {
    emitIncomingProgress(transfer, "failed");
    sendRaw(p.encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: p.TransferStateFlag.Failed, reasonCode: 6 }));
    return;
  }
  const blob = new Blob(transfer.chunks.map((chunk) => chunk as unknown as BlobPart));
  if (transfer.flags & p.TransferFlag.ChecksumSHA256) {
    const actual = new Uint8Array(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()));
    if (!equalBytes(actual, transfer.checksum)) {
      sendRaw(p.encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: p.TransferStateFlag.Failed, reasonCode: 2 }));
      post({ type: "protocol", event: { code: "checksum_mismatch", message: "incoming transfer checksum mismatch", frameType: p.FrameType.TransferEnd, transferID: frame.transferID } });
      emitIncomingProgress(transfer, "failed");
      return;
    }
  }
  const data = await materializeIncoming(transfer, blob);
  if (pending.has(transfer.requestID)) resolveResponse(transfer.requestID, data);
  else post({ type: "event", event: transfer.event || transfer.field || "", data, requestID: transfer.requestID || undefined });
  completedIncoming.set(frame.transferID, { receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk });
  if (completedIncoming.size > 256) completedIncoming.delete(completedIncoming.keys().next().value!);
  emitIncomingProgress(transfer, "completed");
  sendRaw(p.encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: p.TransferStateFlag.Completed, reasonCode: 0 }));
}

async function materializeIncoming(transfer: IncomingTransfer, blob: Blob): Promise<unknown> {
  if (!transfer.parts.length && !transfer.fields.length) {
    if (transfer.event) { try { return JSON.parse(await blob.text()); } catch { /* binary body */ } }
    return blob;
  }
  const result: Record<string, unknown> = {};
  for (const field of transfer.fields) {
    let value: unknown = field.value;
    try { value = JSON.parse(field.value); } catch { /* string field */ }
    result[field.key] = value;
  }
  let offset = 0;
  for (const part of transfer.parts) {
    const value = blob.slice(offset, offset + Number(part.totalSize)); offset += Number(part.totalSize);
    const current = result[part.field];
    if (part.index === 0 && current === undefined) result[part.field] = value;
    else { const list = Array.isArray(current) ? current : current === undefined ? [] : [current]; list[part.index] = value; result[part.field] = list; }
  }
  if (!transfer.parts.length && blob.size) {
    try { result.body = JSON.parse(await blob.text()); } catch { result.body = blob; }
  }
  return result;
}

function materializeEventMessage(message: p.EventMessage): unknown {
  if (!message.fields?.length) return message.data;
  const result: Record<string, unknown> = message.data && typeof message.data === "object" && !Array.isArray(message.data) ? { ...(message.data as Record<string, unknown>) } : { body: message.data };
  for (const field of message.fields) { try { result[field.key] = JSON.parse(field.value); } catch { result[field.key] = field.value; } }
  return result;
}

function handleIncomingTransferResume(frame: p.Frame): void {
  const resume = p.decodeTransferResume(frame), transfer = incomingTransfers.get(resume.transferID);
  const expectedToken = config?.protocol.resumeToken ?? new Uint8Array();
  const accepted = Boolean(transfer && equalBytes(resume.token, expectedToken) && transfer.receivedBytes === Number(resume.receivedBytes) && transfer.nextChunk === resume.nextChunk);
  sendRaw(p.encodeTransferState({ transferID: resume.transferID, receivedBytes: BigInt(transfer?.receivedBytes ?? 0), nextChunk: transfer?.nextChunk ?? 0, flags: accepted ? p.TransferStateFlag.ResumeAccepted : p.TransferStateFlag.ResumeRejected, reasonCode: accepted ? 0 : 3 }));
  if (accepted && transfer) { const chunks = config?.protocol.maxInFlightChunks ?? 16; sendRaw(p.encodeWindow({ transferID: resume.transferID, windowBytes: BigInt(Math.min(transfer.totalSize - transfer.receivedBytes, chunks * transfer.chunkSize)), windowChunks: chunks, flags: p.WindowFlag.Transfer })); }
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean { return left.length === right.length && left.every((value, index) => value === right[index]); }

function emitIncomingProgress(transfer: IncomingTransfer, state: TransferProgress["state"]): void {
  queueProgress({ transferID: transfer.transferID, totalBytes: transfer.totalSize, sentBytes: 0, acknowledgedBytes: 0, receivedBytes: transfer.receivedBytes, direction: "receive", state });
}

function failProtocol(current: WebSocket, error: unknown): void {
  protocolFailure = true;
  desiredConnection = false;
  emitError("protocol", error instanceof Error ? error.message : "invalid ETP frame");
  current.close();
}

function resolveResponse(requestID: bigint, data: unknown): void {
  const request = pending.get(requestID);
  if (!request) {
    return;
  }
  if (request.timeout !== undefined) clearTimeout(request.timeout);
  pending.delete(requestID);
  postResponse(request.callID, data);
}

function rejectResponse(requestID: bigint, code: SocketErrorCode, message: string): void {
  const request = pending.get(requestID);
  if (!request) {
    emitError(code, message);
    return;
  }
  if (request.timeout !== undefined) clearTimeout(request.timeout);
  pending.delete(requestID);
  postResponse(request.callID, undefined, { code, message });
}

function handleClose(current: WebSocket): void {
  if (socket !== current) {
    return;
  }
  clearWriteScheduler();
  clearProgressScheduler();
  socket = undefined;
  const preserveOutgoing = desiredConnection && Boolean(remoteHello?.capabilities && (remoteHello.capabilities & p.Capability.TransferResume));
  resumePending = preserveOutgoing && outgoingTransfers.size > 0;
  if (preserveOutgoing) {
    for (const transfer of outgoingTransfers.values()) {
      for (const sent of transfer.inFlight.values()) sent.queued = true;
      if (transfer.ended) transfer.commitQueued = true;
    }
  }
  remoteHello = undefined;
  resetOutboundRateLimits();
  if (!preserveOutgoing) outgoingTransfers.clear();
  if (!preserveOutgoing) incomingTransfers.clear();
  stopHeartbeat();
  clearHandshakeTimer();
  phase = "closed";
  if (!preserveOutgoing) rejectPending("closed", "connection closed before a response was received");
  const reason: DisconnectReason = terminated ? "terminated" : unauthorized ? "auth" : protocolFailure ? "protocol" : desiredConnection ? "connection" : "client";
  post({ type: "disconnect", reason });
  if (terminated) {
    return;
  }
  post({ type: "status", state: SocketState.Closed });
  if (desiredConnection && !unauthorized) {
    scheduleReconnect();
  }
}

function closeSocket(reason: DisconnectReason): void {
  if (!socket) {
    post({ type: "status", state: SocketState.Closed });
    post({ type: "disconnect", reason });
    return;
  }
  if (phase === "open" && remoteHello?.capabilities && (remoteHello.capabilities & p.Capability.GracefulClose)) {
    phase = "closing";
    sendRaw(p.encodeClose({ reasonCode: reason === "client" ? 4 : 0, flags: p.CloseFlag.Immediate, drainTimeoutMillis: 0 }));
    setTimeout(() => socket?.close(), config?.timeout ?? 10_000);
    return;
  }
  socket.close();
}

function handlePeerClose(current: WebSocket, close: p.CloseMessage): void {
  if (!(close.flags & p.CloseFlag.Drain)) {
    phase = "closing";
    sendRaw(p.encodeClose(close, true));
    current.close();
    return;
  }
  if (!(remoteHello?.capabilities && (remoteHello.capabilities & p.Capability.GracefulClose))) throw new p.ProtocolError("graceful close was not negotiated");
  phase = "draining";
  const deadline = Date.now() + (close.drainTimeoutMillis || 5_000);
  const finish = () => {
    if ((outgoingTransfers.size || incomingTransfers.size) && Date.now() < deadline) {
      setTimeout(finish, 10);
      return;
    }
    phase = "closing";
    sendRaw(p.encodeClose({ ...close, reasonCode: outgoingTransfers.size || incomingTransfers.size ? 5 : close.reasonCode }, true));
    current.close();
  };
  finish();
}

function scheduleReconnect(): void {
  if (!config || !desiredConnection || unauthorized || reconnectTimer || !config.reconnection.enabled) {
    return;
  }
  if (reconnectAttempt >= config.reconnection.attempts) {
    desiredConnection = false;
    emitError("connection", "reconnection attempts exhausted");
    return;
  }
  const delay = Math.min(config.reconnection.delay * 2 ** reconnectAttempt, config.reconnection.maxDelay);
  reconnectAttempt += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = undefined;
    requestAuth(true);
  }, delay);
}

function clearReconnectTimer(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  }
}

function startHeartbeat(): void {
  stopHeartbeat();
  heartbeat = setInterval(() => {
    // A suspended browser can run this timer before its queued close event.
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    monitorOutgoingTransfers();
    if ((phase === "open" || phase === "draining") && Date.now() - lastReadAt > (config?.protocol.heartbeatTimeout ?? 20_000)) {
      emitError("timeout", "ETP heartbeat timed out");
      socket?.close();
      return;
    }
    if (isOpen() && Date.now() - lastWriteAt >= (config?.protocol.heartbeatInterval ?? 10_000)) {
      sendRaw(p.encodePing());
    }
  }, 1_000);
}

function monitorOutgoingTransfers(): void {
  const now = Date.now(), ackTimeout = config?.protocol.ackTimeout ?? 2_000, retryLimit = config?.protocol.retryLimit ?? 3;
  for (const transfer of outgoingTransfers.values()) {
    let failed = false;
    const total = Math.ceil(transfer.bytes.length / transfer.chunkSize);
    for (const [chunkID, sent] of transfer.inFlight) {
      if (sent.queued || sent.sentAt === 0) continue;
      if (now - sent.sentAt < ackTimeout) continue;
      if (sent.retries >= retryLimit) { failed = true; break; }
      sent.queued = true;
      sent.retries += 1;
      sendRaw(p.encodeData(transfer.transferID, transfer.requestID, chunkID, sent.payload, chunkID === 0, chunkID + 1 === total));
    }
    if (!failed && transfer.ended && !transfer.commitQueued && transfer.commitSentAt > 0 && now - transfer.commitSentAt >= ackTimeout) {
      if (transfer.commitRetries >= retryLimit) failed = true;
      else { transfer.commitQueued = true; transfer.commitRetries += 1; sendRaw(p.encodeTransferEnd(transfer.transferID, transfer.requestID)); }
    }
    if (!failed) continue;
    outgoingTransfers.delete(transfer.transferID);
    emitProgress(transfer, "failed");
    rejectResponse(transfer.requestID, "timeout", "transfer acknowledgment timed out");
  }
}

function startHandshakeTimer(): void {
  clearHandshakeTimer();
  handshakeTimer = setTimeout(() => {
    emitError("timeout", "ETP authentication or handshake timed out");
    socket?.close();
  }, config?.timeout ?? 10_000);
}

function clearHandshakeTimer(): void {
  if (handshakeTimer) {
    clearTimeout(handshakeTimer);
    handshakeTimer = undefined;
  }
}

function stopHeartbeat(): void {
  if (heartbeat) {
    clearInterval(heartbeat);
    heartbeat = undefined;
  }
}

function isOpen(): boolean {
  return phase === "open" && socket?.readyState === WebSocket.OPEN && heartbeat !== undefined;
}

function requirePhase(expected: typeof phase, message: string): void {
  if (phase !== expected) {
    throw new p.ProtocolError(message);
  }
}

function requireTransferPhase(message: string): void {
  if (phase !== "open" && phase !== "draining") throw new p.ProtocolError(message);
}

function enforceFrameCapability(frameType: number, transferID: bigint): void {
  if (phase === "auth" || phase === "hello" || !remoteHello) return;
  let capability = 0n;
  if (frameType === p.FrameType.Ack) capability = p.Capability.Ack;
  else if (frameType === p.FrameType.Nack) capability = p.Capability.Nack;
  else if (frameType === p.FrameType.Ping || frameType === p.FrameType.Pong) capability = p.Capability.Heartbeat;
  else if (frameType === p.FrameType.Window) capability = p.Capability.FlowControl;
  else if (frameType === p.FrameType.Cancel || frameType === p.FrameType.CancelAck) capability = p.Capability.Cancel;
  else if (frameType === p.FrameType.Request || frameType === p.FrameType.Response) capability = p.Capability.RequestResponse;
  else if (frameType === p.FrameType.TransferResume) capability = p.Capability.TransferResume;
  else if (frameType === p.FrameType.TransferBegin || frameType === p.FrameType.TransferEnd || frameType === p.FrameType.TransferState || (frameType === p.FrameType.Data && transferID !== 0n)) capability = p.Capability.Transfers;
  if (capability && (!(BrowserCapabilities & capability) || !(remoteHello.capabilities & capability))) throw new p.ProtocolError(`ETP capability was not negotiated for frame type ${frameType}`);
}

function enforceIncomingRate(frame: p.Frame, bytes: number): void {
  if (!config || !incomingRequestTokens || !incomingFrameTokens || !incomingByteTokens) return;
  const now = Date.now(), options = config.protocol;
  refillTokens(incomingRequestTokens, now, options.maxRequestsPerSecond, options.maxRequestsPerSecond);
  refillTokens(incomingFrameTokens, now, options.maxFramesPerSecond, options.maxFramesPerSecond);
  refillTokens(incomingByteTokens, now, options.maxBytesPerSecond, options.maxBytesPerSecond);
  const isRequest = frame.type === p.FrameType.Request || (frame.type === p.FrameType.TransferBegin && frame.requestID !== 0n);
  if (isRequest && tokenWait(incomingRequestTokens, 1, options.maxRequestsPerSecond) > 0) throw new p.ProtocolError("incoming ETP rate limit exceeded: requests");
  if (tokenWait(incomingFrameTokens, 1, options.maxFramesPerSecond) > 0) throw new p.ProtocolError("incoming ETP rate limit exceeded: frames");
  if (tokenWait(incomingByteTokens, bytes, options.maxBytesPerSecond) > 0) throw new p.ProtocolError("incoming ETP rate limit exceeded: bytes");
  if (isRequest) incomingRequestTokens.tokens -= 1;
  incomingFrameTokens.tokens -= 1;
  incomingByteTokens.tokens -= bytes;
}

function resetIncomingRateLimits(): void {
  if (!config) { incomingRequestTokens = incomingFrameTokens = incomingByteTokens = undefined; return; }
  const now = Date.now(), options = config.protocol;
  incomingRequestTokens = { tokens: options.maxRequestsPerSecond, updatedAt: now };
  incomingFrameTokens = { tokens: options.maxFramesPerSecond, updatedAt: now };
  incomingByteTokens = { tokens: options.maxBytesPerSecond, updatedAt: now };
}

function sendRaw(frame: ArrayBuffer): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    throw new p.ProtocolError("WebSocket is not open");
  }
  if (queuedWriteFrames >= MaxQueuedWriteFrames || queuedWriteBytes + frame.byteLength > MaxQueuedWriteBytes) {
    throw new p.ProtocolError("outgoing ETP write queue limit exceeded");
  }
  const queue = writeQueueFor(frame);
  queue.frames.push(frame);
  queuedWriteFrames += 1;
  queuedWriteBytes += frame.byteLength;
  if (queue !== bulkWrites) {
    const delay = drainPriorityWrites();
    if (delay > 0) scheduleWriteDrain(delay);
  } else {
    scheduleWriteDrain(0);
  }
}

function writeQueueFor(frame: ArrayBuffer): WriteQueue {
  const view = new DataView(frame);
  const flags = view.getUint16(2, false), priority = view.getUint8(4), channel = view.getUint16(6, false);
  if (flags & p.FrameFlag.Control || channel === p.Channel.Control) return controlWrites;
  if (channel === p.Channel.Realtime && priority <= p.Priority.Normal) return realtimeWrites;
  return bulkWrites;
}

function drainPriorityWrites(): number {
  if (!socket || socket.readyState !== WebSocket.OPEN) return 0;
  const controlDelay = drainRateLimitedQueue(controlWrites);
  if (controlDelay > 0) return controlDelay;
  return drainRateLimitedQueue(realtimeWrites);
}

function drainWrites(): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    clearWriteScheduler();
    return;
  }
  try {
    const priorityDelay = drainPriorityWrites();
    if (priorityDelay > 0) {
      scheduleWriteDrain(priorityDelay);
      return;
    }
    const creditWake = writeCreditWake;
    writeCreditWake = false;
    if (!creditWake && socket.bufferedAmount >= writeBufferLimit()) {
      scheduleWriteDrain(1);
      return;
    }
    let frame: ArrayBuffer | undefined, sent = 0;
    while (sent < MaxBulkBurstFrames && (frame = peekWrite(bulkWrites))) {
      if (shouldDropWrite(frame)) { dequeueWrite(bulkWrites); continue; }
      const delay = reserveOutboundRate(frame);
      if (delay > 0) { scheduleWriteDrain(delay); return; }
      dequeueWrite(bulkWrites);
      writeFrame(frame);
      sent += 1;
    }
    if (queuedWriteFrames > 0) scheduleWriteDrain(socket.bufferedAmount >= writeBufferLimit() ? 1 : 0);
  } catch (error) {
    clearWriteScheduler();
    emitError("connection", error instanceof Error ? error.message : "WebSocket transport write failed");
    socket?.close();
  }
}

function writeFrame(frame: ArrayBuffer): void {
  if (!socket || socket.readyState !== WebSocket.OPEN || shouldDropWrite(frame)) return;
  socket.send(frame);
  const now = Date.now();
  lastWriteAt = now;
  const view = new DataView(frame), type = view.getUint8(1), transferID = view.getBigUint64(28, false);
  if ((type === p.FrameType.Request || type === p.FrameType.TransferBegin) && view.getBigUint64(20, false) !== 0n) armPendingTimeout(view.getBigUint64(20, false));
  if (type === p.FrameType.Data && transferID !== 0n) {
    const sent = outgoingTransfers.get(transferID)?.inFlight.get(view.getUint32(36, false));
    if (sent) { sent.queued = false; sent.sentAt = now; }
  } else if (type === p.FrameType.TransferEnd) {
    const transfer = outgoingTransfers.get(transferID);
    if (transfer) { transfer.commitQueued = false; transfer.commitSentAt = now; }
  }
}

function drainRateLimitedQueue(queue: WriteQueue): number {
  let frame: ArrayBuffer | undefined;
  while ((frame = peekWrite(queue))) {
    if (shouldDropWrite(frame)) { dequeueWrite(queue); continue; }
    const delay = reserveOutboundRate(frame);
    if (delay > 0) return delay;
    dequeueWrite(queue);
    writeFrame(frame);
  }
  return 0;
}

function peekWrite(queue: WriteQueue): ArrayBuffer | undefined {
  return queue.head < queue.frames.length ? queue.frames[queue.head] : undefined;
}

function resetOutboundRateLimits(): void {
  if (!remoteHello) { requestTokens = frameTokens = byteTokens = undefined; return; }
  const now = Date.now(), limits = remoteHello.rateLimits;
  requestTokens = { tokens: limits.availableRequests, updatedAt: now };
  frameTokens = { tokens: limits.availableFrames, updatedAt: now };
  const byteReserve = Math.min(Number(limits.byteBurst), OutboundByteReserveChunks * (remoteHello.maxChunkSize + p.HeaderSize));
  byteTokens = { tokens: Math.max(0, Number(limits.availableBytes) - byteReserve), updatedAt: now };
}

function reserveOutboundRate(frame: ArrayBuffer): number {
  if (phase !== "open" && phase !== "draining") return 0;
  if (!remoteHello || !requestTokens || !frameTokens || !byteTokens) return 0;
  const now = Date.now(), limits = remoteHello.rateLimits, view = new DataView(frame);
  refillTokens(requestTokens, now, limits.maxRequestsPerSecond, limits.requestBurst);
  refillTokens(frameTokens, now, limits.maxFramesPerSecond, limits.frameBurst);
  const safeByteRate = Number(limits.maxBytesPerSecond) * OutboundByteRateSafety;
  refillTokens(byteTokens, now, safeByteRate, Number(limits.byteBurst));
  const isRequest = view.getUint8(1) === p.FrameType.Request || (view.getUint8(1) === p.FrameType.TransferBegin && view.getBigUint64(20, false) !== 0n);
  let wait = Math.max(tokenWait(frameTokens, 1, limits.maxFramesPerSecond), tokenWait(byteTokens, frame.byteLength, safeByteRate));
  if (isRequest) wait = Math.max(wait, tokenWait(requestTokens, 1, limits.maxRequestsPerSecond));
  if (wait > 0) return Math.max(1, Math.ceil(wait));
  frameTokens.tokens -= 1;
  byteTokens.tokens -= frame.byteLength;
  if (isRequest) requestTokens.tokens -= 1;
  return 0;
}

function refillTokens(state: TokenState, now: number, rate: number, burst: number): void {
  if (now <= state.updatedAt) return;
  state.tokens = Math.min(burst, state.tokens + (now - state.updatedAt) * rate / 1_000);
  state.updatedAt = now;
}

function tokenWait(state: TokenState, cost: number, rate: number): number {
  if (rate <= 0) return 0;
  return state.tokens >= cost ? 0 : (cost - state.tokens) * 1_000 / rate;
}

function armPendingTimeout(requestID: bigint): void {
  const request = pending.get(requestID);
  if (!request || request.timeout !== undefined) return;
  request.timeout = setTimeout(() => {
    if (pending.delete(requestID)) {
      cancelTransferForRequest(requestID, 2);
      postResponse(request.callID, undefined, { code: "timeout", message: "request timed out" });
    }
  }, config?.timeout ?? 10_000);
}

function clearPendingTimeout(requestID: bigint): void {
  const timeout = pending.get(requestID)?.timeout;
  if (timeout !== undefined) clearTimeout(timeout);
}

function shouldDropWrite(frame: ArrayBuffer): boolean {
  const view = new DataView(frame), type = view.getUint8(1);
  if (type !== p.FrameType.TransferBegin && type !== p.FrameType.Data && type !== p.FrameType.TransferEnd) return false;
  return !outgoingTransfers.has(view.getBigUint64(28, false));
}

function dequeueWrite(queue: WriteQueue): ArrayBuffer | undefined {
  if (queue.head >= queue.frames.length) return undefined;
  const frame = queue.frames[queue.head++];
  queuedWriteFrames -= 1;
  queuedWriteBytes -= frame.byteLength;
  if (queue.head === queue.frames.length) { queue.frames.length = 0; queue.head = 0; }
  return frame;
}

function scheduleWriteDrain(delay: number): void {
  if (writeScheduled) {
    if (delay !== 0 || writeTimer === undefined) return;
    clearTimeout(writeTimer);
    writeTimer = undefined;
    writeScheduled = false;
  }
  writeScheduled = true;
  if (delay === 0) {
    writeChannel.port2.postMessage(0);
    return;
  }
  writeTimer = setTimeout(() => {
    writeTimer = undefined;
    writeScheduled = false;
    drainWrites();
  }, delay);
}

function writeBufferLimit(): number {
  const chunkSize = config?.protocol.chunkSize ?? p.DefaultChunkSize;
  return Math.max(16 << 10, chunkSize * Math.max(MaxBulkBurstFrames, config?.protocol.maxInFlightChunks ?? 16));
}

function clearWriteScheduler(): void {
  if (writeTimer !== undefined) clearTimeout(writeTimer);
  writeTimer = undefined;
  writeScheduled = false;
  writeCreditWake = false;
  for (const queue of [controlWrites, realtimeWrites, bulkWrites]) { queue.frames.length = 0; queue.head = 0; }
  queuedWriteFrames = 0;
  queuedWriteBytes = 0;
}

function queueProgress(progress: TransferProgress): void {
  const terminal = progress.state !== "sending" && progress.state !== "receiving";
  if (terminal) {
    pendingProgress.delete(progress.transferID);
    progressPublishedAt.delete(progress.transferID);
    if (pendingProgress.size === 0 && progressTimer !== undefined) {
      clearTimeout(progressTimer);
      progressTimer = undefined;
    }
    post({ type: "progress", progress });
    return;
  }
  const now = Date.now();
  const publishedAt = progressPublishedAt.get(progress.transferID);
  if (publishedAt === undefined || now - publishedAt >= ProgressIntervalMillis) {
    pendingProgress.delete(progress.transferID);
    progressPublishedAt.set(progress.transferID, now);
    post({ type: "progress", progress });
    return;
  }
  pendingProgress.set(progress.transferID, progress);
  if (progressTimer === undefined) progressTimer = setTimeout(flushProgress, ProgressIntervalMillis);
}

function flushProgress(): void {
  progressTimer = undefined;
  const now = Date.now();
  for (const progress of pendingProgress.values()) {
    progressPublishedAt.set(progress.transferID, now);
    post({ type: "progress", progress });
  }
  pendingProgress.clear();
}

function clearProgressScheduler(): void {
  if (progressTimer !== undefined) clearTimeout(progressTimer);
  progressTimer = undefined;
  pendingProgress.clear();
  progressPublishedAt.clear();
}

function rejectPending(code: SocketErrorCode, message: string): void {
  for (const request of pending.values()) {
    if (request.timeout !== undefined) clearTimeout(request.timeout);
    postResponse(request.callID, undefined, { code, message });
  }
  pending.clear();
}

function emitError(code: SocketErrorCode, message: string): void {
  post({ type: "error", error: { code, message } });
}

function postResponse(callID: number, data?: unknown, error?: { code: SocketErrorCode; message: string }): void {
  post({ type: "response", callID, data, error });
}

function post(message: WorkerMessage): void {
  scope.postMessage(message);
}
