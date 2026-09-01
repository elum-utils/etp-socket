import {
  Capability,
  AllCapabilities,
  CloseFlag,
  DefaultChunkSize,
  type Hello,
  decodeAck,
  decodeAuthAccept,
  decodeAuthReject,
  decodeCancel,
  decodeCancelAck,
  decodeClose,
  decodeEvent,
  decodeError,
  decodeFrame,
  decodeGoAway,
  decodeHello,
  decodeNack,
  decodeText,
  decodeTransferState,
  decodeTransferResume,
  decodeTransferBegin,
  decodeWindow,
  encodeAck,
  encodeAuth,
  encodeCancelAck,
  encodeCancel,
  encodeData,
  encodeNack,
  encodeClose,
  encodeHello,
  encodePing,
  encodePong,
  encodeRequest,
  encodeResponse,
  encodeTransferBegin,
  encodeTransferEnd,
  encodeTransferResume,
  encodeTransferState,
  encodeWindow,
  TransferStateFlag,
  TransferFlag,
  WindowFlag,
  FrameType,
  ProtocolError,
} from "./protocol";
import { SocketState, type DisconnectReason, type ProtocolEvent, type ReconnectionOptions, type SocketErrorCode, type TransferProgress } from "./types";

type WorkerConfig = {
  url: string;
  timeout: number;
  reconnection: Required<ReconnectionOptions>;
  protocol: {
    chunkSize: number; maxTransferBytes: number; maxConcurrentTransfers: number; maxInFlightChunks: number;
    heartbeatInterval: number; heartbeatTimeout: number; ackTimeout: number; retryLimit: number;
    maxFramesPerSecond: number; maxBytesPerSecond: number; checksum: boolean; resumeToken: Uint8Array;
  };
};

type Pending = {
  callID: number;
  timeout: ReturnType<typeof setTimeout>;
};

type OutgoingTransfer = {
  callID: number;
  requestID: bigint;
  transferID: bigint;
  bytes: Uint8Array;
  chunkSize: number;
  nextChunk: number;
  nextOffset: number;
  inFlight: Map<number, { payload: Uint8Array; sentAt: number; retries: number }>;
  windowBytes: bigint;
  windowChunks: number;
  ended: boolean;
  acknowledgedBytes: number;
  commitSentAt: number;
  commitRetries: number;
};

type IncomingTransfer = {
  requestID: bigint;
  transferID: bigint;
  totalSize: number;
  chunkSize: number;
  chunkCount: number;
  event: string;
  field: string;
  fields: Array<{ key: string; value: string }>;
  parts: Array<{ field: string; index: number; name: string; totalSize: bigint; contentType: number }>;
  contentType: number;
  flags: number;
  checksum: Uint8Array;
  chunks: Uint8Array[];
  receivedBytes: number;
  nextChunk: number;
};

type MultipartPayload = { __etpMultipart: true; fields: Array<{ key: string; value: string }>; parts: Array<{ field: string; index: number; name: string; blob: Blob }> };

type MainMessage =
  | { type: "configure"; config: WorkerConfig }
  | { type: "connect" }
  | { type: "disconnect" }
  | { type: "terminate" }
  | { type: "auth"; epoch: number; token?: string; error?: string }
  | { type: "emit"; callID: number; event: string; data: unknown }
  | { type: "respond"; requestID: bigint; event: string; data: unknown }
  | { type: "cancel"; callID: number };

type WorkerMessage =
  | { type: "auth"; epoch: number }
  | { type: "status"; state: SocketState }
  | { type: "disconnect"; reason: DisconnectReason }
  | { type: "event"; event: string; data: unknown; requestID?: bigint }
  | { type: "text"; text: string }
  | { type: "identity"; identity: { userID: string } }
  | { type: "response"; callID: number; data?: unknown; error?: { code: SocketErrorCode; message: string } }
  | { type: "error"; error: { code: SocketErrorCode; message: string } }
  | { type: "progress"; progress: TransferProgress }
  | { type: "protocol"; event: ProtocolEvent };

type WorkerScope = {
  onmessage: ((event: MessageEvent<MainMessage>) => void) | null;
  postMessage(message: WorkerMessage): void;
  close(): void;
};

const scope = self as unknown as WorkerScope;
const BrowserCapabilities = Capability.Transfers | Capability.Cancel | Capability.Ack | Capability.Nack | Capability.Heartbeat | Capability.TransferSHA256 | Capability.FlowControl | Capability.RequestResponse | Capability.GracefulClose | Capability.TransferResume | Capability.TransferCommit;

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
let remoteHello: Hello | undefined;
let resumePending = false;
let rateSecond = 0;
let rateFrames = 0;
let rateBytes = 0;

scope.onmessage = ({ data }: MessageEvent<MainMessage>) => {
  switch (data.type) {
    case "configure":
      config = data.config;
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
      if (data.error || !data.token) {
        emitError("auth", data.error ?? "authentication token is empty");
        scheduleReconnect();
        return;
      }
      openSocket(data.token);
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
  const current = new WebSocket(config.url);
  socket = current;
  current.binaryType = "arraybuffer";
  current.onopen = () => {
    if (socket !== current) {
      return;
    }
    phase = "auth";
    post({ type: "status", state: SocketState.Authenticating });
    sendRaw(encodeAuth(token));
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
    enforceIncomingRate(data.byteLength);
    const frame = decodeFrame(data);
    lastReadAt = Date.now();
    enforceFrameCapability(frame.type, frame.transferID);
    switch (frame.type) {
      case FrameType.AuthAccept:
        requirePhase("auth", "unexpected auth acceptance");
        post({ type: "identity", identity: { userID: decodeAuthAccept(frame) } });
        phase = "hello";
        sendRaw(encodeHello({ capabilities: BrowserCapabilities, maxChunkSize: config!.protocol.chunkSize, maxTransferBytes: BigInt(config!.protocol.maxTransferBytes), maxInFlightChunks: config!.protocol.maxInFlightChunks, heartbeatMillis: config!.protocol.heartbeatInterval }));
        return;
      case FrameType.AuthReject:
        requirePhase("auth", "unexpected auth rejection");
        unauthorized = true;
        desiredConnection = false;
        post({ type: "status", state: SocketState.Unauthorized });
        emitError("auth", decodeAuthReject(frame) || "authentication rejected");
        current.close();
        return;
      case FrameType.HelloAck:
        requirePhase("hello", "unexpected hello acknowledgment");
        remoteHello = decodeHello(frame);
        if (remoteHello.role !== "server") {
          throw new ProtocolError("unexpected ETP hello role");
        }
        if (remoteHello.capabilities & ~AllCapabilities) {
          throw new ProtocolError("remote advertised unknown ETP capabilities");
        }
        clearHandshakeTimer();
        phase = "open";
        reconnectAttempt = 0;
        lastWriteAt = Date.now();
        lastReadAt = Date.now();
        post({ type: "status", state: SocketState.Open });
        startHeartbeat();
        resumeOutgoingTransfers();
        return;
      case FrameType.Ping:
        requireTransferPhase("ping before handshake completion");
        sendRaw(encodePong());
        return;
      case FrameType.Pong:
        requireTransferPhase("pong before handshake completion");
        return;
      case FrameType.Response:
        requirePhase("open", "response before handshake completion");
        resolveResponse(frame.requestID, materializeEventMessage(decodeEvent(frame)));
        return;
      case FrameType.Request: {
        requirePhase("open", "request before handshake completion");
        const event = decodeEvent(frame);
        post({ type: "event", event: event.event, data: materializeEventMessage(event), requestID: frame.requestID });
        return;
      }
      case FrameType.Error:
        requireTransferPhase("error before handshake completion");
        const remoteError = decodeError(frame);
        post({ type: "protocol", event: { code: `remote_error_${remoteError.code}`, message: remoteError.message, frameType: remoteError.frameType, transferID: remoteError.transferID } });
        rejectResponse(remoteError.requestID || frame.requestID, remoteError.code === 2 ? "auth" : "protocol", remoteError.message || "server returned an ETP error");
        if (remoteError.code === 1 || remoteError.code === 2) throw new ProtocolError(`fatal remote ETP error ${remoteError.code}`);
        return;
      case FrameType.Ack:
        requireTransferPhase("ack before handshake completion");
        handleTransferAck(decodeAck(frame));
        return;
      case FrameType.Nack:
        requireTransferPhase("nack before handshake completion");
        handleTransferNack(decodeNack(frame));
        return;
      case FrameType.Window:
        requireTransferPhase("window before handshake completion");
        handleTransferWindow(decodeWindow(frame));
        return;
      case FrameType.TransferState:
        requireTransferPhase("transfer state before handshake completion");
        handleTransferState(decodeTransferState(frame));
        return;
      case FrameType.TransferBegin:
        requirePhase("open", "transfer begin before handshake completion");
        handleIncomingTransferBegin(frame);
        return;
      case FrameType.Data:
        requireTransferPhase("transfer data before handshake completion");
        if (frame.transferID === 0n) {
          post({ type: "text", text: decodeText(frame) });
          return;
        }
        handleIncomingTransferData(frame);
        return;
      case FrameType.TransferEnd:
        requireTransferPhase("transfer end before handshake completion");
        void handleIncomingTransferEnd(frame).catch((error) => failProtocol(current, error));
        return;
      case FrameType.TransferResume:
        requirePhase("open", "transfer resume before handshake completion");
        handleIncomingTransferResume(frame);
        return;
      case FrameType.Cancel: {
        requireTransferPhase("cancel before handshake completion");
        const cancel = decodeCancel(frame);
        const outgoing = outgoingTransfers.get(cancel.transferID), incoming = incomingTransfers.get(cancel.transferID), completed = completedIncoming.has(cancel.transferID);
        if (outgoing) { outgoingTransfers.delete(cancel.transferID); emitProgress(outgoing, "canceled"); rejectResponse(outgoing.requestID, "closed", "transfer was canceled by the server"); }
        if (incoming) { incomingTransfers.delete(cancel.transferID); emitIncomingProgress(incoming, "canceled"); }
        sendRaw(encodeCancelAck(cancel.transferID, completed ? 3 : outgoing || incoming ? 1 : 2));
        return;
      }
      case FrameType.CancelAck: {
        requireTransferPhase("cancel acknowledgment before handshake completion");
        const ack = decodeCancelAck(frame);
        post({ type: "protocol", event: { code: "cancel_ack", message: `transfer cancellation status ${ack.status}`, frameType: frame.type, transferID: ack.transferID } });
        return;
      }
      case FrameType.Close:
        if (phase !== "open" && phase !== "draining" && phase !== "closing") throw new ProtocolError("close before handshake completion");
        handlePeerClose(current, decodeClose(frame));
        return;
      case FrameType.GoAway:
        requirePhase("open", "goaway before handshake completion");
        const goAway = decodeGoAway(frame);
        if (goAway.flags & CloseFlag.Drain) {
          if (!(remoteHello?.capabilities && (remoteHello.capabilities & Capability.GracefulClose))) throw new ProtocolError("graceful close was not negotiated");
          phase = "draining";
        } else {
          phase = "closing";
          current.close();
        }
        return;
      case FrameType.CloseAck:
        current.close();
        return;
      default:
        throw new ProtocolError(`unsupported ETP frame type ${frame.type}`);
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
  const timeout = setTimeout(() => {
    if (pending.delete(requestID)) {
      cancelTransferForRequest(requestID, 2);
      postResponse(callID, undefined, { code: "timeout", message: "request timed out" });
    }
  }, config?.timeout ?? 10_000);
  pending.set(requestID, { callID, timeout });
  try {
    if (isMultipartPayload(data)) {
      const total = data.parts.reduce((size, part) => size + part.blob.size, 0), bytes = new Uint8Array(total);
      let offset = 0;
      for (const part of data.parts) { const chunk = new Uint8Array(await part.blob.arrayBuffer()); bytes.set(chunk, offset); offset += chunk.length; }
      if (!pending.has(requestID)) return;
      await startTransfer(callID, requestID, event, bytes, "", data.parts.map((part) => ({ field: part.field, index: part.index, name: part.name, totalSize: BigInt(part.blob.size), contentType: 1 })), data.fields);
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
    const body = new TextEncoder().encode(JSON.stringify(data) ?? "null");
    const inlineLimit = Math.min(64 << 10, (remoteHello?.maxFrameBytes ?? 8 << 20) - 40);
    if (body.length <= inlineLimit) {
      sendRaw(encodeRequest(requestID, event, data));
    } else {
      await startTransfer(callID, requestID, event, body);
    }
  } catch (error) {
    clearTimeout(timeout);
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
      const total = data.parts.reduce((size, part) => size + part.blob.size, 0), bytes = new Uint8Array(total);
      let offset = 0;
      for (const part of data.parts) { const chunk = new Uint8Array(await part.blob.arrayBuffer()); bytes.set(chunk, offset); offset += chunk.length; }
      await startTransfer(0, requestID, event, bytes, "", data.parts.map((part) => ({ field: part.field, index: part.index, name: part.name, totalSize: BigInt(part.blob.size), contentType: 1 })), data.fields, false);
      return;
    }
    const binary = data instanceof Blob ? new Uint8Array(await data.arrayBuffer()) : data instanceof ArrayBuffer ? new Uint8Array(data) : data instanceof Uint8Array ? data : undefined;
    if (binary) { await startTransfer(0, requestID, event, binary, "", [], [], false); return; }
    const body = new TextEncoder().encode(JSON.stringify(data) ?? "null");
    const inlineLimit = Math.min(64 << 10, (remoteHello?.maxFrameBytes ?? 8 << 20) - 40);
    if (body.length <= inlineLimit) sendRaw(encodeResponse(requestID, event, data));
    else await startTransfer(0, requestID, event, body, "", [], [], false);
  } catch (error) {
    emitError("protocol", error instanceof Error ? error.message : "response encoding failed");
  }
}

async function startTransfer(callID: number, requestID: bigint, event: string, bytes: Uint8Array, name = "", parts: Array<{ field: string; index: number; name: string; totalSize: bigint; contentType: number }> = [], fields: Array<{ key: string; value: string }> = [], requirePending = true): Promise<void> {
  if (!remoteHello || !(remoteHello.capabilities & Capability.Transfers) || !(remoteHello.capabilities & Capability.Ack) || !(remoteHello.capabilities & Capability.TransferCommit)) throw new ProtocolError("remote peer does not support ETP transfers");
  if (BigInt(bytes.length) > remoteHello.maxTransferBytes) throw new ProtocolError("message exceeds remote ETP transfer limit");
  const transferID = ++nextTransferID, chunkSize = Math.min(config?.protocol.chunkSize ?? DefaultChunkSize, remoteHello.maxChunkSize), chunkCount = Math.ceil(bytes.length / chunkSize);
  const checksumEnabled = Boolean(config?.protocol.checksum && remoteHello.capabilities & Capability.TransferSHA256);
  const checksum = checksumEnabled ? new Uint8Array(await crypto.subtle.digest("SHA-256", bytes.slice().buffer)) : new Uint8Array(32);
  if (requirePending && !pending.has(requestID)) return;
  const transfer: OutgoingTransfer = { callID, requestID, transferID, bytes, chunkSize, nextChunk: 0, nextOffset: 0, inFlight: new Map(), windowBytes: 0n, windowChunks: 0, ended: false, acknowledgedBytes: 0, commitSentAt: 0, commitRetries: 0 };
  outgoingTransfers.set(transferID, transfer);
  sendRaw(encodeTransferBegin(transferID, requestID, { totalSize: BigInt(bytes.length), chunkSize, chunkCount, contentType: 1, flags: checksumEnabled ? TransferFlag.ChecksumSHA256 : 0, checksum, name, event, field: "", index: 0, parts, fields }));
  if (!(remoteHello.capabilities & Capability.FlowControl)) {
    transfer.windowBytes = BigInt(bytes.length);
    transfer.windowChunks = remoteHello.maxInFlightChunks;
    pumpTransfer(transfer);
  }
}

function isMultipartPayload(data: unknown): data is MultipartPayload { return Boolean(data && typeof data === "object" && (data as { __etpMultipart?: unknown }).__etpMultipart === true); }

function handleTransferWindow(window: { transferID: bigint; windowBytes: bigint; windowChunks: number; flags: number }): void {
  if (window.flags !== WindowFlag.Transfer) throw new ProtocolError("invalid transfer window");
  const transfer = outgoingTransfers.get(window.transferID);
  if (!transfer) return;
  transfer.windowBytes = window.windowBytes;
  transfer.windowChunks = window.windowChunks;
  pumpTransfer(transfer);
}

function handleTransferAck(ack: { transferID: bigint; chunkFrom: number; chunkTo: number; receivedBytes: bigint }): void {
  const transfer = outgoingTransfers.get(ack.transferID);
  if (!transfer) return;
  let matched = false, acknowledged = transfer.acknowledgedBytes;
  for (let chunk = ack.chunkFrom; chunk <= ack.chunkTo; chunk += 1) {
    const sent = transfer.inFlight.get(chunk);
    if (!sent) continue;
    matched = true; acknowledged += sent.payload.length; transfer.inFlight.delete(chunk);
  }
  if ((!matched && ack.receivedBytes > BigInt(transfer.acknowledgedBytes)) || (matched && ack.receivedBytes !== BigInt(acknowledged))) throw new ProtocolError("ack byte counter does not match committed chunks");
  transfer.acknowledgedBytes = Number(ack.receivedBytes);
  emitProgress(transfer, "sending");
  pumpTransfer(transfer);
}

function handleTransferNack(nack: { transferID: bigint; chunkFrom: number; chunkTo: number; reasonCode: number }): void {
  const transfer = outgoingTransfers.get(nack.transferID);
  if (!transfer) return;
  post({ type: "protocol", event: { code: "nack_received", message: `NACK ${nack.chunkFrom}-${nack.chunkTo}`, frameType: FrameType.Nack, transferID: nack.transferID, chunkID: nack.chunkFrom } });
  if (nack.reasonCode === 7) { transfer.windowBytes = 0n; transfer.windowChunks = 0; return; }
  if (nack.reasonCode !== 1) { outgoingTransfers.delete(nack.transferID); emitProgress(transfer, "failed"); rejectResponse(transfer.requestID, "protocol", `remote rejected transfer with reason ${nack.reasonCode}`); return; }
  const total = Math.ceil(transfer.bytes.length / transfer.chunkSize);
  for (let chunk = nack.chunkFrom; chunk <= nack.chunkTo; chunk += 1) {
    const sent = transfer.inFlight.get(chunk);
    if (sent) { sendRaw(encodeData(transfer.transferID, transfer.requestID, chunk, sent.payload, chunk === 0, chunk + 1 === total)); sent.sentAt = Date.now(); sent.retries += 1; }
  }
}

function handleTransferState(state: { transferID: bigint; receivedBytes: bigint; nextChunk: number; flags: number; reasonCode: number }): void {
  const transfer = outgoingTransfers.get(state.transferID);
  if (!transfer) return;
  if (state.flags & TransferStateFlag.ResumeAccepted) {
    const expected = Math.min(transfer.bytes.length, state.nextChunk * transfer.chunkSize);
    if (state.receivedBytes !== BigInt(expected)) throw new ProtocolError("invalid transfer resume counters");
    transfer.acknowledgedBytes = Number(state.receivedBytes);
    transfer.nextOffset = transfer.acknowledgedBytes;
    transfer.nextChunk = state.nextChunk;
    transfer.inFlight.clear(); transfer.ended = false; transfer.commitRetries = 0;
    emitProgress(transfer, "sending");
    return;
  }
  if (state.flags & TransferStateFlag.Completed) {
    const chunkCount = Math.ceil(transfer.bytes.length / transfer.chunkSize);
    if (state.receivedBytes !== BigInt(transfer.bytes.length) || state.nextChunk !== chunkCount) throw new ProtocolError("invalid transfer completion counters");
  }
  if (state.flags & TransferStateFlag.ResumeRejected) {
    outgoingTransfers.delete(state.transferID);
    emitProgress(transfer, "failed");
    rejectResponse(transfer.requestID, "protocol", `transfer resume rejected with reason ${state.reasonCode}`);
    return;
  }
  outgoingTransfers.delete(state.transferID);
  if (state.flags & TransferStateFlag.Failed) rejectResponse(transfer.requestID, "protocol", `transfer failed with reason ${state.reasonCode}`);
  emitProgress(transfer, state.flags & TransferStateFlag.Failed ? "failed" : "completed");
}

function resumeOutgoingTransfers(): void {
  if (!resumePending) return;
  resumePending = false;
  if (!remoteHello || !(remoteHello.capabilities & Capability.TransferResume)) {
    for (const transfer of outgoingTransfers.values()) rejectResponse(transfer.requestID, "protocol", "remote no longer supports transfer resume");
    outgoingTransfers.clear();
    return;
  }
  for (const transfer of outgoingTransfers.values()) {
    sendRaw(encodeTransferResume(transfer.requestID, { transferID: transfer.transferID, receivedBytes: BigInt(transfer.acknowledgedBytes), nextChunk: Math.ceil(transfer.acknowledgedBytes / transfer.chunkSize), token: config?.protocol.resumeToken ?? new Uint8Array() }));
  }
}

function cancelTransferForRequest(requestID: bigint, reasonCode: number): void {
  for (const transfer of outgoingTransfers.values()) {
    if (transfer.requestID === requestID) {
      emitProgress(transfer, "canceling");
      outgoingTransfers.delete(transfer.transferID);
      if (socket?.readyState === WebSocket.OPEN) sendRaw(encodeCancel(transfer.transferID, reasonCode, 1));
      return;
    }
  }
}

function cancelCall(callID: number): void {
  for (const [requestID, request] of pending) {
    if (request.callID !== callID) continue;
    clearTimeout(request.timeout);
    pending.delete(requestID);
    cancelTransferForRequest(requestID, 1);
    postResponse(callID, undefined, { code: "closed", message: "request was canceled" });
    return;
  }
}

function pumpTransfer(transfer: OutgoingTransfer): void {
  while (!transfer.ended && transfer.nextOffset < transfer.bytes.length && transfer.windowChunks > 0 && transfer.windowBytes > 0n) {
    const size = Math.min(transfer.chunkSize, transfer.bytes.length - transfer.nextOffset);
    if (BigInt(size) > transfer.windowBytes) return;
    const chunkID = transfer.nextChunk, payload = transfer.bytes.slice(transfer.nextOffset, transfer.nextOffset + size);
    sendRaw(encodeData(transfer.transferID, transfer.requestID, chunkID, payload, chunkID === 0, transfer.nextOffset + size === transfer.bytes.length));
    transfer.inFlight.set(chunkID, { payload, sentAt: Date.now(), retries: 0 });
    transfer.nextChunk += 1; transfer.nextOffset += size; transfer.windowChunks -= 1; transfer.windowBytes -= BigInt(size);
    emitProgress(transfer, "sending");
  }
  if (!transfer.ended && transfer.nextOffset === transfer.bytes.length && transfer.inFlight.size === 0) {
    transfer.ended = true;
    sendRaw(encodeTransferEnd(transfer.transferID, transfer.requestID));
    transfer.commitSentAt = Date.now();
  }
}

function emitProgress(transfer: OutgoingTransfer, state: TransferProgress["state"]): void {
  post({ type: "progress", progress: { transferID: transfer.transferID, totalBytes: transfer.bytes.length, sentBytes: transfer.nextOffset, acknowledgedBytes: transfer.acknowledgedBytes, state } });
}

function handleIncomingTransferBegin(frame: import("./protocol").Frame): void {
  const begin = decodeTransferBegin(frame);
  const partsSize = begin.parts.reduce((size, part) => size + part.totalSize, 0n);
  if (incomingTransfers.size >= (config?.protocol.maxConcurrentTransfers ?? 16) || (frame.requestID === 0n) !== (begin.event.length === 0) || begin.totalSize > BigInt(config?.protocol.maxTransferBytes ?? 64 << 20) || begin.totalSize > BigInt(Number.MAX_SAFE_INTEGER) || begin.chunkSize === 0 || begin.chunkSize > (config?.protocol.chunkSize ?? DefaultChunkSize) || begin.chunkCount !== Math.ceil(Number(begin.totalSize) / begin.chunkSize) || begin.flags & ~TransferFlag.ChecksumSHA256 || (begin.parts.length > 0 && partsSize !== begin.totalSize)) {
    sendRaw(encodeNack({ transferID: frame.transferID, chunkFrom: 0, chunkTo: 0, reasonCode: 6, flags: 0 }));
    return;
  }
  if (incomingTransfers.has(frame.transferID)) throw new ProtocolError("duplicate incoming transfer");
  const transfer: IncomingTransfer = { requestID: frame.requestID, transferID: frame.transferID, totalSize: Number(begin.totalSize), chunkSize: begin.chunkSize, chunkCount: begin.chunkCount, event: begin.event, field: begin.field, fields: begin.fields, parts: begin.parts, contentType: begin.contentType, flags: begin.flags, checksum: begin.checksum, chunks: [], receivedBytes: 0, nextChunk: 0 };
  incomingTransfers.set(frame.transferID, transfer);
  emitIncomingProgress(transfer, "receiving");
  const windowChunks = config?.protocol.maxInFlightChunks ?? 16;
  sendRaw(encodeWindow({ transferID: frame.transferID, windowBytes: BigInt(Math.min(transfer.totalSize, windowChunks * transfer.chunkSize)), windowChunks, flags: WindowFlag.Transfer }));
}

function handleIncomingTransferData(frame: import("./protocol").Frame): void {
  const transfer = incomingTransfers.get(frame.transferID);
  if (!transfer) {
    sendRaw(encodeNack({ transferID: frame.transferID, chunkFrom: frame.chunkID, chunkTo: frame.chunkID, reasonCode: 3, flags: 0 }));
    return;
  }
  if (frame.chunkID < transfer.nextChunk) {
    sendRaw(encodeAck({ transferID: frame.transferID, chunkFrom: 0, chunkTo: transfer.nextChunk - 1, receivedBytes: BigInt(transfer.receivedBytes) }));
    return;
  }
  if (frame.chunkID > transfer.nextChunk || frame.chunkID >= transfer.chunkCount || !frame.payload.length) {
    sendRaw(encodeNack({ transferID: frame.transferID, chunkFrom: transfer.nextChunk, chunkTo: frame.chunkID, reasonCode: 1, flags: 0 }));
    return;
  }
  const expected = frame.chunkID + 1 === transfer.chunkCount ? transfer.totalSize - frame.chunkID * transfer.chunkSize : transfer.chunkSize;
  const allowedFlags = 1 | 2 | 4;
  const first = Boolean(frame.flags & 1), last = Boolean(frame.flags & 2);
  if (frame.flags & ~allowedFlags || !Boolean(frame.flags & 4) || first !== (frame.chunkID === 0) || last !== (frame.chunkID + 1 === transfer.chunkCount) || frame.payload.length !== expected) {
    sendRaw(encodeNack({ transferID: frame.transferID, chunkFrom: frame.chunkID, chunkTo: frame.chunkID, reasonCode: 2, flags: 0 }));
    return;
  }
  transfer.chunks.push(frame.payload.slice());
  transfer.receivedBytes += frame.payload.length;
  transfer.nextChunk += 1;
  emitIncomingProgress(transfer, "receiving");
  sendRaw(encodeAck({ transferID: frame.transferID, chunkFrom: frame.chunkID, chunkTo: frame.chunkID, receivedBytes: BigInt(transfer.receivedBytes) }));
  sendRaw(encodeWindow({ transferID: frame.transferID, windowBytes: BigInt(frame.payload.length), windowChunks: 1, flags: WindowFlag.Transfer }));
}

async function handleIncomingTransferEnd(frame: import("./protocol").Frame): Promise<void> {
  const transfer = incomingTransfers.get(frame.transferID);
  if (!transfer) {
    const completed = completedIncoming.get(frame.transferID);
    if (completed) { sendRaw(encodeTransferState({ transferID: frame.transferID, receivedBytes: completed.receivedBytes, nextChunk: completed.nextChunk, flags: TransferStateFlag.Completed, reasonCode: 0 })); return; }
    throw new ProtocolError("transfer end for unknown transfer");
  }
  incomingTransfers.delete(frame.transferID);
  if (transfer.receivedBytes !== transfer.totalSize || transfer.nextChunk !== transfer.chunkCount) {
    emitIncomingProgress(transfer, "failed");
    sendRaw(encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: TransferStateFlag.Failed, reasonCode: 6 }));
    return;
  }
  const blob = new Blob(transfer.chunks.map((chunk) => chunk as unknown as BlobPart));
  if (transfer.flags & TransferFlag.ChecksumSHA256) {
    const actual = new Uint8Array(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()));
    if (!equalBytes(actual, transfer.checksum)) {
      sendRaw(encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: TransferStateFlag.Failed, reasonCode: 2 }));
      post({ type: "protocol", event: { code: "checksum_mismatch", message: "incoming transfer checksum mismatch", frameType: FrameType.TransferEnd, transferID: frame.transferID } });
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
  sendRaw(encodeTransferState({ transferID: frame.transferID, receivedBytes: BigInt(transfer.receivedBytes), nextChunk: transfer.nextChunk, flags: TransferStateFlag.Completed, reasonCode: 0 }));
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

function materializeEventMessage(message: import("./protocol").EventMessage): unknown {
  if (!message.fields?.length) return message.data;
  const result: Record<string, unknown> = message.data && typeof message.data === "object" && !Array.isArray(message.data) ? { ...(message.data as Record<string, unknown>) } : { body: message.data };
  for (const field of message.fields) { try { result[field.key] = JSON.parse(field.value); } catch { result[field.key] = field.value; } }
  return result;
}

function handleIncomingTransferResume(frame: import("./protocol").Frame): void {
  const resume = decodeTransferResume(frame), transfer = incomingTransfers.get(resume.transferID);
  const expectedToken = config?.protocol.resumeToken ?? new Uint8Array();
  const accepted = Boolean(transfer && equalBytes(resume.token, expectedToken) && transfer.receivedBytes === Number(resume.receivedBytes) && transfer.nextChunk === resume.nextChunk);
  sendRaw(encodeTransferState({ transferID: resume.transferID, receivedBytes: BigInt(transfer?.receivedBytes ?? 0), nextChunk: transfer?.nextChunk ?? 0, flags: accepted ? TransferStateFlag.ResumeAccepted : TransferStateFlag.ResumeRejected, reasonCode: accepted ? 0 : 3 }));
  if (accepted && transfer) { const chunks = config?.protocol.maxInFlightChunks ?? 16; sendRaw(encodeWindow({ transferID: resume.transferID, windowBytes: BigInt(Math.min(transfer.totalSize - transfer.receivedBytes, chunks * transfer.chunkSize)), windowChunks: chunks, flags: WindowFlag.Transfer })); }
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean { return left.length === right.length && left.every((value, index) => value === right[index]); }

function emitIncomingProgress(transfer: IncomingTransfer, state: TransferProgress["state"]): void {
  post({ type: "progress", progress: { transferID: transfer.transferID, totalBytes: transfer.totalSize, sentBytes: 0, acknowledgedBytes: 0, receivedBytes: transfer.receivedBytes, direction: "receive", state } });
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
  clearTimeout(request.timeout);
  pending.delete(requestID);
  postResponse(request.callID, data);
}

function rejectResponse(requestID: bigint, code: SocketErrorCode, message: string): void {
  const request = pending.get(requestID);
  if (!request) {
    emitError(code, message);
    return;
  }
  clearTimeout(request.timeout);
  pending.delete(requestID);
  postResponse(request.callID, undefined, { code, message });
}

function handleClose(current: WebSocket): void {
  if (socket !== current) {
    return;
  }
  socket = undefined;
  const preserveOutgoing = desiredConnection && Boolean(remoteHello?.capabilities && (remoteHello.capabilities & Capability.TransferResume));
  resumePending = preserveOutgoing && outgoingTransfers.size > 0;
  remoteHello = undefined;
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
  if (phase === "open" && remoteHello?.capabilities && (remoteHello.capabilities & Capability.GracefulClose)) {
    phase = "closing";
    sendRaw(encodeClose({ reasonCode: reason === "client" ? 4 : 0, flags: CloseFlag.Immediate, drainTimeoutMillis: 0 }));
    setTimeout(() => socket?.close(), config?.timeout ?? 10_000);
    return;
  }
  socket.close();
}

function handlePeerClose(current: WebSocket, close: { reasonCode: number; flags: number; drainTimeoutMillis: number }): void {
  if (!(close.flags & CloseFlag.Drain)) {
    phase = "closing";
    sendRaw(encodeClose(close, true));
    current.close();
    return;
  }
  if (!(remoteHello?.capabilities && (remoteHello.capabilities & Capability.GracefulClose))) throw new ProtocolError("graceful close was not negotiated");
  phase = "draining";
  const deadline = Date.now() + (close.drainTimeoutMillis || 5_000);
  const finish = () => {
    if ((outgoingTransfers.size || incomingTransfers.size) && Date.now() < deadline) {
      setTimeout(finish, 10);
      return;
    }
    phase = "closing";
    sendRaw(encodeClose({ ...close, reasonCode: outgoingTransfers.size || incomingTransfers.size ? 5 : close.reasonCode }, true));
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
    monitorOutgoingTransfers();
    if ((phase === "open" || phase === "draining") && Date.now() - lastReadAt > (config?.protocol.heartbeatTimeout ?? 20_000)) {
      emitError("timeout", "ETP heartbeat timed out");
      socket?.close();
      return;
    }
    if (isOpen() && Date.now() - lastWriteAt >= (config?.protocol.heartbeatInterval ?? 10_000)) {
      sendRaw(encodePing());
    }
  }, 1_000);
}

function monitorOutgoingTransfers(): void {
  const now = Date.now(), ackTimeout = config?.protocol.ackTimeout ?? 2_000, retryLimit = config?.protocol.retryLimit ?? 3;
  for (const transfer of outgoingTransfers.values()) {
    let failed = false;
    const total = Math.ceil(transfer.bytes.length / transfer.chunkSize);
    for (const [chunkID, sent] of transfer.inFlight) {
      if (now - sent.sentAt < ackTimeout) continue;
      if (sent.retries >= retryLimit) { failed = true; break; }
      sendRaw(encodeData(transfer.transferID, transfer.requestID, chunkID, sent.payload, chunkID === 0, chunkID + 1 === total));
      sent.sentAt = now;
      sent.retries += 1;
    }
    if (!failed && transfer.ended && now - transfer.commitSentAt >= ackTimeout) {
      if (transfer.commitRetries >= retryLimit) failed = true;
      else { sendRaw(encodeTransferEnd(transfer.transferID, transfer.requestID)); transfer.commitSentAt = now; transfer.commitRetries += 1; }
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
    throw new ProtocolError(message);
  }
}

function requireTransferPhase(message: string): void {
  if (phase !== "open" && phase !== "draining") throw new ProtocolError(message);
}

function enforceFrameCapability(frameType: number, transferID: bigint): void {
  if (phase === "auth" || phase === "hello" || !remoteHello) return;
  let capability = 0n;
  if (frameType === FrameType.Ack) capability = Capability.Ack;
  else if (frameType === FrameType.Nack) capability = Capability.Nack;
  else if (frameType === FrameType.Ping || frameType === FrameType.Pong) capability = Capability.Heartbeat;
  else if (frameType === FrameType.Window) capability = Capability.FlowControl;
  else if (frameType === FrameType.Cancel || frameType === FrameType.CancelAck) capability = Capability.Cancel;
  else if (frameType === FrameType.Request || frameType === FrameType.Response) capability = Capability.RequestResponse;
  else if (frameType === FrameType.TransferResume) capability = Capability.TransferResume;
  else if (frameType === FrameType.TransferBegin || frameType === FrameType.TransferEnd || frameType === FrameType.TransferState || (frameType === FrameType.Data && transferID !== 0n)) capability = Capability.Transfers;
  if (capability && (!(BrowserCapabilities & capability) || !(remoteHello.capabilities & capability))) throw new ProtocolError(`ETP capability was not negotiated for frame type ${frameType}`);
}

function enforceIncomingRate(bytes: number): void {
  const second = Math.floor(Date.now() / 1_000);
  if (second !== rateSecond) { rateSecond = second; rateFrames = 0; rateBytes = 0; }
  rateFrames += 1; rateBytes += bytes;
  if (rateFrames > (config?.protocol.maxFramesPerSecond ?? 2_000) || rateBytes > (config?.protocol.maxBytesPerSecond ?? 64 << 20)) throw new ProtocolError("incoming ETP rate limit exceeded");
}

function sendRaw(frame: ArrayBuffer): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    throw new ProtocolError("WebSocket is not open");
  }
  socket.send(frame);
  lastWriteAt = Date.now();
}

function rejectPending(code: SocketErrorCode, message: string): void {
  for (const request of pending.values()) {
    clearTimeout(request.timeout);
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
