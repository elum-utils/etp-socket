const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export const HeaderSize = 40;
export const MaxFrameBytes = 8 << 20;
export const WireVersion = 1;
export const DefaultChunkSize = 16 << 10;
export const MaxTransferFields = 1024;
export const MaxTransferParts = 1024;

export const FrameType = {
  Data: 1, Ack: 2, Nack: 3, Ping: 4, Pong: 5, Window: 6, Cancel: 7, CancelAck: 8,
  Hello: 9, HelloAck: 10, Close: 11, TransferBegin: 12, TransferEnd: 13, TransferState: 14,
  Auth: 15, AuthAccept: 16, AuthReject: 17, Request: 18, Response: 19, Error: 20,
  GoAway: 21, CloseAck: 22, TransferResume: 23,
} as const;
export const FrameFlag = { First: 1 << 0, Last: 1 << 1, AckRequest: 1 << 2, Encrypted: 1 << 3, Compressed: 1 << 4, Control: 1 << 5 } as const;
export const Priority = { Critical: 0, High: 1, Normal: 2, Low: 3, Idle: 4 } as const;
export const Channel = { Control: 0, Realtime: 1, Bulk: 2, Sync: 3, Background: 4 } as const;
export const Schema = { Hello: 1, Text: 100, TransferBegin: 200, Ack: 201, Cancel: 202, Nack: 203, Auth: 204, AuthResult: 205, Event: 300, Error: 400, GoAway: 401, Close: 402, Window: 500, TransferState: 501 } as const;
export const Capability = {
  Transfers: 1n << 0n, Cancel: 1n << 1n, Ack: 1n << 2n, Nack: 1n << 3n, Heartbeat: 1n << 4n,
  TransferSHA256: 1n << 5n, FlowControl: 1n << 6n, SlowlorisGuard: 1n << 7n, ProtocolEvents: 1n << 8n,
  RequestResponse: 1n << 9n, GracefulClose: 1n << 10n, TransferResume: 1n << 11n, TransferCommit: 1n << 12n,
} as const;
export const DefaultCapabilities = Capability.Transfers | Capability.Cancel | Capability.Ack | Capability.Nack | Capability.Heartbeat | Capability.TransferSHA256 | Capability.FlowControl | Capability.SlowlorisGuard | Capability.ProtocolEvents | Capability.RequestResponse | Capability.GracefulClose | Capability.TransferCommit;
export const AllCapabilities = DefaultCapabilities | Capability.TransferResume;
export const CloseFlag = { Immediate: 1 << 0, Drain: 1 << 1, NoNewRequests: 1 << 2, NoNewTransfers: 1 << 3 } as const;
export const WindowFlag = { Connection: 1 << 0, Transfer: 1 << 1 } as const;
export const TransferStateFlag = { ResumeAccepted: 1 << 0, ResumeRejected: 1 << 1, Completed: 1 << 2, Failed: 1 << 3 } as const;
export const TransferFlag = { ChecksumSHA256: 1 << 0 } as const;
export const CancelReason = { User: 1, Timeout: 2, Network: 3, Rejected: 4, Protocol: 5 } as const;
export const CancelAckStatus = { OK: 1, NotFound: 2, Completed: 3 } as const;

const knownFrameFlags = Object.values(FrameFlag).reduce((all, flag) => all | flag, 0);
const knownCloseFlags = Object.values(CloseFlag).reduce((all, flag) => all | flag, 0);

export type Frame = { type: number; flags: number; priority: number; channel: number; schema: number; requestID: bigint; transferID: bigint; chunkID: number; payload: Uint8Array };
export type Field = { key: string; value: string };
export type TransferPart = { field: string; index: number; name: string; totalSize: bigint; contentType: number };
export type EventMessage = { event: string; data: unknown; fields?: Field[] };
export type Hello = { role: string; capabilities: bigint; maxFrameBytes: number; maxChunkSize: number; maxTransferBytes: bigint; maxInFlightChunks: number; heartbeatMillis: number };
export type TransferBegin = { totalSize: bigint; chunkSize: number; chunkCount: number; contentType: number; flags: number; checksum: Uint8Array; name: string; event: string; field: string; index: number; parts: TransferPart[]; fields: Field[] };
export type Ack = { transferID: bigint; chunkFrom: number; chunkTo: number; receivedBytes: bigint };
export type Nack = { transferID: bigint; chunkFrom: number; chunkTo: number; reasonCode: number; flags: number };
export type Window = { transferID: bigint; windowBytes: bigint; windowChunks: number; flags: number };
export type TransferState = { transferID: bigint; receivedBytes: bigint; nextChunk: number; flags: number; reasonCode: number };
export type TransferResume = { transferID: bigint; receivedBytes: bigint; nextChunk: number; token: Uint8Array };
export type CloseMessage = { reasonCode: number; flags: number; drainTimeoutMillis: number };
export type GoAway = CloseMessage & { lastAcceptedRequestID: bigint; lastAcceptedTransferID: bigint; message: string };
export type ErrorMessage = { code: number; frameType: number; schema: number; requestID: bigint; transferID: bigint; message: string };

export class ProtocolError extends Error { constructor(message: string) { super(message); this.name = "ProtocolError"; } }

export function encodeAuth(token: string): ArrayBuffer {
  const tokenBytes = encoder.encode(token), payload = new Uint8Array(12 + tokenBytes.length), view = viewOf(payload);
  view.setUint16(0, 1, false); view.setUint32(8, tokenBytes.length, false); payload.set(tokenBytes, 12);
  return encodeControl(FrameType.Auth, Schema.Auth, payload);
}

export function encodeHello(hello: Partial<Hello> = {}): ArrayBuffer {
  const role = encoder.encode(hello.role ?? "client"), payload = new Uint8Array(40 + role.length), view = viewOf(payload);
  view.setBigUint64(0, hello.capabilities ?? DefaultCapabilities, false); view.setUint32(8, hello.maxFrameBytes ?? MaxFrameBytes, false); view.setUint32(12, hello.maxChunkSize ?? DefaultChunkSize, false); view.setBigUint64(16, hello.maxTransferBytes ?? (512n << 20n), false); view.setUint32(24, hello.maxInFlightChunks ?? 16, false); view.setUint32(28, hello.heartbeatMillis ?? 10_000, false); view.setUint32(36, role.length, false); payload.set(role, 40);
  return encodeControl(FrameType.Hello, Schema.Hello, payload);
}

export function encodeRequest(requestID: bigint, event: string, data: unknown, fields: Field[] = []): ArrayBuffer { return encodeEvent(FrameType.Request, requestID, event, data, fields); }
export function encodeResponse(requestID: bigint, event: string, data: unknown, fields: Field[] = []): ArrayBuffer { return encodeEvent(FrameType.Response, requestID, event, data, fields); }
export function encodePing(): ArrayBuffer { return encodeControl(FrameType.Ping, 0, new Uint8Array()); }
export function encodePong(): ArrayBuffer { return encodeControl(FrameType.Pong, 0, new Uint8Array()); }
export function encodeData(transferID: bigint, requestID: bigint, chunkID: number, payload: Uint8Array, first: boolean, last: boolean): ArrayBuffer { return encodeFrame({ type: FrameType.Data, flags: FrameFlag.AckRequest | (first ? FrameFlag.First : 0) | (last ? FrameFlag.Last : 0), priority: Priority.Low, channel: Channel.Bulk, schema: 0, requestID, transferID, chunkID, payload }); }
export function encodeTransferBegin(transferID: bigint, requestID: bigint, begin: TransferBegin): ArrayBuffer { return encodeFrame({ type: FrameType.TransferBegin, flags: FrameFlag.First, priority: Priority.Low, channel: Channel.Bulk, schema: Schema.TransferBegin, requestID, transferID, chunkID: 0, payload: encodeTransferBeginPayload(begin) }); }
export function encodeTransferEnd(transferID: bigint, requestID: bigint): ArrayBuffer { return encodeFrame({ type: FrameType.TransferEnd, flags: FrameFlag.Last, priority: Priority.Low, channel: Channel.Bulk, schema: 0, requestID, transferID, chunkID: 0, payload: new Uint8Array() }); }
export function encodeAck(value: Ack): ArrayBuffer { return encodeControl(FrameType.Ack, Schema.Ack, encodeAckPayload(value), value.transferID); }
export function encodeNack(value: Nack): ArrayBuffer { return encodeControl(FrameType.Nack, Schema.Nack, encodeNackPayload(value), value.transferID); }
export function encodeWindow(value: Window): ArrayBuffer { return encodeControl(FrameType.Window, Schema.Window, encodeWindowPayload(value), value.transferID, Priority.High); }
export function encodeCancel(transferID: bigint, reasonCode: number = CancelReason.User, flags = 0): ArrayBuffer { const payload = new Uint8Array(12), view = viewOf(payload); view.setBigUint64(0, transferID, false); view.setUint16(8, reasonCode, false); view.setUint16(10, flags, false); return encodeControl(FrameType.Cancel, Schema.Cancel, payload, transferID); }
export function encodeCancelAck(transferID: bigint, status: number): ArrayBuffer { const payload = new Uint8Array(16), view = viewOf(payload); view.setBigUint64(0, transferID, false); payload[8] = status; return encodeControl(FrameType.CancelAck, Schema.Cancel, payload, transferID); }
export function encodeTransferResume(requestID: bigint, value: TransferResume): ArrayBuffer { return encodeFrame({ type: FrameType.TransferResume, flags: FrameFlag.Control, priority: Priority.High, channel: Channel.Bulk, schema: Schema.TransferState, requestID, transferID: value.transferID, chunkID: 0, payload: encodeTransferResumePayload(value) }); }
export function encodeTransferState(value: TransferState): ArrayBuffer { return encodeControl(FrameType.TransferState, Schema.TransferState, encodeTransferStatePayload(value), value.transferID, Priority.High); }
export function encodeClose(value: CloseMessage, ack = false): ArrayBuffer { return encodeControl(ack ? FrameType.CloseAck : FrameType.Close, Schema.Close, encodeClosePayload(value)); }
export function encodeGoAway(value: GoAway): ArrayBuffer { return encodeControl(FrameType.GoAway, Schema.GoAway, encodeGoAwayPayload(value)); }

export function decodeFrame(data: ArrayBuffer): Frame {
  if (data.byteLength < HeaderSize || data.byteLength > MaxFrameBytes) throw new ProtocolError("invalid ETP frame length");
  const view = new DataView(data), type = view.getUint8(1), flags = view.getUint16(2, false), priority = view.getUint8(4), channel = view.getUint16(6, false);
  if (view.getUint8(0) !== WireVersion) throw new ProtocolError("unsupported ETP wire version");
  if (view.getUint8(5) !== HeaderSize || view.getUint16(8, false) !== HeaderSize || view.getUint16(10, false) !== 0) throw new ProtocolError("invalid ETP frame header");
  if (type < FrameType.Data || type > FrameType.TransferResume || priority > Priority.Idle || channel > Channel.Background || flags & ~knownFrameFlags || flags & (FrameFlag.Encrypted | FrameFlag.Compressed)) throw new ProtocolError("invalid ETP frame values");
  const payloadLength = view.getUint32(12, false); if (HeaderSize + payloadLength !== data.byteLength) throw new ProtocolError("invalid ETP frame payload length");
  const frame = { type, flags, priority, channel, schema: view.getUint32(16, false), requestID: view.getBigUint64(20, false), transferID: view.getBigUint64(28, false), chunkID: view.getUint32(36, false), payload: new Uint8Array(data, HeaderSize, payloadLength) };
  validateFrameEnvelope(frame);
  return frame;
}

export function decodeEvent(frame: Frame): EventMessage {
  if (frame.schema !== Schema.Event || frame.payload.length < 8) throw new ProtocolError("invalid ETP event frame");
  const view = viewOf(frame.payload), eventEnd = checkedEnd(frame.payload, 4, view.getUint32(0, false)); if (eventEnd === undefined || eventEnd + 4 > frame.payload.length) throw new ProtocolError("invalid ETP event name");
  const dataEnd = checkedEnd(frame.payload, eventEnd + 4, view.getUint32(eventEnd, false)); if (dataEnd === undefined) throw new ProtocolError("invalid ETP event data");
  const fields = decodeFields(frame.payload.subarray(dataEnd));
  const event = decodeUTF8(frame.payload.subarray(4, eventEnd));
  const data = JSON.parse(decodeUTF8(frame.payload.subarray(eventEnd + 4, dataEnd)));
  return fields.length ? { event, data, fields } : { event, data };
}
export function decodeHello(frame: Frame): Hello {
  if (frame.schema !== Schema.Hello || frame.payload.length < 40) throw new ProtocolError("invalid ETP hello frame"); const view = viewOf(frame.payload), end = checkedEnd(frame.payload, 40, view.getUint32(36, false));
  if (end === undefined || end !== frame.payload.length) throw new ProtocolError("invalid ETP hello payload");
  if (!isZero(frame.payload.subarray(32, 36))) throw new ProtocolError("invalid ETP hello reserved bytes");
  const hello = { capabilities: view.getBigUint64(0, false), maxFrameBytes: view.getUint32(8, false), maxChunkSize: view.getUint32(12, false), maxTransferBytes: view.getBigUint64(16, false), maxInFlightChunks: view.getUint32(24, false), heartbeatMillis: view.getUint32(28, false), role: decodeUTF8(frame.payload.subarray(40)) };
  if (hello.maxFrameBytes < HeaderSize || hello.maxFrameBytes > MaxFrameBytes || hello.maxChunkSize === 0 || hello.maxChunkSize > hello.maxFrameBytes - HeaderSize || hello.maxTransferBytes === 0n || hello.maxInFlightChunks === 0 || hello.heartbeatMillis === 0) throw new ProtocolError("invalid ETP hello limits"); return hello;
}
export function decodeAuthReject(frame: Frame): string { if (frame.payload.length < 8) throw new ProtocolError("invalid ETP auth rejection"); const view = viewOf(frame.payload), end = checkedEnd(frame.payload, 8, view.getUint32(4, false)); if (end === undefined || end !== frame.payload.length) throw new ProtocolError("invalid ETP auth rejection message"); return decodeUTF8(frame.payload.subarray(8)); }
export function decodeAuthAccept(frame: Frame): string { if (frame.payload.length < 4) throw new ProtocolError("invalid ETP auth acceptance"); const view = viewOf(frame.payload), end = checkedEnd(frame.payload, 4, view.getUint32(0, false)); if (end === undefined || end !== frame.payload.length) throw new ProtocolError("invalid ETP auth acceptance user id"); return decodeUTF8(frame.payload.subarray(4)); }
export function decodeText(frame: Frame): string { if (frame.payload.length < 4) throw new ProtocolError("invalid ETP text message"); const view = viewOf(frame.payload), end = checkedEnd(frame.payload, 4, view.getUint32(0, false)); if (end === undefined || end !== frame.payload.length) throw new ProtocolError("invalid ETP text message length"); return decodeUTF8(frame.payload.subarray(4)); }
export function decodeError(frame: Frame): ErrorMessage { return decodeErrorPayload(frame.payload); }
export function decodeGoAway(frame: Frame): GoAway { if (frame.schema !== Schema.GoAway) throw new ProtocolError("invalid goaway schema"); return decodeGoAwayPayload(frame.payload); }
export function decodeClose(frame: Frame): CloseMessage { if (frame.schema !== Schema.Close) throw new ProtocolError("invalid close schema"); return decodeClosePayload(frame.payload); }
export function decodeAck(frame: Frame): Ack { const value = decodeAckPayload(frame.payload); if (value.transferID !== frame.transferID || value.chunkFrom > value.chunkTo) throw new ProtocolError("invalid ack transfer id or range"); return value; }
export function decodeNack(frame: Frame): Nack { const value = decodeNackPayload(frame.payload); if (value.transferID !== frame.transferID || value.chunkFrom > value.chunkTo) throw new ProtocolError("invalid nack transfer id or range"); return value; }
export function decodeWindow(frame: Frame): Window { const value = decodeWindowPayload(frame.payload); if (value.transferID !== frame.transferID || value.transferID === 0n || value.flags !== WindowFlag.Transfer) throw new ProtocolError("invalid transfer window"); return value; }
export function decodeCancel(frame: Frame): { transferID: bigint; reasonCode: number; flags: number } { const p = frame.payload; if (p.length !== 12) throw new ProtocolError("invalid cancel payload length"); const v = viewOf(p), transferID = v.getBigUint64(0, false), reasonCode = v.getUint16(8, false), flags = v.getUint16(10, false); if (transferID !== frame.transferID || reasonCode < 1 || reasonCode > 5 || flags & ~3) throw new ProtocolError("invalid cancel values"); return { transferID, reasonCode, flags }; }
export function decodeCancelAck(frame: Frame): { transferID: bigint; status: number } { const p = frame.payload; if (p.length !== 16 || p[8] < 1 || p[8] > 3 || !isZero(p.subarray(9))) throw new ProtocolError("invalid cancel acknowledgment"); const transferID = viewOf(p).getBigUint64(0, false); if (transferID !== frame.transferID) throw new ProtocolError("cancel acknowledgment transfer id mismatch"); return { transferID, status: p[8] }; }
export function decodeTransferBegin(frame: Frame): TransferBegin { if (frame.schema !== Schema.TransferBegin) throw new ProtocolError("invalid transfer begin schema"); return decodeTransferBeginPayload(frame.payload); }
export function decodeTransferState(frame: Frame): TransferState { const value = decodeTransferStatePayload(frame.payload), flags = value.flags; if (value.transferID !== frame.transferID || flags === 0 || flags & ~(TransferStateFlag.ResumeAccepted | TransferStateFlag.ResumeRejected | TransferStateFlag.Completed | TransferStateFlag.Failed) || (flags & (flags - 1)) !== 0 || ((flags === TransferStateFlag.Completed || flags === TransferStateFlag.ResumeAccepted) ? value.reasonCode !== 0 : value.reasonCode === 0)) throw new ProtocolError("invalid transfer state"); return value; }
export function decodeTransferResume(frame: Frame): TransferResume { const value = decodeTransferResumePayload(frame.payload); if (value.transferID !== frame.transferID) throw new ProtocolError("transfer resume id mismatch"); return value; }

function encodeEvent(type: number, requestID: bigint, event: string, data: unknown, fields: Field[]): ArrayBuffer { const eventBytes = encoder.encode(event), dataBytes = encoder.encode(JSON.stringify(data) ?? "null"), fieldsBytes = encodeFields(fields), payload = new Uint8Array(8 + eventBytes.length + dataBytes.length + fieldsBytes.length), view = viewOf(payload); view.setUint32(0, eventBytes.length, false); payload.set(eventBytes, 4); view.setUint32(4 + eventBytes.length, dataBytes.length, false); payload.set(dataBytes, 8 + eventBytes.length); payload.set(fieldsBytes, 8 + eventBytes.length + dataBytes.length); return encodeFrame({ type, flags: FrameFlag.First | FrameFlag.Last, priority: Priority.Normal, channel: Channel.Realtime, schema: Schema.Event, requestID, transferID: 0n, chunkID: 0, payload }); }
function encodeControl(type: number, schema: number, payload: Uint8Array, transferID = 0n, priority: number = Priority.Critical): ArrayBuffer { return encodeFrame({ type, flags: FrameFlag.Control, priority, channel: Channel.Control, schema, requestID: 0n, transferID, chunkID: 0, payload }); }
function encodeFrame(frame: Frame): ArrayBuffer { if (frame.payload.length > MaxFrameBytes - HeaderSize) throw new ProtocolError("ETP frame exceeds maximum size"); const out = new Uint8Array(HeaderSize + frame.payload.length), view = viewOf(out); view.setUint8(0, WireVersion); view.setUint8(1, frame.type); view.setUint16(2, frame.flags, false); view.setUint8(4, frame.priority); view.setUint8(5, HeaderSize); view.setUint16(6, frame.channel, false); view.setUint16(8, HeaderSize, false); view.setUint32(12, frame.payload.length, false); view.setUint32(16, frame.schema, false); view.setBigUint64(20, frame.requestID, false); view.setBigUint64(28, frame.transferID, false); view.setUint32(36, frame.chunkID, false); out.set(frame.payload, HeaderSize); return out.buffer; }
function encodeFields(fields: Field[]): Uint8Array { if (fields.length === 0) return new Uint8Array(); if (fields.length > MaxTransferFields) throw new ProtocolError("too many transfer fields"); const values = fields.map((field) => ({ key: encoder.encode(field.key), value: encoder.encode(field.value) })); const out = new Uint8Array(4 + values.reduce((n, field) => n + 8 + field.key.length + field.value.length, 0)), view = viewOf(out); view.setUint32(0, values.length, false); let pos = 4; for (const field of values) { view.setUint32(pos, field.key.length, false); pos += 4; out.set(field.key, pos); pos += field.key.length; view.setUint32(pos, field.value.length, false); pos += 4; out.set(field.value, pos); pos += field.value.length; } return out; }
function decodeFields(payload: Uint8Array): Field[] { if (!payload.length) return []; if (payload.length < 4) throw new ProtocolError("invalid transfer fields length"); const view = viewOf(payload), count = view.getUint32(0, false); if (count > MaxTransferFields || count * 8 > payload.length - 4) throw new ProtocolError("invalid transfer field count"); const fields: Field[] = []; let pos = 4; for (let i = 0; i < count; i += 1) { const key = readLengthEnd(payload, view, pos); pos = key.next; const value = readLengthEnd(payload, view, pos); pos = value.next; fields.push({ key: decodeUTF8(payload.subarray(key.start, key.end)), value: decodeUTF8(payload.subarray(value.start, value.end)) }); } if (pos !== payload.length) throw new ProtocolError("invalid transfer fields trailing data"); return fields; }
function encodeTransferBeginPayload(value: TransferBegin): Uint8Array { if (value.checksum.length !== 32 || value.parts.length > MaxTransferParts) throw new ProtocolError("invalid transfer metadata"); const name = encoder.encode(value.name), event = encoder.encode(value.event), field = encoder.encode(value.field), parts = encodeParts(value.parts), fields = encodeFields(value.fields), out = new Uint8Array(72 + name.length + event.length + field.length + parts.length + fields.length), view = viewOf(out); view.setBigUint64(0, value.totalSize, false); view.setUint32(8, value.chunkSize, false); view.setUint32(12, value.chunkCount, false); view.setUint32(16, value.contentType, false); view.setUint32(20, value.flags, false); out.set(value.checksum, 24); view.setUint32(56, name.length, false); out.set(name, 60); let pos = 60 + name.length; view.setUint32(pos, event.length, false); pos += 4; out.set(event, pos); pos += event.length; view.setUint32(pos, field.length, false); pos += 4; out.set(field, pos); pos += field.length; view.setUint32(pos, value.index, false); pos += 4; out.set(parts, pos); pos += parts.length; out.set(fields, pos); return out; }
function decodeTransferBeginPayload(payload: Uint8Array): TransferBegin { if (payload.length < 72) throw new ProtocolError("transfer begin payload too small"); const view = viewOf(payload), name = readLengthEnd(payload, view, 56); let pos = name.next; const event = readLengthEnd(payload, view, pos); pos = event.next; const field = readLengthEnd(payload, view, pos); pos = field.next; if (pos + 4 > payload.length) throw new ProtocolError("invalid transfer field index"); const index = view.getUint32(pos, false); pos += 4; const parts = decodeParts(payload.subarray(pos)); pos += parts.bytes; const fields = decodeFields(payload.subarray(pos)); return { totalSize: view.getBigUint64(0, false), chunkSize: view.getUint32(8, false), chunkCount: view.getUint32(12, false), contentType: view.getUint32(16, false), flags: view.getUint32(20, false), checksum: payload.slice(24, 56), name: decodeUTF8(payload.subarray(name.start, name.end)), event: decodeUTF8(payload.subarray(event.start, event.end)), field: decodeUTF8(payload.subarray(field.start, field.end)), index, parts: parts.values, fields }; }
function encodeParts(parts: TransferPart[]): Uint8Array { if (parts.length > MaxTransferParts) throw new ProtocolError("too many transfer parts"); const values = parts.map((part) => ({ ...part, fieldBytes: encoder.encode(part.field), nameBytes: encoder.encode(part.name) })); const out = new Uint8Array(4 + values.reduce((size, part) => size + 24 + part.fieldBytes.length + part.nameBytes.length, 0)), view = viewOf(out); view.setUint32(0, values.length, false); let pos = 4; for (const part of values) { view.setUint32(pos, part.fieldBytes.length, false); pos += 4; out.set(part.fieldBytes, pos); pos += part.fieldBytes.length; view.setUint32(pos, part.index, false); pos += 4; view.setUint32(pos, part.nameBytes.length, false); pos += 4; out.set(part.nameBytes, pos); pos += part.nameBytes.length; view.setBigUint64(pos, part.totalSize, false); pos += 8; view.setUint32(pos, part.contentType, false); pos += 4; } return out; }
function decodeParts(payload: Uint8Array): { values: TransferPart[]; bytes: number } { if (payload.length < 4) throw new ProtocolError("invalid transfer parts length"); const view = viewOf(payload), count = view.getUint32(0, false); if (count > MaxTransferParts || count * 24 > payload.length - 4) throw new ProtocolError("invalid transfer part count"); const values: TransferPart[] = []; let pos = 4; for (let i = 0; i < count; i += 1) { const field = readLengthEnd(payload, view, pos); pos = field.next; if (pos + 4 > payload.length) throw new ProtocolError("invalid transfer part index"); const index = view.getUint32(pos, false); pos += 4; const name = readLengthEnd(payload, view, pos); pos = name.next; if (pos + 12 > payload.length) throw new ProtocolError("invalid transfer part metadata"); const totalSize = view.getBigUint64(pos, false); pos += 8; const contentType = view.getUint32(pos, false); pos += 4; values.push({ field: decodeUTF8(payload.subarray(field.start, field.end)), index, name: decodeUTF8(payload.subarray(name.start, name.end)), totalSize, contentType }); } return { values, bytes: pos }; }
function encodeAckPayload(value: Ack): Uint8Array { const out = new Uint8Array(24), view = viewOf(out); view.setBigUint64(0, value.transferID, false); view.setUint32(8, value.chunkFrom, false); view.setUint32(12, value.chunkTo, false); view.setBigUint64(16, value.receivedBytes, false); return out; }
function decodeAckPayload(payload: Uint8Array): Ack { if (payload.length !== 24) throw new ProtocolError("invalid ack payload length"); const view = viewOf(payload); return { transferID: view.getBigUint64(0, false), chunkFrom: view.getUint32(8, false), chunkTo: view.getUint32(12, false), receivedBytes: view.getBigUint64(16, false) }; }
function encodeNackPayload(value: Nack): Uint8Array { const out = new Uint8Array(20), view = viewOf(out); view.setBigUint64(0, value.transferID, false); view.setUint32(8, value.chunkFrom, false); view.setUint32(12, value.chunkTo, false); view.setUint16(16, value.reasonCode, false); view.setUint16(18, value.flags, false); return out; }
function decodeNackPayload(payload: Uint8Array): Nack { if (payload.length !== 20) throw new ProtocolError("invalid nack payload length"); const view = viewOf(payload), reasonCode = view.getUint16(16, false), flags = view.getUint16(18, false); if (reasonCode < 1 || reasonCode > 7 || flags !== 0) throw new ProtocolError("invalid nack values"); return { transferID: view.getBigUint64(0, false), chunkFrom: view.getUint32(8, false), chunkTo: view.getUint32(12, false), reasonCode, flags }; }
function encodeWindowPayload(value: Window): Uint8Array { const out = new Uint8Array(24), view = viewOf(out); view.setBigUint64(0, value.transferID, false); view.setBigUint64(8, value.windowBytes, false); view.setUint32(16, value.windowChunks, false); view.setUint16(20, value.flags, false); return out; }
function decodeWindowPayload(payload: Uint8Array): Window { if (payload.length !== 24) throw new ProtocolError("invalid window payload length"); const view = viewOf(payload), flags = view.getUint16(20, false); if ((flags !== WindowFlag.Connection && flags !== WindowFlag.Transfer) || !isZero(payload.subarray(22))) throw new ProtocolError("invalid window payload"); return { transferID: view.getBigUint64(0, false), windowBytes: view.getBigUint64(8, false), windowChunks: view.getUint32(16, false), flags }; }
function encodeTransferResumePayload(value: TransferResume): Uint8Array { const out = new Uint8Array(24 + value.token.length), view = viewOf(out); view.setBigUint64(0, value.transferID, false); view.setBigUint64(8, value.receivedBytes, false); view.setUint32(16, value.nextChunk, false); view.setUint32(20, value.token.length, false); out.set(value.token, 24); return out; }
function decodeTransferResumePayload(payload: Uint8Array): TransferResume { if (payload.length < 24) throw new ProtocolError("transfer resume payload too small"); const view = viewOf(payload), end = checkedEnd(payload, 24, view.getUint32(20, false)); if (end === undefined || end !== payload.length) throw new ProtocolError("invalid transfer resume token length"); return { transferID: view.getBigUint64(0, false), receivedBytes: view.getBigUint64(8, false), nextChunk: view.getUint32(16, false), token: payload.slice(24) }; }
function encodeTransferStatePayload(value: TransferState): Uint8Array { const out = new Uint8Array(24), view = viewOf(out); view.setBigUint64(0, value.transferID, false); view.setBigUint64(8, value.receivedBytes, false); view.setUint32(16, value.nextChunk, false); view.setUint16(20, value.flags, false); view.setUint16(22, value.reasonCode, false); return out; }
function decodeTransferStatePayload(payload: Uint8Array): TransferState { if (payload.length !== 24) throw new ProtocolError("invalid transfer state payload length"); const view = viewOf(payload); return { transferID: view.getBigUint64(0, false), receivedBytes: view.getBigUint64(8, false), nextChunk: view.getUint32(16, false), flags: view.getUint16(20, false), reasonCode: view.getUint16(22, false) }; }
function encodeClosePayload(value: CloseMessage): Uint8Array { validateCloseFlags(value.flags); const out = new Uint8Array(12), view = viewOf(out); view.setUint32(0, value.reasonCode, false); view.setUint16(4, value.flags, false); view.setUint32(8, value.drainTimeoutMillis, false); return out; }
function decodeClosePayload(payload: Uint8Array): CloseMessage { if (payload.length !== 12) throw new ProtocolError("invalid close payload length"); const view = viewOf(payload), flags = view.getUint16(4, false); validateCloseFlags(flags); if (!isZero(payload.subarray(6, 8))) throw new ProtocolError("invalid close reserved bytes"); return { reasonCode: view.getUint32(0, false), flags, drainTimeoutMillis: view.getUint32(8, false) }; }
function encodeGoAwayPayload(value: GoAway): Uint8Array { validateCloseFlags(value.flags); const message = encoder.encode(value.message), out = new Uint8Array(32 + message.length), view = viewOf(out); view.setUint32(0, value.reasonCode, false); view.setUint16(4, value.flags, false); view.setUint32(8, value.drainTimeoutMillis, false); view.setBigUint64(12, value.lastAcceptedRequestID, false); view.setBigUint64(20, value.lastAcceptedTransferID, false); view.setUint32(28, message.length, false); out.set(message, 32); return out; }
function decodeGoAwayPayload(payload: Uint8Array): GoAway { if (payload.length < 32) throw new ProtocolError("goaway payload too small"); const view = viewOf(payload), flags = view.getUint16(4, false), end = checkedEnd(payload, 32, view.getUint32(28, false)); validateCloseFlags(flags); if (end === undefined || end !== payload.length || !isZero(payload.subarray(6, 8))) throw new ProtocolError("invalid goaway payload"); return { reasonCode: view.getUint32(0, false), flags, drainTimeoutMillis: view.getUint32(8, false), lastAcceptedRequestID: view.getBigUint64(12, false), lastAcceptedTransferID: view.getBigUint64(20, false), message: decodeUTF8(payload.subarray(32)) }; }
function decodeErrorPayload(payload: Uint8Array): ErrorMessage { if (payload.length < 32) throw new ProtocolError("error payload too small"); const view = viewOf(payload), end = checkedEnd(payload, 32, view.getUint32(28, false)); if (end === undefined || end !== payload.length || !isZero(payload.subarray(5, 8))) throw new ProtocolError("invalid error payload"); return { code: view.getUint32(0, false), frameType: payload[4], schema: view.getUint32(8, false), requestID: view.getBigUint64(12, false), transferID: view.getBigUint64(20, false), message: decodeUTF8(payload.subarray(32)) }; }
function validateFrameEnvelope(frame: Frame): void {
  let schema = 0;
  switch (frame.type) {
    case FrameType.Data: schema = frame.transferID === 0n ? Schema.Text : 0; break;
    case FrameType.Ack: schema = Schema.Ack; break;
    case FrameType.Nack: schema = Schema.Nack; break;
    case FrameType.Window: schema = Schema.Window; break;
    case FrameType.Cancel:
    case FrameType.CancelAck: schema = Schema.Cancel; break;
    case FrameType.Hello:
    case FrameType.HelloAck: schema = Schema.Hello; break;
    case FrameType.TransferBegin: schema = Schema.TransferBegin; break;
    case FrameType.TransferState:
    case FrameType.TransferResume: schema = Schema.TransferState; break;
    case FrameType.Auth: schema = Schema.Auth; break;
    case FrameType.AuthAccept:
    case FrameType.AuthReject: schema = Schema.AuthResult; break;
    case FrameType.Request:
    case FrameType.Response: schema = Schema.Event; break;
    case FrameType.Error: schema = Schema.Error; break;
    case FrameType.GoAway: schema = Schema.GoAway; break;
    case FrameType.Close:
    case FrameType.CloseAck: schema = Schema.Close; break;
  }
  if (frame.schema !== schema) throw new ProtocolError(`invalid schema ${frame.schema} for ETP frame ${frame.type}`);
  const transferFrames: readonly number[] = [FrameType.TransferBegin, FrameType.TransferEnd, FrameType.TransferState, FrameType.TransferResume, FrameType.Ack, FrameType.Nack, FrameType.Cancel, FrameType.CancelAck];
  if (frame.transferID === 0n && transferFrames.includes(frame.type)) throw new ProtocolError("ETP transfer id is required");
  if (frame.payload.length === 0 && frame.type === FrameType.Data && frame.transferID !== 0n) throw new ProtocolError("ETP transfer data payload is empty");
}
function validateCloseFlags(flags: number): void { if (flags & ~knownCloseFlags || (flags & CloseFlag.Immediate && flags & CloseFlag.Drain)) throw new ProtocolError("invalid close flags"); }
function readLengthEnd(payload: Uint8Array, view: DataView, pos: number): { start: number; end: number; next: number } { if (pos + 4 > payload.length) throw new ProtocolError("invalid payload length"); const start = pos + 4, end = checkedEnd(payload, start, view.getUint32(pos, false)); if (end === undefined) throw new ProtocolError("invalid payload length"); return { start, end, next: end }; }
function checkedEnd(payload: Uint8Array, start: number, length: number): number | undefined { return start <= payload.length && length <= payload.length - start ? start + length : undefined; }
function viewOf(data: Uint8Array): DataView { return new DataView(data.buffer, data.byteOffset, data.byteLength); }
function decodeUTF8(data: Uint8Array): string { try { return decoder.decode(data); } catch { throw new ProtocolError("invalid UTF-8 payload"); } }
function isZero(data: Uint8Array): boolean { return data.every((value) => value === 0); }
