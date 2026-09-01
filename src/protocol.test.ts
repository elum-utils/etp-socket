import { describe, expect, it } from "vitest";

import { CloseFlag, decodeCancel, decodeCancelAck, decodeClose, decodeEvent, decodeFrame, decodeGoAway, decodeHello, decodeNack, decodeTransferBegin, decodeTransferResume, decodeTransferState, decodeWindow, encodeAck, encodeAuth, encodeCancel, encodeCancelAck, encodeClose, encodeGoAway, encodeHello, encodeNack, encodeRequest, encodeTransferBegin, encodeTransferResume, encodeTransferState, encodeWindow, FrameType, HeaderSize, ProtocolError, TransferStateFlag, WindowFlag } from "./protocol";

describe("ETP binary codec", () => {
  it("encodes a bearer auth frame", () => {
    const frame = decodeFrame(encodeAuth("token"));

    expect(frame.type).toBe(FrameType.Auth);
    expect(frame.payload.byteLength).toBe(17);
    expect(new DataView(frame.payload.buffer, frame.payload.byteOffset).getUint16(0, false)).toBe(1);
  });

  it("encodes a client hello frame", () => {
    const frame = decodeFrame(encodeHello());

    expect(frame.type).toBe(FrameType.Hello);
    expect(new TextDecoder().decode(frame.payload.subarray(88))).toBe("client");
    expect(decodeHello(frame).rateLimits.maxRequestsPerSecond).toBe(200);
  });

  it("round trips a JSON event request", () => {
    const frame = decodeFrame(encodeRequest(1001n, "message.send", { text: "Hello" }));

    expect(frame.type).toBe(FrameType.Request);
    expect(frame.requestID).toBe(1001n);
    expect(decodeEvent(frame)).toEqual({ event: "message.send", data: { text: "Hello" } });
  });

  it("matches Go golden encodings for event, ack, and transfer window payloads", () => {
    const event = new Uint8Array(encodeRequest(7n, "message.get", { id: 42 }));
    expect(hex(event.subarray(HeaderSize))).toBe("0000000b6d6573736167652e676574000000097b226964223a34327d");

    const ack = new Uint8Array(encodeAck({ transferID: 7n, chunkFrom: 1, chunkTo: 3, receivedBytes: 4096n }));
    expect(hex(ack.subarray(HeaderSize))).toBe("000000000000000700000001000000030000000000001000");

    const window = new Uint8Array(encodeWindow({ transferID: 7n, windowBytes: 65536n, windowChunks: 4, flags: WindowFlag.Transfer }));
    expect(hex(window.subarray(HeaderSize))).toBe("000000000000000700000000000100000000000400020000");
  });

  it("round trips every client control message", () => {
    expect(decodeNack(decodeFrame(encodeNack({ transferID: 7n, chunkFrom: 2, chunkTo: 4, reasonCode: 1, flags: 0 })))).toEqual({ transferID: 7n, chunkFrom: 2, chunkTo: 4, reasonCode: 1, flags: 0 });
    expect(decodeWindow(decodeFrame(encodeWindow({ transferID: 7n, windowBytes: 1024n, windowChunks: 3, flags: WindowFlag.Transfer })))).toEqual({ transferID: 7n, windowBytes: 1024n, windowChunks: 3, flags: WindowFlag.Transfer });
    expect(decodeCancel(decodeFrame(encodeCancel(7n, 1, 1)))).toEqual({ transferID: 7n, reasonCode: 1, flags: 1 });
    expect(decodeCancelAck(decodeFrame(encodeCancelAck(7n, 1)))).toEqual({ transferID: 7n, status: 1 });
    expect(decodeClose(decodeFrame(encodeClose({ reasonCode: 0, flags: CloseFlag.Drain, drainTimeoutMillis: 5000 })))).toEqual({ reasonCode: 0, flags: CloseFlag.Drain, drainTimeoutMillis: 5000 });
    const goAway = { reasonCode: 7, flags: CloseFlag.Immediate, drainTimeoutMillis: 0, lastAcceptedRequestID: 12n, lastAcceptedTransferID: 13n, message: "shutdown" };
    expect(decodeGoAway(decodeFrame(encodeGoAway(goAway)))).toEqual(goAway);
    const state = { transferID: 7n, receivedBytes: 100n, nextChunk: 2, flags: TransferStateFlag.ResumeAccepted, reasonCode: 0 };
    expect(decodeTransferState(decodeFrame(encodeTransferState(state)))).toEqual(state);
    const resume = { transferID: 7n, receivedBytes: 100n, nextChunk: 2, token: new Uint8Array([1, 2, 3]) };
    expect(decodeTransferResume(decodeFrame(encodeTransferResume(9n, resume)))).toEqual(resume);
  });

  it("round trips multipart transfer metadata", () => {
    const begin = { totalSize: 5n, chunkSize: 5, chunkCount: 1, contentType: 1, flags: 0, checksum: new Uint8Array(32), name: "", event: "attach.upload", field: "", index: 0, parts: [{ field: "files", index: 0, name: "a.bin", totalSize: 5n, contentType: 1 }], fields: [{ key: "dialog", value: "123" }] };
    expect(decodeTransferBegin(decodeFrame(encodeTransferBegin(7n, 8n, begin)))).toEqual(begin);
  });

  it("rejects frames with a corrupted header or payload length", () => {
    const frame = new Uint8Array(encodeRequest(1n, "message.send", {}));
    frame[0] = 2;
    expect(() => decodeFrame(frame.buffer)).toThrow(ProtocolError);

    frame[0] = 1;
    new DataView(frame.buffer).setUint32(12, 1, false);
    expect(() => decodeFrame(frame.buffer)).toThrow("payload length");

    expect(() => decodeFrame(new ArrayBuffer(HeaderSize - 1))).toThrow("frame length");
  });

  it("rejects invalid event and hello payloads", () => {
    const event = decodeFrame(encodeRequest(1n, "message.send", { text: "Hello" }));
    const eventPayload = new Uint8Array(event.payload);
    new DataView(eventPayload.buffer).setUint32(0, 0xffff_ffff, false);
    expect(() => decodeEvent({ ...event, payload: eventPayload })).toThrow("event name");

    const hello = decodeFrame(encodeHello());
    const helloPayload = new Uint8Array(hello.payload);
    helloPayload[80] = 1;
    expect(() => decodeHello({ ...hello, payload: helloPayload })).toThrow("reserved");

	const zeroRate = decodeFrame(encodeHello());
	const zeroRatePayload = new Uint8Array(zeroRate.payload);
	new DataView(zeroRatePayload.buffer).setUint32(32, 0, false);
	expect(() => decodeHello({ ...zeroRate, payload: zeroRatePayload })).toThrow("hello limits");
  });

  it("rejects mismatched envelopes and invalid control state", () => {
    const wrongSchema = new Uint8Array(encodeRequest(1n, "event", {}));
    new DataView(wrongSchema.buffer).setUint32(16, 999, false);
    expect(() => decodeFrame(wrongSchema.buffer)).toThrow("invalid schema");

    const missingTransfer = new Uint8Array(encodeAck({ transferID: 7n, chunkFrom: 0, chunkTo: 0, receivedBytes: 0n }));
    new DataView(missingTransfer.buffer).setBigUint64(28, 0n, false);
    expect(() => decodeFrame(missingTransfer.buffer)).toThrow("transfer id");

    const invalidState = decodeFrame(encodeTransferState({ transferID: 7n, receivedBytes: 0n, nextChunk: 0, flags: TransferStateFlag.Completed | TransferStateFlag.Failed, reasonCode: 1 }));
    expect(() => decodeTransferState(invalidState)).toThrow("invalid transfer state");
    expect(() => encodeClose({ reasonCode: 0, flags: CloseFlag.Immediate | CloseFlag.Drain, drainTimeoutMillis: 0 })).toThrow("invalid close flags");
  });
});

function hex(data: Uint8Array): string {
  return Array.from(data, (value) => value.toString(16).padStart(2, "0")).join("");
}
