import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { decodeCancelAck, decodeEvent, decodeFrame, decodeTransferResume, decodeTransferState, encodeAck, encodeCancel, encodeClose, encodeData, encodeGoAway, encodeNack, encodeRequest, encodeTransferBegin, encodeTransferEnd, encodeTransferResume, encodeTransferState, encodeWindow, CloseFlag, FrameFlag, FrameType, HeaderSize, Schema, TransferFlag, TransferStateFlag, WindowFlag } from "./protocol";
import { SocketState } from "./types";

type WorkerOutput = Record<string, unknown>;

class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  binaryType = "blob";
  readyState = FakeWebSocket.CONNECTING;
  bufferedAmount = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  readonly sent: ArrayBuffer[] = [];

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  send(data: ArrayBuffer): void {
    this.sent.push(data);
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.();
  }

  receive(data: ArrayBuffer): void {
    this.onmessage?.({ data } as MessageEvent<ArrayBuffer>);
  }

  close(): void {
    if (this.readyState === FakeWebSocket.CLOSED) {
      return;
    }
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.();
  }
}

class FakeMessagePort {
  peer?: FakeMessagePort;
  onmessage: ((event: MessageEvent<unknown>) => void) | null = null;

  postMessage(data: unknown): void {
    setTimeout(() => this.peer?.onmessage?.({ data } as MessageEvent<unknown>), 0);
  }
}

class FakeMessageChannel {
  readonly port1 = new FakeMessagePort();
  readonly port2 = new FakeMessagePort();

  constructor() {
    this.port1.peer = this.port2;
    this.port2.peer = this.port1;
  }
}

type Scope = {
  onmessage: ((event: MessageEvent<unknown>) => void) | null;
  postMessage(message: WorkerOutput): void;
  close(): void;
};

describe("ETP socket worker", () => {
  let output: WorkerOutput[];
  let scope: Scope;

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    FakeWebSocket.instances = [];
    output = [];
    scope = {
      onmessage: null,
      postMessage: (message) => output.push(message),
      close: vi.fn(),
    };
    vi.stubGlobal("self", scope);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    vi.stubGlobal("MessageChannel", FakeMessageChannel);
    vi.resetModules();
    await import("./worker");
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("authenticates, performs hello handshake, sends a request and resolves its response", () => {
    connect();
    const authRequest = output.find((message) => message.type === "auth") as { epoch: number };
    dispatch({ type: "auth", epoch: authRequest.epoch, token: "token" });

    const connection = latestSocket();
    expect(connection.binaryType).toBe("arraybuffer");
    connection.open();
    expect(decodeFrame(connection.sent[0]).type).toBe(FrameType.Auth);

    connection.receive(serverAuthAccept("account-1"));
    expect(output).toContainEqual({ type: "identity", identity: { userID: "account-1" } });
    expect(decodeFrame(connection.sent[1]).type).toBe(FrameType.Hello);

    connection.receive(serverHello("server"));
    expect(output).toContainEqual({ type: "status", state: SocketState.Open });

    dispatch({ type: "emit", callID: 7, event: "message.send", data: { text: "Hello" } });
    const request = decodeFrame(connection.sent[2]);
    expect(request.type).toBe(FrameType.Request);
    expect(decodeEvent(request)).toEqual({ event: "message.send", data: { text: "Hello" } });

    connection.receive(serverResponse(request.requestID, "message.sent", { id: "message-1" }));
    expect(output).toContainEqual({ type: "response", callID: 7, data: { id: "message-1" }, error: undefined });
  });

  it("delivers a server request and sends its callback response with the same request id", () => {
    const connection = openConnection();
    connection.receive(encodeRequest(77n, "client.confirm", { value: 1 }, [{ key: "attempt", value: "2" }]));
    expect(output).toContainEqual({ type: "event", event: "client.confirm", data: { value: 1, attempt: 2 }, requestID: 77n });

    dispatch({ type: "respond", requestID: 77n, event: "client.confirm", data: { accepted: true } });
    const response = decodeFrame(connection.sent.at(-1)!);
    expect(response.type).toBe(FrameType.Response);
    expect(response.requestID).toBe(77n);
    expect(decodeEvent(response).data).toEqual({ accepted: true });
  });

  it("delivers standalone text frames", () => {
    const connection = openConnection();
    const text = new TextEncoder().encode("hello"), payload = new Uint8Array(4 + text.length);
    new DataView(payload.buffer).setUint32(0, text.length, false);
    payload.set(text, 4);
    connection.receive(serverFrame(FrameType.Data, payload, Schema.Text, 5n, FrameFlag.First | FrameFlag.Last));
    expect(output).toContainEqual({ type: "text", text: "hello" });
  });

  it("reports when the incoming frame rate limit is exceeded", () => {
    const connection = openConnection();
    vi.advanceTimersByTime(1_000);
    for (let index = 0; index <= 2_000; index += 1) {
      connection.receive(encodeWindow({ transferID: 1n, windowBytes: 1n, windowChunks: 1, flags: WindowFlag.Transfer }));
    }
    expect(output).toContainEqual({ type: "error", error: { code: "protocol", message: "incoming ETP rate limit exceeded: frames" } });
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);
  });

  it("enforces the request limit it advertises to the server", () => {
    const connection = openConnection(0x37ffn, 10_000, undefined, 1);
    connection.receive(encodeRequest(9001n, "server.first", {}));
    expect(output).toContainEqual({ type: "event", event: "server.first", data: {}, requestID: 9001n });
    connection.receive(encodeRequest(9002n, "server.second", {}));
    expect(output).toContainEqual({ type: "error", error: { code: "protocol", message: "incoming ETP rate limit exceeded: requests" } });
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);
  });

  it("answers ping and sends an idle heartbeat from the worker", () => {
    const connection = openConnection();
    connection.receive(serverFrame(FrameType.Ping));
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.Pong);

    vi.advanceTimersByTime(10_000);
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.Ping);
  });

  it("rejects auth and does not reconnect", () => {
    connect();
    const authRequest = output.find((message) => message.type === "auth") as { epoch: number };
    dispatch({ type: "auth", epoch: authRequest.epoch, token: "token" });
    const connection = latestSocket();
    connection.open();
    connection.receive(serverAuthReject("invalid token"));

    expect(output).toContainEqual({ type: "status", state: SocketState.Unauthorized });
    expect(output).toContainEqual({ type: "error", error: { code: "auth", message: "invalid token" } });
    vi.advanceTimersByTime(60_000);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("rejects pending requests inside the worker on timeout and on disconnect", () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 1, event: "message.send", data: {} });
    vi.advanceTimersByTime(10_000);
    expect(output).toContainEqual({
      type: "response",
      callID: 1,
      data: undefined,
      error: { code: "timeout", message: "request timed out" },
    });

    dispatch({ type: "emit", callID: 2, event: "message.send", data: {} });
    connection.close();
    expect(output).toContainEqual({
      type: "response",
      callID: 2,
      data: undefined,
      error: { code: "closed", message: "connection closed before a response was received" },
    });
  });

  it("rejects an emit before the ETP handshake is established", () => {
    connect();
    dispatch({ type: "emit", callID: 9, event: "message.send", data: {} });

    expect(output).toContainEqual({
      type: "response",
      callID: 9,
      data: undefined,
      error: { code: "closed", message: "socket is not connected" },
    });
  });

  it("reconnects after a network close and requests a fresh auth token", () => {
    const connection = openConnection();
    connection.close();
    expect(output).toContainEqual({ type: "disconnect", reason: "connection" });

    vi.advanceTimersByTime(1_000);
    const authRequests = output.filter((message) => message.type === "auth") as Array<{ epoch: number }>;
    expect(authRequests).toHaveLength(2);
    dispatch({ type: "auth", epoch: authRequests[1].epoch, token: "refreshed-token" });
    expect(FakeWebSocket.instances).toHaveLength(2);
  });

  it("closes a handshake that never completes and retries as a connection failure", () => {
    connect();
    const authRequest = output.find((message) => message.type === "auth") as { epoch: number };
    dispatch({ type: "auth", epoch: authRequest.epoch, token: "token" });
    latestSocket().open();

    vi.advanceTimersByTime(10_000);
    expect(output).toContainEqual({ type: "error", error: { code: "timeout", message: "ETP authentication or handshake timed out" } });
    vi.advanceTimersByTime(1_000);
    expect(output.filter((message) => message.type === "auth")).toHaveLength(2);
  });

  it("handles manual disconnect before a socket is created and terminates pending work", () => {
    connect();
    dispatch({ type: "disconnect" });
    expect(output).toContainEqual({ type: "disconnect", reason: "client" });

    const connection = openConnection();
    dispatch({ type: "emit", callID: 4, event: "message.send", data: {} });
    dispatch({ type: "terminate" });
    expect(output).toContainEqual({
      type: "response",
      callID: 4,
      data: undefined,
      error: { code: "terminated", message: "socket worker is terminated" },
    });
    expect(output).toContainEqual({ type: "status", state: SocketState.Terminated });
    expect(scope.close).toHaveBeenCalledOnce();
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);
  });

  it("maps FrameError to its pending request and surfaces unknown request errors", () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 5, event: "message.send", data: {} });
    const request = decodeFrame(connection.sent.at(-1)!);
    connection.receive(serverError(request.requestID, "server returned an ETP error"));
    expect(output).toContainEqual({
      type: "response",
      callID: 5,
      data: undefined,
      error: { code: "protocol", message: "server returned an ETP error" },
    });

    connection.receive(serverError(999n, "server returned an ETP error"));
    expect(output).toContainEqual({ type: "error", error: { code: "protocol", message: "server returned an ETP error" } });
  });

  it("rejects an invalid hello without reconnecting a protocol failure", () => {
    connect();
    const authRequest = output.find((message) => message.type === "auth") as { epoch: number };
    dispatch({ type: "auth", epoch: authRequest.epoch, token: "token" });
    const connection = latestSocket();
    connection.open();
    connection.receive(serverAuthAccept("account-1"));
    connection.receive(serverHello("client"));

    expect(output).toContainEqual({ type: "error", error: { code: "protocol", message: "unexpected ETP hello role" } });
    vi.advanceTimersByTime(60_000);
    expect(output.filter((message) => message.type === "auth")).toHaveLength(1);
  });

  it("sends a large request through receiver window, ACKs, and transfer commit", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 21, event: "message.large", data: { text: "x".repeat(70 << 10) } });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = decodeFrame(connection.sent.find((value) => decodeFrame(value).type === FrameType.TransferBegin)!);
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 1n << 20n, windowChunks: 16, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(20);
    const chunks = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data);
    expect(chunks.length).toBeGreaterThan(1);
    let received = 0;
    for (const chunk of chunks) { received += chunk.payload.length; connection.receive(encodeAck({ transferID: begin.transferID, chunkFrom: chunk.chunkID, chunkTo: chunk.chunkID, receivedBytes: BigInt(received) })); }
    await vi.advanceTimersByTimeAsync(1);
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.TransferEnd);
    connection.receive(encodeTransferState({ transferID: begin.transferID, receivedBytes: BigInt(70 << 10), nextChunk: chunks.length, flags: TransferStateFlag.Completed, reasonCode: 0 }));
  });

  it("does not exceed max in-flight chunks when receiver windows arrive before ACKs", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 22, event: "message.large", data: new Uint8Array(512 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    const window = encodeWindow({ transferID: begin.transferID, windowBytes: 1n << 20n, windowChunks: 16, flags: WindowFlag.Transfer });

    connection.receive(window);
    connection.receive(window);
    await vi.advanceTimersByTimeAsync(20);

    const chunks = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data);
    expect(chunks).toHaveLength(16);
    expect(chunks.filter((frame) => frame.flags & FrameFlag.AckRequest)).toHaveLength(2);
  });

  it("sends realtime requests before queued bulk chunks", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 23, event: "upload", data: new Uint8Array(512 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 1n << 20n, windowChunks: 16, flags: WindowFlag.Transfer }));

    await vi.advanceTimersToNextTimerAsync();
    const firstData = connection.sent.map(decodeFrame).findIndex((frame) => frame.type === FrameType.Data);
    expect(firstData).toBeGreaterThanOrEqual(0);

    connection.bufferedAmount = 256 << 10;
    dispatch({ type: "emit", callID: 24, event: "message.send", data: { text: "priority" } });
    await vi.advanceTimersByTimeAsync(20);
    let frames = connection.sent.map(decodeFrame);
    expect(frames.filter((frame) => frame.type === FrameType.Data)).toHaveLength(8);
    connection.bufferedAmount = 0;
    await vi.advanceTimersByTimeAsync(20);
    frames = connection.sent.map(decodeFrame);
    const request = frames.findIndex((frame) => frame.type === FrameType.Request);
    const data = frames.map((frame, index) => frame.type === FrameType.Data ? index : -1).filter((index) => index >= 0);
    expect(request).toBeGreaterThan(firstData);
    expect(request).toBeLessThan(data[8]);
  });

  it("paces requests using server limits and starts timeout only after the frame is sent", async () => {
    const limits = { requests: 1, requestBurst: 1, frames: 100, frameBurst: 100, bytes: 1n << 20n, byteBurst: 1n << 20n };
    const connection = openConnection(0x37ffn, 500, limits);
    dispatch({ type: "emit", callID: 101, event: "first", data: {} });
    dispatch({ type: "emit", callID: 102, event: "second", data: {} });

    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(600);
    expect(output.some((message) => message.type === "response" && message.callID === 102)).toBe(false);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(400);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(499);
    expect(output.some((message) => message.type === "response" && message.callID === 102)).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(output).toContainEqual({ type: "response", callID: 102, data: undefined, error: { code: "timeout", message: "request timed out" } });
  });

  it("starts pacing from the server-advertised remaining quota", async () => {
    const limits = {
      requests: 10,
      requestBurst: 10,
      frames: 100,
      frameBurst: 100,
      bytes: 1n << 20n,
      byteBurst: 1n << 20n,
      availableRequests: 0,
      availableFrames: 100,
      availableBytes: 1n << 20n,
    };
    const connection = openConnection(0x37ffn, 10_000, limits);
    dispatch({ type: "emit", callID: 105, event: "message.send", data: {} });

    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(99);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Request)).toHaveLength(1);
  });

  it("gives a realtime request the next frame token ahead of queued file chunks", async () => {
    const limits = { requests: 100, requestBurst: 100, frames: 2, frameBurst: 2, bytes: 1n << 20n, byteBurst: 1n << 20n };
    const connection = openConnection(0x37ffn, 10_000, limits);
    dispatch({ type: "emit", callID: 103, event: "upload", data: new Uint8Array(64 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 64n << 10n, windowChunks: 4, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(1);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data)).toHaveLength(1);

    dispatch({ type: "emit", callID: 104, event: "message.send", data: { text: "priority" } });
    await vi.advanceTimersByTimeAsync(499);
    const frames = connection.sent.map(decodeFrame);
    const requestIndex = frames.findIndex((frame) => frame.type === FrameType.Request);
    const dataIndexes = frames.map((frame, index) => frame.type === FrameType.Data ? index : -1).filter((index) => index >= 0);
    expect(requestIndex).toBeGreaterThan(dataIndexes[0]);
    expect(dataIndexes).toHaveLength(1);
  });

  it("coalesces chunk progress without delaying the exact terminal update", async () => {
    const connection = openConnection();
    const total = 512 << 10;
    dispatch({ type: "emit", callID: 41, event: "upload", data: new Uint8Array(total) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 1n << 20n, windowChunks: 64, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(20);

    let acknowledgedChunks = 0, receivedBytes = 0;
    for (let batch = 0; batch < 4 && !connection.sent.map(decodeFrame).some((frame) => frame.type === FrameType.TransferEnd); batch += 1) {
      const chunks = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data);
      const unacknowledged = chunks.slice(acknowledgedChunks);
      expect(unacknowledged.length).toBeGreaterThan(0);
      receivedBytes += unacknowledged.reduce((sum, frame) => sum + frame.payload.byteLength, 0);
      acknowledgedChunks = chunks.length;
      connection.receive(encodeAck({ transferID: begin.transferID, chunkFrom: unacknowledged[0].chunkID, chunkTo: unacknowledged.at(-1)!.chunkID, receivedBytes: BigInt(receivedBytes) }));
      await vi.advanceTimersByTimeAsync(20);
    }
    expect(connection.sent.map(decodeFrame).some((frame) => frame.type === FrameType.TransferEnd)).toBe(true);
    connection.receive(encodeTransferState({ transferID: begin.transferID, receivedBytes: BigInt(receivedBytes), nextChunk: acknowledgedChunks, flags: TransferStateFlag.Completed, reasonCode: 0 }));

    const progress = output.filter((message) => message.type === "progress").map((message) => message.progress as { acknowledgedBytes: number; state: string });
    expect(progress.length).toBeLessThan(acknowledgedChunks);
    expect(progress.at(-1)).toMatchObject({ acknowledgedBytes: receivedBytes, state: "completed" });
  });

  it("drops queued chunks when an outgoing request is canceled", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 25, event: "upload", data: new Uint8Array(512 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 1n << 20n, windowChunks: 16, flags: WindowFlag.Transfer }));
    dispatch({ type: "cancel", callID: 25 });
    await vi.advanceTimersByTimeAsync(20);

    const frames = connection.sent.map(decodeFrame);
    expect(frames.filter((frame) => frame.type === FrameType.Data)).toHaveLength(0);
    expect(frames.at(-1)?.type).toBe(FrameType.Cancel);
  });

  it("does not start ACK timeouts while bulk frames wait for WebSocket backpressure", async () => {
    const connection = openConnection(0x37ffn, 20_000);
    dispatch({ type: "emit", callID: 26, event: "upload", data: new Uint8Array(16 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((value) => decodeFrame(value).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.bufferedAmount = 256 << 10;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 16n << 10n, windowChunks: 1, flags: WindowFlag.Transfer }));
    for (let second = 0; second < 7; second += 1) await vi.advanceTimersByTimeAsync(1_000);

    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data)).toHaveLength(0);
    expect(output.some((message) => message.type === "response" && message.callID === 26)).toBe(false);
    connection.bufferedAmount = 0;
    await vi.advanceTimersByTimeAsync(2);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data)).toHaveLength(1);
  });

  it("batches ACK and WINDOW frames while receiving a transfer", () => {
    const connection = openConnection(), transferID = 8000n, requestID = 9000n;
    connection.receive(encodeTransferBegin(transferID, requestID, { totalSize: 64n, chunkSize: 4, chunkCount: 16, contentType: 1, flags: 0, checksum: new Uint8Array(32), name: "x.bin", event: "attach.ready", field: "", index: 0, parts: [], fields: [] }));
    for (let chunkID = 0; chunkID < 16; chunkID += 1) {
      const last = chunkID === 15;
      connection.receive(encodeData(transferID, requestID, chunkID, new Uint8Array(4), chunkID === 0, last, chunkID === 7 || last));
    }
    const frames = connection.sent.map(decodeFrame);
    expect(frames.filter((frame) => frame.type === FrameType.Ack)).toHaveLength(2);
    expect(frames.filter((frame) => frame.type === FrameType.Window)).toHaveLength(2);
  });

  it("receives and verifies a checksum transfer as a Blob event", async () => {
    const connection = openConnection(), body = new TextEncoder().encode("binary-response");
    const checksum = new Uint8Array(await crypto.subtle.digest("SHA-256", body));
    const transferID = 8001n, requestID = 9001n;
    connection.receive(encodeTransferBegin(transferID, requestID, { totalSize: BigInt(body.length), chunkSize: body.length, chunkCount: 1, contentType: 1, flags: TransferFlag.ChecksumSHA256, checksum, name: "x.bin", event: "attach.ready", field: "", index: 0, parts: [], fields: [] }));
    connection.receive(encodeData(transferID, requestID, 0, body, true, true));
    connection.receive(encodeTransferEnd(transferID, requestID));
    await vi.waitFor(() => expect(output.some((message) => message.type === "event" && message.event === "attach.ready")).toBe(true));
    const event = output.find((message) => message.type === "event" && message.event === "attach.ready") as { data: Blob };
    expect(await event.data.text()).toBe("binary-response");
    expect(connection.sent.map(decodeFrame).some((frame) => frame.type === FrameType.TransferState)).toBe(true);

    const statesBefore = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.TransferState).length;
    connection.receive(encodeTransferEnd(transferID, requestID));
    const statesAfter = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.TransferState).length;
    expect(statesAfter).toBe(statesBefore + 1);

    const json = new TextEncoder().encode(`{"ok":true}`);
    connection.receive(encodeTransferBegin(8002n, 9002n, { totalSize: BigInt(json.length), chunkSize: json.length, chunkCount: 1, contentType: 1, flags: 0, checksum: new Uint8Array(32), name: "", event: "json.ready", field: "", index: 0, parts: [], fields: [] }));
    connection.receive(encodeData(8002n, 9002n, 0, json, true, true));
    connection.receive(encodeTransferEnd(8002n, 9002n));
    await vi.waitFor(() => expect(output).toContainEqual({ type: "event", event: "json.ready", data: { ok: true }, requestID: 9002n }));
  });

  it("handles remote cancellation and returns the correct cancel status", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 31, event: "upload", data: new Uint8Array(32 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((item) => decodeFrame(item).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeCancel(begin.transferID, 4));

    expect(output).toContainEqual({ type: "response", callID: 31, data: undefined, error: { code: "closed", message: "transfer was canceled by the server" } });
    const ack = decodeFrame(connection.sent.at(-1)!);
    expect(ack.type).toBe(FrameType.CancelAck);
    expect(decodeCancelAck(ack).status).toBe(1);

    connection.receive(encodeCancel(999n, 4));
    expect(decodeCancelAck(decodeFrame(connection.sent.at(-1)!)).status).toBe(2);
  });

  it("retries missing chunks, pauses on flow control, and fails rejected transfers", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 32, event: "upload", data: new Uint8Array(32 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((item) => decodeFrame(item).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 32n << 10n, windowChunks: 2, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(2);
    const before = connection.sent.length;
    connection.receive(encodeNack({ transferID: begin.transferID, chunkFrom: 0, chunkTo: 0, reasonCode: 1, flags: 0 }));
    await vi.advanceTimersByTimeAsync(1);
    expect(connection.sent.length).toBe(before + 1);
    connection.receive(encodeNack({ transferID: begin.transferID, chunkFrom: 0, chunkTo: 0, reasonCode: 7, flags: 0 }));
    connection.receive(encodeNack({ transferID: begin.transferID, chunkFrom: 0, chunkTo: 0, reasonCode: 5, flags: 0 }));
    expect(output).toContainEqual({ type: "response", callID: 32, data: undefined, error: { code: "protocol", message: "remote rejected transfer: receiver write failed (reason 5)" } });
  });

  it("resumes an outgoing transfer after reconnect", async () => {
    const connection = openConnection(0x3fffn);
    dispatch({ type: "emit", callID: 33, event: "upload", data: new Uint8Array(32 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((item) => decodeFrame(item).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 16n << 10n, windowChunks: 1, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(1);
    const chunk = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.Data)!;
    connection.receive(encodeAck({ transferID: begin.transferID, chunkFrom: 0, chunkTo: 0, receivedBytes: BigInt(chunk.payload.length) }));
    connection.close();
    vi.advanceTimersByTime(1_000);
    const auth = output.filter((message) => message.type === "auth").at(-1) as { epoch: number };
    dispatch({ type: "auth", epoch: auth.epoch, token: "token-2" });
    const resumed = latestSocket(); resumed.open(); resumed.receive(serverAuthAccept("account-1")); resumed.receive(serverHello("server", 0x3fffn));
    const resumeFrame = resumed.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferResume)!;
    const resume = decodeTransferResume(resumeFrame);
    expect(resume.transferID).toBe(begin.transferID);
    expect(resume.receivedBytes).toBe(BigInt(chunk.payload.length));
    resumed.receive(encodeTransferState({ transferID: begin.transferID, receivedBytes: resume.receivedBytes, nextChunk: 1, flags: TransferStateFlag.ResumeAccepted, reasonCode: 0 }));
    resumed.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 16n << 10n, windowChunks: 1, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(1);
    expect(resumed.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data)).toHaveLength(1);
  });

  it("performs graceful peer close and immediate goaway", () => {
    const connection = openConnection();
    connection.receive(encodeClose({ reasonCode: 0, flags: CloseFlag.Drain, drainTimeoutMillis: 100 }));
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.CloseAck);
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);

    const second = openConnection();
    second.receive(encodeGoAway({ reasonCode: 1, flags: CloseFlag.Immediate, drainTimeoutMillis: 0, lastAcceptedRequestID: 0n, lastAcceptedTransferID: 0n, message: "shutdown" }));
    expect(second.readyState).toBe(FakeWebSocket.CLOSED);
  });

  it("times out a silent established connection", () => {
    const connection = openConnection();
    vi.advanceTimersByTime(21_000);
    expect(output).toContainEqual({ type: "error", error: { code: "timeout", message: "ETP heartbeat timed out" } });
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);
  });

  it("retries unacknowledged chunks and fails at the configured retry limit", async () => {
    const connection = openConnection(0x37ffn, 20_000);
    dispatch({ type: "emit", callID: 34, event: "upload", data: new Uint8Array(16 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((item) => decodeFrame(item).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeWindow({ transferID: begin.transferID, windowBytes: 16n << 10n, windowChunks: 1, flags: WindowFlag.Transfer }));
    await vi.advanceTimersByTimeAsync(1);
    for (let second = 0; second < 12; second += 1) await vi.advanceTimersByTimeAsync(1_000);
    expect(connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.Data)).toHaveLength(4);
    expect(output).toContainEqual({ type: "response", callID: 34, data: undefined, error: { code: "timeout", message: "transfer acknowledgment timed out" } });
  });

  it("fails checksum-corrupt downloads and resumes a partial incoming transfer", async () => {
    const connection = openConnection(0x3fffn), transferID = 8100n, requestID = 9100n, body = new TextEncoder().encode("chunk");
    connection.receive(encodeTransferBegin(transferID, requestID, { totalSize: BigInt(body.length), chunkSize: body.length, chunkCount: 1, contentType: 1, flags: TransferFlag.ChecksumSHA256, checksum: new Uint8Array(32), name: "x", event: "download", field: "", index: 0, parts: [], fields: [] }));
    connection.receive(encodeData(transferID, requestID, 0, body, true, true));
    connection.receive(encodeTransferEnd(transferID, requestID));
    await vi.waitFor(() => expect(output.some((message) => message.type === "protocol" && (message.event as { code?: string }).code === "checksum_mismatch")).toBe(true));

    const partialID = 8101n;
    connection.receive(encodeTransferBegin(partialID, 9101n, { totalSize: 10n, chunkSize: 5, chunkCount: 2, contentType: 1, flags: 0, checksum: new Uint8Array(32), name: "x", event: "download", field: "", index: 0, parts: [], fields: [] }));
    connection.receive(encodeData(partialID, 9101n, 0, new Uint8Array(5), true, false));
    connection.receive(encodeTransferResume(9101n, { transferID: partialID, receivedBytes: 5n, nextChunk: 1, token: new Uint8Array() }));
    const state = connection.sent.map(decodeFrame).filter((frame) => frame.type === FrameType.TransferState).at(-1)!;
    expect(decodeTransferState(state).flags).toBe(TransferStateFlag.ResumeAccepted);
  });

  it("rejects remote transfer failures and oversized incoming transfers", async () => {
    const connection = openConnection();
    dispatch({ type: "emit", callID: 35, event: "upload", data: new Uint8Array(16 << 10) });
    await vi.waitFor(() => expect(connection.sent.some((item) => decodeFrame(item).type === FrameType.TransferBegin)).toBe(true));
    const begin = connection.sent.map(decodeFrame).find((frame) => frame.type === FrameType.TransferBegin)!;
    connection.receive(encodeTransferState({ transferID: begin.transferID, receivedBytes: 0n, nextChunk: 0, flags: TransferStateFlag.Failed, reasonCode: 5 }));
    expect(output).toContainEqual({ type: "response", callID: 35, data: undefined, error: { code: "protocol", message: "transfer failed with reason 5" } });

    connection.receive(encodeTransferBegin(8200n, 9200n, { totalSize: 65n << 20n, chunkSize: 16 << 10, chunkCount: 4160, contentType: 1, flags: 0, checksum: new Uint8Array(32), name: "x", event: "too.large", field: "", index: 0, parts: [], fields: [] }));
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.Nack);
  });

  it("uses protocol close on manual disconnect and handles its acknowledgment", () => {
    const connection = openConnection();
    dispatch({ type: "disconnect" });
    expect(decodeFrame(connection.sent.at(-1)!).type).toBe(FrameType.Close);
    connection.receive(encodeClose({ reasonCode: 4, flags: CloseFlag.Immediate, drainTimeoutMillis: 0 }, true));
    expect(connection.readyState).toBe(FakeWebSocket.CLOSED);
  });

  function connect(timeout = 10_000, maxRequestsPerSecond = 200): void {
    dispatch({
      type: "configure",
      config: {
        url: "wss://example.test/ws",
        timeout,
        reconnection: { enabled: true, attempts: 5, delay: 1_000, maxDelay: 10_000 },
        protocol: { chunkSize: 16 << 10, maxTransferBytes: 64 << 20, maxConcurrentTransfers: 16, maxInFlightChunks: 16, heartbeatInterval: 10_000, heartbeatTimeout: 20_000, ackTimeout: 2_000, retryLimit: 3, maxRequestsPerSecond, maxFramesPerSecond: 2_000, maxBytesPerSecond: 64 << 20, checksum: true, resumeToken: new Uint8Array() },
      },
    });
    dispatch({ type: "connect" });
  }

  function openConnection(capabilities = 0x37ffn, timeout = 10_000, limits?: ServerLimits, maxRequestsPerSecond = 200): FakeWebSocket {
    connect(timeout, maxRequestsPerSecond);
    const authRequest = output.filter((message) => message.type === "auth").at(-1) as { epoch: number };
    dispatch({ type: "auth", epoch: authRequest.epoch, token: "token" });
    const connection = latestSocket();
    connection.open();
    connection.receive(serverAuthAccept("account-1"));
    connection.receive(serverHello("server", capabilities, limits));
    return connection;
  }

  function latestSocket(): FakeWebSocket {
    const connection = FakeWebSocket.instances.at(-1);
    if (!connection) {
      throw new Error("missing fake WebSocket");
    }
    return connection;
  }

  function dispatch(message: unknown): void {
    scope.onmessage?.({ data: message } as MessageEvent<unknown>);
  }
});

function serverAuthAccept(userID: string): ArrayBuffer {
  const user = new TextEncoder().encode(userID);
  const payload = new Uint8Array(4 + user.length);
  new DataView(payload.buffer).setUint32(0, user.length, false);
  payload.set(user, 4);
  return serverFrame(FrameType.AuthAccept, payload, 205);
}

function serverAuthReject(message: string): ArrayBuffer {
  const text = new TextEncoder().encode(message);
  const payload = new Uint8Array(8 + text.length);
  const view = new DataView(payload.buffer);
  view.setUint16(0, 401, false);
  view.setUint16(2, 401, false);
  view.setUint32(4, text.length, false);
  payload.set(text, 8);
  return serverFrame(FrameType.AuthReject, payload, 205);
}

type ServerLimits = {
  requests: number;
  requestBurst: number;
  frames: number;
  frameBurst: number;
  bytes: bigint;
  byteBurst: bigint;
  availableRequests?: number;
  availableFrames?: number;
  availableBytes?: bigint;
};

function serverHello(role: string, capabilities = 0x37ffn, limits: ServerLimits = { requests: 200, requestBurst: 200, frames: 2_000, frameBurst: 2_000, bytes: 64n << 20n, byteBurst: 64n << 20n }): ArrayBuffer {
  const roleBytes = new TextEncoder().encode(role);
  const payload = new Uint8Array(88 + roleBytes.length);
  const view = new DataView(payload.buffer);
  view.setBigUint64(0, capabilities, false);
  view.setUint32(8, 8 << 20, false);
  view.setUint32(12, 64 << 10, false);
  view.setBigUint64(16, 512n << 20n, false);
  view.setUint32(24, 16, false);
  view.setUint32(28, 10_000, false);
  view.setUint32(32, limits.requests, false);
  view.setUint32(36, limits.requestBurst, false);
  view.setUint32(40, limits.frames, false);
  view.setUint32(44, limits.frameBurst, false);
  view.setBigUint64(48, limits.bytes, false);
  view.setBigUint64(56, limits.byteBurst, false);
  view.setUint32(64, limits.availableRequests ?? limits.requestBurst, false);
  view.setUint32(68, limits.availableFrames ?? limits.frameBurst, false);
  view.setBigUint64(72, limits.availableBytes ?? limits.byteBurst, false);
  view.setUint32(84, roleBytes.length, false);
  payload.set(roleBytes, 88);
  return serverFrame(FrameType.HelloAck, payload, 1);
}

function serverResponse(requestID: bigint, event: string, data: unknown): ArrayBuffer {
  const response = new Uint8Array(encodeRequest(requestID, event, data));
  response[1] = FrameType.Response;
  return response.buffer;
}

function serverError(requestID: bigint, message: string): ArrayBuffer {
  const text = new TextEncoder().encode(message), payload = new Uint8Array(32 + text.length), view = new DataView(payload.buffer);
  view.setUint32(0, 9, false);
  view.setUint8(4, FrameType.Request);
  view.setUint32(8, 300, false);
  view.setBigUint64(12, requestID, false);
  view.setUint32(28, text.length, false);
  payload.set(text, 32);
  return serverFrame(FrameType.Error, payload, 400, requestID);
}

function serverFrame(type: number, payload = new Uint8Array(), schema = 0, requestID = 0n, flags = 0): ArrayBuffer {
  const frame = new Uint8Array(HeaderSize + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint8(0, 1);
  view.setUint8(1, type);
  view.setUint16(2, flags, false);
  view.setUint8(5, HeaderSize);
  view.setUint16(8, HeaderSize, false);
  view.setUint32(12, payload.length, false);
  view.setUint32(16, schema, false);
  view.setBigUint64(20, requestID, false);
  frame.set(payload, HeaderSize);
  return frame.buffer;
}
