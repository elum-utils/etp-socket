import { beforeEach, describe, expect, it, vi } from "vitest";

const fakeWorkers = vi.hoisted(() => {
  class FakeWorker {
    onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
    onerror: (() => void) | null = null;
    readonly messages: unknown[] = [];
    readonly transfers: Transferable[][] = [];

    constructor() {
      instances.push(this);
    }

    postMessage(message: unknown, transfer: Transferable[] = []): void {
      this.messages.push(message);
      this.transfers.push(transfer);
    }

    deliver(message: unknown): void {
      this.onmessage?.({ data: message } as MessageEvent<unknown>);
    }
  }

  const instances: InstanceType<typeof FakeWorker>[] = [];
  return { FakeWorker, instances };
});

type FakeWorker = InstanceType<typeof fakeWorkers.FakeWorker>;

vi.mock("./worker?worker&inline", () => ({ default: fakeWorkers.FakeWorker }));

import { io, SocketError, SocketState } from "./index";

type Outgoing = {
  "message.send": {
    request: { text: string };
    response: { id: string };
  };
  upload: {
    request: { file: Uint8Array; name: string };
    response: { ok: boolean };
  };
};

type Incoming = {
  "message.new": { id: string; text: string };
  "session.expired": { reason: string };
};

describe("public socket API", () => {
  beforeEach(() => {
    fakeWorkers.instances.length = 0;
  });

  it("delegates auth to the main thread and resolves emit promises", async () => {
    const auth = vi.fn(async () => "token");
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth, autoConnect: false });
    const worker = latestWorker();
    expect((worker.messages[0] as { config: { protocol: { checksum: boolean } } }).config.protocol.checksum).toBe(false);

    socket.connect();
    worker.deliver({ type: "auth", epoch: 4 });
    await Promise.resolve();
    expect(auth).toHaveBeenCalledOnce();
    expect(worker.messages).toContainEqual({ type: "auth", epoch: 4, token: "token" });

    const connected = vi.fn();
    socket.onConnect(connected);
    worker.deliver({ type: "status", state: SocketState.Open });
    expect(socket.state).toBe(SocketState.Open);
    expect(connected).toHaveBeenCalledOnce();

    const result = socket.emit("message.send", { text: "Hello" });
    const emit = worker.messages.at(-1) as { type: string; callID: number };
    expect(emit.type).toBe("emit");
    worker.deliver({ type: "response", callID: emit.callID, data: { id: "message-1" } });
    await expect(result).resolves.toEqual({ id: "message-1" });
  });

  it("supports callback emits and converts worker errors to SocketError", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker();
    const callback = vi.fn();

    socket.emit("message.send", { text: "Hello" }, callback);
    const emit = worker.messages.at(-1) as { callID: number };
    worker.deliver({ type: "response", callID: emit.callID, error: { code: "timeout", message: "request timed out" } });

    expect(callback).toHaveBeenCalledWith(expect.objectContaining({ code: "timeout" }), undefined);
  });

  it("prepares byte arrays as multipart binary fields", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker(), file = new Uint8Array([1, 2, 3]);
    void socket.emit("upload", { file, name: "bytes.bin" });
    const emit = worker.messages.at(-1) as { data: { __etpMultipart: boolean; fields: unknown[]; parts: Array<{ field: string; blob: Uint8Array }> } };
    expect(emit.data.__etpMultipart).toBe(true);
    expect(emit.data.parts).toEqual([{ field: "file", index: 0, name: "", blob: file }]);
    expect(emit.data.fields).toEqual([{ key: "name", value: "bytes.bin" }]);
  });

  it("transfers binary ownership only when explicitly requested", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker(), copied = new Uint8Array([1, 2]), transferred = new Uint8Array([3, 4]);

    void socket.emit("upload", { file: copied, name: "copied.bin" });
    expect(worker.transfers.at(-1)).toEqual([]);
    void socket.emit("upload", { file: transferred, name: "transferred.bin" }, { transfer: true });
    expect(worker.transfers.at(-1)).toEqual([transferred.buffer]);
  });

  it("registers, removes and runs one-shot incoming event listeners", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker();
    const persistent = vi.fn();
    const oneShot = vi.fn();
    const any = vi.fn();

    const unsubscribe = socket.on("message.new", persistent);
    socket.once("message.new", oneShot);
    socket.onAny(any);
    worker.deliver({ type: "event", event: "message.new", data: { id: "1", text: "Hello" } });
    worker.deliver({ type: "event", event: "message.new", data: { id: "2", text: "Again" } });
    unsubscribe();
    worker.deliver({ type: "event", event: "message.new", data: { id: "3", text: "Ignored" } });

    expect(persistent).toHaveBeenCalledTimes(2);
    expect(oneShot).toHaveBeenCalledTimes(1);
    expect(any).toHaveBeenCalledTimes(3);
  });

  it("exposes auth identity, text, and responds to server requests once", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker(), text = vi.fn(), listener = vi.fn((_data, respond) => { respond({ ok: true }); respond({ ok: false }); });
    socket.onText(text);
    socket.on("message.new", listener);

    worker.deliver({ type: "identity", identity: { userID: "account-1" } });
    worker.deliver({ type: "text", text: "hello" });
    worker.deliver({ type: "event", event: "message.new", data: { id: "1", text: "Hello" }, requestID: 44n });

    expect(socket.identity).toEqual({ userID: "account-1" });
    expect(text).toHaveBeenCalledWith("hello");
    expect(worker.messages.filter((message) => (message as { type?: string }).type === "respond")).toEqual([
      { type: "respond", requestID: 44n, event: "message.new", data: { ok: true } },
    ]);
  });

  it("terminates the worker and rejects a pending request", async () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker();
    const result = socket.emit("message.send", { text: "Hello" });

    socket.close();
    expect(socket.state).toBe(SocketState.Terminated);
    expect(worker.messages).toContainEqual({ type: "terminate" });
    await expect(result).rejects.toEqual(expect.objectContaining({ code: "terminated" }));
    expect(() => socket.emit("message.send", { text: "Again" })).toThrow(SocketError);
  });

  it("supports abort signals before and during an emit", async () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false });
    const worker = latestWorker(), already = new AbortController(); already.abort();
    await expect(socket.emit("message.send", { text: "x" }, { signal: already.signal })).rejects.toMatchObject({ code: "closed" });

    const active = new AbortController();
    const pending = socket.emit("message.send", { text: "x" }, { signal: active.signal });
    const emit = worker.messages.at(-1) as { callID: number };
    active.abort();
    expect(worker.messages).toContainEqual({ type: "cancel", callID: emit.callID });
    worker.deliver({ type: "response", callID: emit.callID, error: { code: "closed", message: "request was canceled" } });
    await expect(pending).rejects.toMatchObject({ code: "closed" });
  });

  it("surfaces lifecycle, worker, progress, protocol, and listener errors", async () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: async () => { throw new Error("token failed"); }, autoConnect: false });
    const worker = latestWorker(), disconnected = vi.fn(), progress = vi.fn(), protocol = vi.fn(), errors = vi.fn();
    socket.onDisconnect(disconnected); socket.onProgress(progress); socket.onProtocolEvent(protocol); socket.onError(errors);
    socket.on("message.new", () => { throw new Error("listener failed"); });

    socket.connect(); worker.deliver({ type: "auth", epoch: 2 }); await Promise.resolve();
    expect(worker.messages).toContainEqual({ type: "auth", epoch: 2, error: "token failed" });
    worker.deliver({ type: "disconnect", reason: "connection" });
    worker.deliver({ type: "progress", progress: { transferID: 1n, totalBytes: 1, sentBytes: 1, acknowledgedBytes: 1, state: "completed" } });
    worker.deliver({ type: "protocol", event: { code: "nack", message: "nack" } });
    worker.deliver({ type: "error", error: { code: "protocol", message: "bad frame" } });
    worker.deliver({ type: "event", event: "message.new", data: { id: "1", text: "x" } });
    worker.onerror?.();

    expect(disconnected).toHaveBeenCalledWith("connection"); expect(progress).toHaveBeenCalledOnce(); expect(protocol).toHaveBeenCalledOnce();
    expect(errors.mock.calls.map(([error]) => error.message)).toEqual(expect.arrayContaining(["bad frame", "listener failed", "socket worker failed"]));
    socket.disconnect(); expect(worker.messages).toContainEqual({ type: "disconnect" });
  });

  it("normalizes custom reconnect and protocol configuration and supports callback cancellation", () => {
    const socket = io<Outgoing, Incoming>({ url: "wss://example.test/ws", auth: () => "token", autoConnect: false, timeout: 123, reconnection: { attempts: 2, delay: 3, maxDelay: 4 }, protocol: { chunkSize: 1024, checksum: false } });
    const worker = latestWorker(), configured = worker.messages[0] as { config: { timeout: number; reconnection: { attempts: number }; protocol: { chunkSize: number; checksum: boolean } } };
    expect(configured.config).toMatchObject({ timeout: 123, reconnection: { attempts: 2, delay: 3, maxDelay: 4, enabled: true }, protocol: { chunkSize: 1024, checksum: false } });
    const controller = new AbortController(); controller.abort(); const callback = vi.fn();
    socket.emit("message.send", { text: "x" }, { signal: controller.signal }, callback);
    expect(callback).toHaveBeenCalledWith(expect.objectContaining({ code: "closed" }));
    socket.off("message.new"); socket.close(); socket.close();
  });

  function latestWorker(): FakeWorker {
    const worker = fakeWorkers.instances.at(-1);
    if (!worker) {
      throw new Error("missing fake worker");
    }
    return worker;
  }
});
