import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { SocketState } from "./types";

const url = process.env.ETP_GO_URL;
const suite = url ? describe : describe.skip;

type Output = Record<string, unknown>;
type Scope = { onmessage: ((event: MessageEvent<unknown>) => void) | null; postMessage(message: Output): void; close(): void };

suite("Go ETP conformance", () => {
  let scope: Scope;
  let output: Output[];

  beforeAll(async () => {
    output = [];
    scope = { onmessage: null, postMessage: (message) => output.push(message), close: () => undefined };
    Object.assign(globalThis, { self: scope });
    await import("./worker");
    dispatch({ type: "configure", config: { url, timeout: 10_000, reconnection: { enabled: false, attempts: 0, delay: 0, maxDelay: 0 }, protocol: { chunkSize: 16 << 10, maxTransferBytes: 64 << 20, maxConcurrentTransfers: 16, maxInFlightChunks: 16, heartbeatInterval: 10_000, heartbeatTimeout: 20_000, ackTimeout: 2_000, retryLimit: 3, maxRequestsPerSecond: 200, maxFramesPerSecond: 2_000, maxBytesPerSecond: 64 << 20, checksum: true, resumeToken: new Uint8Array() } } });
    dispatch({ type: "connect" });
    await waitFor(() => output.find((message) => message.type === "auth"));
    const auth = output.find((message) => message.type === "auth") as { epoch: number };
    dispatch({ type: "auth", epoch: auth.epoch, token: "integration-token" });
    await waitFor(() => output.find((message) => message.type === "status" && message.state === SocketState.Open));
  });

  afterAll(() => dispatch({ type: "terminate" }));

  it("round trips inline and chunked requests against the Go server", async () => {
    await waitFor(() => output.find((message) => message.type === "text" && message.text === "server-ready"));
    const request = await waitFor(() => output.find((message) => message.type === "event" && message.event === "client.confirm")) as { requestID: bigint };
    dispatch({ type: "respond", requestID: request.requestID, event: "client.confirm", data: { accepted: true } });
    await waitFor(() => output.find((message) => message.type === "event" && message.event === "client.confirmed"));

    dispatch({ type: "emit", callID: 1, event: "echo", data: { text: "inline" } });
    const inline = await waitFor(() => output.find((message) => message.type === "response" && message.callID === 1)) as { data: unknown };
    expect(inline.data).toEqual({ text: "inline" });

    dispatch({ type: "emit", callID: 2, event: "echo", data: { text: "x".repeat(96 << 10) } });
    const large = await waitFor(
      () => output.find((message) => message.type === "response" && message.callID === 2),
      10_000,
      () => JSON.stringify(output, (_, value) => typeof value === "bigint" ? value.toString() : value),
    ) as { data: { text: string } };
    expect(large.data.text).toHaveLength(96 << 10);
    expect(output.some((message) => message.type === "progress" && (message.progress as { totalBytes?: number; state?: string }).totalBytes! >= (96 << 10) && (message.progress as { state?: string }).state === "completed")).toBe(true);

    dispatch({ type: "emit", callID: 3, event: "multipart", data: { __etpMultipart: true, fields: [{ key: "dialog", value: `"dialog-1"` }], parts: [{ field: "files", index: 0, name: "one.txt", blob: new Blob(["one"]) }, { field: "files", index: 1, name: "two.txt", blob: new Blob(["two"]) }] } });
    const multipart = await waitFor(() => output.find((message) => message.type === "response" && message.callID === 3)) as { data: unknown };
    expect(multipart.data).toEqual({ ok: true });
  }, 20_000);

  it("keeps requests made while offline and flushes them after a real Go handshake", async () => {
    const start = output.length;
    dispatch({ type: "disconnect" });
    await waitFor(() => output.slice(start).find(message => message.type === "status" && message.state === SocketState.Closed));
    for (const callID of [101, 102]) dispatch({ type: "emit", callID, event: "echo", data: { callID } });
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(output.filter(message => message.type === "response" && [101,102].includes(message.callID as number))).toEqual([]);
    await reconnect(start, "integration-token");
    for (const callID of [101,102]) {
      const response = await waitFor(() => output.find(message => message.type === "response" && message.callID === callID));
      expect(response).toMatchObject({ data: { callID }, error: undefined });
      expect(output.filter(message => message.type === "response" && message.callID === callID)).toHaveLength(1);
    }
  });

  it("rejects an offline queue on real Go auth failure and supports a fresh explicit login", async () => {
    const start = output.length;
    dispatch({ type: "disconnect" });
    await waitFor(() => output.slice(start).find(message => message.type === "status" && message.state === SocketState.Closed));
    for (const callID of [103,104]) dispatch({ type: "emit", callID, event: "echo", data: {} });
    dispatch({ type: "connect" });
    const auth = await waitFor(() => output.slice(start).find(message => message.type === "auth")) as { epoch: number };
    dispatch({ type: "auth", epoch: auth.epoch, token: "invalid-token" });
    await waitFor(() => output.slice(start).find(message => message.type === "disconnect" && message.reason === "auth"));
    dispatch({ type: "emit", callID: 105, event: "echo", data: {} });
    for (const callID of [103,104,105]) {
      const response = await waitFor(() => output.find(message => message.type === "response" && message.callID === callID));
      expect(response, JSON.stringify(output.slice(start), (_, value) => typeof value === "bigint" ? value.toString() : value)).toMatchObject({ error: { code: "auth", message: "invalid integration token" } });
      expect(output.filter(message => message.type === "response" && message.callID === callID)).toHaveLength(1);
    }
    const resumed = output.length;
    dispatch({ type: "connect" });
    dispatch({ type: "emit", callID: 106, event: "echo", data: { restored: true } });
    await reconnect(resumed, "integration-token", false);
    const response = await waitFor(() => output.find(message => message.type === "response" && message.callID === 106));
    expect(response).toMatchObject({ data: { restored: true }, error: undefined });
  });

  async function reconnect(start: number, token: string, connect = true): Promise<void> {
    if (connect) dispatch({ type: "connect" });
    const auth = await waitFor(() => output.slice(start).find(message => message.type === "auth")) as { epoch: number };
    dispatch({ type: "auth", epoch: auth.epoch, token });
    await waitFor(() => output.slice(start).find(message => message.type === "status" && message.state === SocketState.Open));
    const challenge = await waitFor(() => output.slice(start).find(message => message.type === "event" && message.event === "client.confirm")) as { requestID: bigint };
    dispatch({ type: "respond", requestID: challenge.requestID, event: "client.confirm", data: { accepted: true } });
  }

  function dispatch(data: unknown): void { scope.onmessage?.({ data } as MessageEvent<unknown>); }
});

async function waitFor<T>(read: () => T | undefined, timeout = 10_000, diagnostics?: () => string): Promise<T> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = read();
    if (value !== undefined) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`integration timeout${diagnostics ? `: ${diagnostics()}` : ""}`);
}
