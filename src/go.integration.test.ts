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
    dispatch({ type: "configure", config: { url, timeout: 10_000, reconnection: { enabled: false, attempts: 0, delay: 0, maxDelay: 0 }, protocol: { chunkSize: 16 << 10, maxTransferBytes: 64 << 20, maxConcurrentTransfers: 16, maxInFlightChunks: 16, heartbeatInterval: 10_000, heartbeatTimeout: 20_000, ackTimeout: 2_000, retryLimit: 3, maxFramesPerSecond: 2_000, maxBytesPerSecond: 64 << 20, checksum: true, resumeToken: new Uint8Array() } } });
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
