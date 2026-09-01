import {
  SocketError,
  SocketState,
  type ClientEvents,
  type DisconnectReason,
  type EmitCallback,
  type EmitOptions,
  type EventResponder,
  type ProtocolEvent,
  type ReconnectionOptions,
  type ServerEvents,
  type Socket,
  type SocketOptions,
  type SocketIdentity,
  type TransferProgress,
  type MainMessage,
  type WorkerConfig,
  type WorkerMessage,
} from "./types";
import Worker from "./worker?worker&inline";

type Pending = {
  resolve: (data: unknown) => void;
  reject: (error: SocketError) => void;
  callback?: EmitCallback<unknown>;
  abortCleanup?: () => void;
};

type Listener<T> = (data: T, respond: EventResponder) => void;

const defaultReconnection: Required<ReconnectionOptions> = {
  enabled: true,
  attempts: 5,
  delay: 1_000,
  maxDelay: 10_000,
};

class ETPWorkerSocket<Outgoing extends ClientEvents, Incoming extends ServerEvents> implements Socket<Outgoing, Incoming> {
  private readonly worker: Worker;
  private readonly events = new Map<string, Set<Listener<unknown>>>();
  private readonly anyListeners = new Set<(event: keyof Incoming & string, data: Incoming[keyof Incoming]) => void>();
  private readonly connectListeners = new Set<() => void>();
  private readonly disconnectListeners = new Set<(reason: DisconnectReason) => void>();
  private readonly errorListeners = new Set<(error: SocketError) => void>();
  private readonly progressListeners = new Set<(progress: TransferProgress) => void>();
  private readonly protocolListeners = new Set<(event: ProtocolEvent) => void>();
  private readonly textListeners = new Set<(text: string) => void>();
  private readonly pending = new Map<number, Pending>();
  private stateValue: SocketState = SocketState.Closed;
  private nextCallID = 0;
  private terminated = false;
  private identityValue: SocketIdentity | undefined;

  constructor(private readonly options: SocketOptions) {
    if (typeof Worker === "undefined") {
      throw new SocketError("connection", "@elum/etp-socket requires a browser Worker runtime");
    }
    this.worker = new Worker({ name: "elum-etp" });
    this.worker.onmessage = ({ data }: MessageEvent<WorkerMessage>) => this.handleWorkerMessage(data);
    this.worker.onerror = () => this.handleError(new SocketError("connection", "socket worker failed"));
    this.post({ type: "configure", config: normalizeConfig(options) });
    if (options.autoConnect ?? true) {
      this.connect();
    }
  }

  get state(): SocketState {
    return this.stateValue;
  }

  get identity(): SocketIdentity | undefined { return this.identityValue; }

  connect(): void {
    this.assertActive();
    this.post({ type: "connect" });
  }

  disconnect(): void {
    if (!this.terminated) {
      this.post({ type: "disconnect" });
    }
  }

  close(): void {
    if (this.terminated) {
      return;
    }
    this.terminated = true;
    this.post({ type: "terminate" });
    this.rejectPending(new SocketError("terminated", "socket is closed"));
    this.stateValue = SocketState.Terminated;
  }

  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"]): Promise<Outgoing[Event]["response"]>;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], options: EmitOptions): Promise<Outgoing[Event]["response"]>;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], callback: EmitCallback<Outgoing[Event]["response"]>): void;
  emit<Event extends keyof Outgoing & string>(event: Event, data: Outgoing[Event]["request"], options: EmitOptions, callback: EmitCallback<Outgoing[Event]["response"]>): void;
  emit<Event extends keyof Outgoing & string>(
    event: Event,
    data: Outgoing[Event]["request"],
    optionsOrCallback?: EmitOptions | EmitCallback<Outgoing[Event]["response"]>,
    callbackArg?: EmitCallback<Outgoing[Event]["response"]>,
  ): Promise<Outgoing[Event]["response"]> | void {
    this.assertActive();
    const options = typeof optionsOrCallback === "function" ? undefined : optionsOrCallback;
    const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : callbackArg;
    const callID = ++this.nextCallID;
    const signal = options?.signal;
    if (signal?.aborted) {
      const error = new SocketError("closed", "request was canceled");
      if (callback) { callback(error); return; }
      return Promise.reject(error);
    }
    const abortCleanup = signal ? this.bindAbort(callID, signal) : undefined;
    const payload = preparePayload(data);
    const transfer = options?.transfer ? payloadTransferables(payload) : undefined;
    if (callback) {
      this.pending.set(callID, {
        resolve: () => undefined,
        reject: () => undefined,
        callback: callback as EmitCallback<unknown>,
        abortCleanup,
      });
      this.post({ type: "emit", callID, event, data: payload }, transfer);
      return;
    }
    return new Promise<Outgoing[Event]["response"]>((resolve, reject) => {
      this.pending.set(callID, { resolve: resolve as (data: unknown) => void, reject, abortCleanup });
      this.post({ type: "emit", callID, event, data: payload }, transfer);
    });
  }

  on<Event extends keyof Incoming & string>(event: Event, listener: (data: Incoming[Event], respond: EventResponder) => void): () => void {
    const listeners = this.events.get(event) ?? new Set<Listener<unknown>>();
    listeners.add(listener as Listener<unknown>);
    this.events.set(event, listeners);
    return () => this.off(event, listener);
  }

  once<Event extends keyof Incoming & string>(event: Event, listener: (data: Incoming[Event], respond: EventResponder) => void): () => void {
    const wrapped = (data: Incoming[Event], respond: EventResponder) => {
      this.off(event, wrapped);
      listener(data, respond);
    };
    return this.on(event, wrapped);
  }

  off<Event extends keyof Incoming & string>(event: Event, listener?: (data: Incoming[Event], respond: EventResponder) => void): void {
    const listeners = this.events.get(event);
    if (!listeners) {
      return;
    }
    if (listener) {
      listeners.delete(listener as Listener<unknown>);
    } else {
      listeners.clear();
    }
    if (listeners.size === 0) {
      this.events.delete(event);
    }
  }

  onAny(listener: (event: keyof Incoming & string, data: Incoming[keyof Incoming]) => void): () => void {
    return this.subscribe(this.anyListeners, listener);
  }

  onConnect(listener: () => void): () => void {
    return this.subscribe(this.connectListeners, listener);
  }

  onDisconnect(listener: (reason: DisconnectReason) => void): () => void {
    return this.subscribe(this.disconnectListeners, listener);
  }

  onError(listener: (error: SocketError) => void): () => void {
    return this.subscribe(this.errorListeners, listener);
  }

  onProgress(listener: (progress: TransferProgress) => void): () => void { return this.subscribe(this.progressListeners, listener); }
  onProtocolEvent(listener: (event: ProtocolEvent) => void): () => void { return this.subscribe(this.protocolListeners, listener); }
  onText(listener: (text: string) => void): () => void { return this.subscribe(this.textListeners, listener); }

  private handleWorkerMessage(message: WorkerMessage): void {
    switch (message.type) {
      case "auth":
        void this.sendAuth(message.epoch);
        return;
      case "status": {
        const wasOpen = this.stateValue === SocketState.Open;
        this.stateValue = message.state;
        if (!wasOpen && message.state === SocketState.Open) {
          this.callListeners(this.connectListeners);
        }
        return;
      }
      case "disconnect":
        this.callListeners(this.disconnectListeners, message.reason);
        return;
      case "event":
        this.emitEvent(message.event, message.data, message.requestID);
        return;
      case "text":
        this.callListeners(this.textListeners, message.text);
        return;
      case "identity":
        this.identityValue = message.identity;
        return;
      case "response":
        this.completePending(message);
        return;
      case "error":
        this.handleError(new SocketError(message.error.code, message.error.message));
        return;
      case "progress":
        this.callListeners(this.progressListeners, message.progress);
        return;
      case "protocol":
        this.callListeners(this.protocolListeners, message.event);
        return;
    }
  }

  private async sendAuth(epoch: number): Promise<void> {
    try {
      const token = await this.options.auth();
      this.post({ type: "auth", epoch, token });
    } catch (error) {
      this.post({
        type: "auth",
        epoch,
        error: error instanceof Error ? error.message : "authentication provider failed",
      });
    }
  }

  private completePending(message: Extract<WorkerMessage, { type: "response" }>): void {
    const pending = this.pending.get(message.callID);
    if (!pending) {
      return;
    }
    this.pending.delete(message.callID);
    pending.abortCleanup?.();
    if (message.error) {
      const error = new SocketError(message.error.code, message.error.message);
      pending.callback?.(error, undefined);
      pending.reject(error);
      return;
    }
    pending.callback?.(null, message.data);
    pending.resolve(message.data);
  }

  private emitEvent(event: string, data: unknown, requestID?: bigint): void {
    let responded = false;
    const respond: EventResponder = (response) => {
      if (!requestID || responded) return;
      responded = true;
      this.post({ type: "respond", requestID, event, data: preparePayload(response) });
    };
    for (const listener of this.events.get(event) ?? []) {
      this.callListener(listener, data, respond);
    }
    for (const listener of this.anyListeners) {
      try {
        listener(event as keyof Incoming & string, data as Incoming[keyof Incoming]);
      } catch (error) {
        this.handleError(new SocketError("protocol", error instanceof Error ? error.message : "socket listener failed"));
      }
    }
  }

  private handleError(error: SocketError): void {
    this.callListeners(this.errorListeners, error);
  }

  private rejectPending(error: SocketError): void {
    for (const pending of this.pending.values()) {
      pending.abortCleanup?.();
      pending.callback?.(error, undefined);
      pending.reject(error);
    }
    this.pending.clear();
  }

  private post(message: MainMessage, transfer?: Transferable[]): void {
    if (transfer?.length) this.worker.postMessage(message, transfer);
    else this.worker.postMessage(message);
  }

  private bindAbort(callID: number, signal: AbortSignal): () => void {
    const abort = () => this.post({ type: "cancel", callID });
    signal.addEventListener("abort", abort, { once: true });
    return () => signal.removeEventListener("abort", abort);
  }

  private assertActive(): void {
    if (this.terminated) {
      throw new SocketError("terminated", "socket is closed");
    }
  }

  private subscribe<T>(listeners: Set<T>, listener: T): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  }

  private callListeners<T>(listeners: Iterable<(value: T) => void>, value?: T): void {
    for (const listener of listeners) {
      try {
        listener(value as T);
      } catch (error) {
        this.handleError(new SocketError("protocol", error instanceof Error ? error.message : "socket listener failed"));
      }
    }
  }

  private callListener(listener: Listener<unknown>, value: unknown, respond: EventResponder): void {
    try {
      listener(value, respond);
    } catch (error) {
      this.handleError(new SocketError("protocol", error instanceof Error ? error.message : "socket listener failed"));
    }
  }
}

function normalizeConfig(options: SocketOptions): WorkerConfig {
  const input = options.reconnection;
  const reconnection =
    input === false
      ? { ...defaultReconnection, enabled: false }
      : input === true || input === undefined
        ? defaultReconnection
        : {
            enabled: input.enabled ?? true,
            attempts: input.attempts ?? defaultReconnection.attempts,
            delay: input.delay ?? defaultReconnection.delay,
            maxDelay: input.maxDelay ?? defaultReconnection.maxDelay,
          };
  return {
    url: options.url,
    timeout: options.timeout ?? 10_000,
    reconnection,
    protocol: {
      chunkSize: options.protocol?.chunkSize ?? 16 << 10,
      maxTransferBytes: options.protocol?.maxTransferBytes ?? 64 << 20,
      maxConcurrentTransfers: options.protocol?.maxConcurrentTransfers ?? 16,
      maxInFlightChunks: options.protocol?.maxInFlightChunks ?? 16,
      heartbeatInterval: options.protocol?.heartbeatInterval ?? 10_000,
      heartbeatTimeout: options.protocol?.heartbeatTimeout ?? 20_000,
      ackTimeout: options.protocol?.ackTimeout ?? 2_000,
      retryLimit: options.protocol?.retryLimit ?? 3,
      maxRequestsPerSecond: options.protocol?.maxRequestsPerSecond ?? 200,
      maxFramesPerSecond: options.protocol?.maxFramesPerSecond ?? 2_000,
      maxBytesPerSecond: options.protocol?.maxBytesPerSecond ?? 64 << 20,
      checksum: options.protocol?.checksum ?? false,
      resumeToken: options.protocol?.resumeToken ?? new Uint8Array(),
    },
  };
}

export function io<Outgoing extends ClientEvents, Incoming extends ServerEvents>(options: SocketOptions): Socket<Outgoing, Incoming> {
  return new ETPWorkerSocket<Outgoing, Incoming>(options);
}

function preparePayload(data: unknown): unknown {
  const fields: Array<{ key: string; value: string }> = [], parts: Array<{ field: string; index: number; name: string; blob: Blob | ArrayBuffer | Uint8Array }> = [];
  const append = (field: string, value: unknown, index: number) => {
    if (isBinaryPart(value)) parts.push({ field, index, name: typeof File !== "undefined" && value instanceof File ? value.name : "", blob: value });
    else fields.push({ key: field, value: typeof value === "string" ? value : JSON.stringify(value) });
  };
  if (typeof FormData !== "undefined" && data instanceof FormData) {
    const indices = new Map<string, number>();
    for (const [field, value] of data.entries()) { const index = indices.get(field) ?? 0; append(field, value, index); indices.set(field, index + 1); }
  } else if (data && typeof data === "object" && !(data instanceof Blob) && !(data instanceof ArrayBuffer) && !(data instanceof Uint8Array)) {
    for (const [field, value] of Object.entries(data)) {
      if (Array.isArray(value) && value.some(isBinaryPart)) value.forEach((entry, index) => append(field, entry, index));
      else if (isBinaryPart(value)) append(field, value, 0);
      else append(field, value, 0);
    }
  }
  return parts.length ? { __etpMultipart: true, fields, parts } : data;
}

function isBinaryPart(value: unknown): value is Blob | ArrayBuffer | Uint8Array {
  return value instanceof Blob || value instanceof ArrayBuffer || value instanceof Uint8Array;
}

function payloadTransferables(data: unknown): Transferable[] {
  const buffers = new Set<ArrayBuffer>();
  const add = (value: unknown) => {
    if (value instanceof ArrayBuffer) buffers.add(value);
    else if (value instanceof Uint8Array && value.buffer instanceof ArrayBuffer) buffers.add(value.buffer);
  };
  if (data && typeof data === "object" && "__etpMultipart" in data) {
    for (const part of (data as unknown as { parts: Array<{ blob: unknown }> }).parts) add(part.blob);
  } else add(data);
  return [...buffers];
}

export { SocketError, SocketState };
export type {
  ClientEvents,
  DisconnectReason,
  EmitCallback,
  EventDefinition,
  EmitOptions,
  EventResponder,
  ProtocolEvent,
  ProtocolOptions,
  ReconnectionOptions,
  ServerEvents,
  Socket,
  SocketErrorCode,
  SocketOptions,
  SocketIdentity,
  TransferProgress,
} from "./types";
