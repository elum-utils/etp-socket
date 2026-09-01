import { io, SocketState, type TransferProgress } from "@elum/etp-socket";

import "./style.css";

type PingResponse = { message: string; echo: string; serverTime: string };
type UploadResponse = { name: string; size: number; sha256: string; elapsedMillis: number };
type ClientEvents = {
  ping: { request: { message: string; sentAt: number }; response: PingResponse };
  "file.upload": { request: { file: File | Uint8Array; name: string; size: number }; response: UploadResponse };
};
type ServerEvents = Record<string, never>;
type UploadItem = {
  id: number;
  name: string;
  size: number;
  file?: File;
  generated?: boolean;
  state: "queued" | "uploading" | "complete" | "failed";
  progress: number;
  acknowledged: number;
  transferID?: bigint;
  response?: UploadResponse;
  error?: string;
};

const demoBytesPerSecond = 1024 * 1024 * 1024;
const demoFramesPerSecond = 50_000;

const socket = io<ClientEvents, ServerEvents>({
  url: "ws://127.0.0.1:18992/socket",
  auth: () => "demo-token",
  timeout: 120_000,
  reconnection: true,
  protocol: {
    chunkSize: 32 << 10,
    maxTransferBytes: 512 << 20,
    maxInFlightChunks: 16,
    maxFramesPerSecond: demoFramesPerSecond,
    maxBytesPerSecond: demoBytesPerSecond,
    checksum: false,
  },
});

const elements = {
  status: byID("status"),
  statusDot: byID("status-dot"),
  connection: button("connection-button"),
  pingForm: form("ping-form"),
  pingMessage: input("ping-message"),
  latency: byID("latency"),
  pong: byID("pong-output"),
  fileInput: input("file-input"),
  dropZone: byID("drop-zone"),
  fileList: byID("file-list"),
  queueSummary: byID("queue-summary"),
  upload: button("upload-button"),
  clear: button("clear-button"),
  generatedSize: select("generated-size"),
  generate: button("generate-file"),
  log: byID("event-log"),
  clearLog: button("clear-log"),
};

let items: UploadItem[] = [];
let nextItemID = 0;
let activeItem: UploadItem | undefined;
let uploading = false;
const transferItems = new Map<bigint, UploadItem>();

socket.onConnect(() => {
  renderConnection(SocketState.Open);
  log("connected", `Authenticated as ${socket.identity?.userID ?? "demo-client"}`);
});
socket.onDisconnect((reason) => {
  renderConnection(socket.state);
  log("disconnected", reason);
});
socket.onError((error) => log("error", `${error.code}: ${error.message}`));
socket.onProtocolEvent((event) => log("protocol", `${event.code}: ${event.message}`));
socket.onProgress((progress) => updateProgress(progress));

elements.connection.addEventListener("click", () => {
  if (socket.state === SocketState.Open || socket.state === SocketState.Connecting || socket.state === SocketState.Reconnecting) socket.disconnect();
  else socket.connect();
});

elements.pingForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const started = performance.now();
  elements.pong.textContent = "Waiting...";
  try {
    const response = await socket.emit("ping", { message: elements.pingMessage.value, sentAt: Date.now() });
    const latency = performance.now() - started;
    elements.latency.textContent = latency.toFixed(1);
    elements.pong.textContent = `${response.message} · echo: ${response.echo} · ${response.serverTime}`;
    log("response", `pong in ${latency.toFixed(1)} ms`);
  } catch (error) {
    elements.pong.textContent = errorMessage(error);
  }
});

elements.fileInput.addEventListener("change", () => addFiles(elements.fileInput.files));
for (const event of ["dragenter", "dragover"]) {
  elements.dropZone.addEventListener(event, (dragEvent) => {
    dragEvent.preventDefault();
    elements.dropZone.classList.add("dragging");
  });
}
for (const event of ["dragleave", "drop"]) {
  elements.dropZone.addEventListener(event, (dragEvent) => {
    dragEvent.preventDefault();
    elements.dropZone.classList.remove("dragging");
    if (dragEvent instanceof DragEvent && dragEvent.dataTransfer?.files.length) addFiles(dragEvent.dataTransfer.files);
  });
}

elements.generate.addEventListener("click", () => {
  const mib = Number(elements.generatedSize.value);
  const size = mib << 20;
  const name = `etp-test-${mib}MiB.bin`;
  items.push({ id: ++nextItemID, name, size, generated: true, state: "queued", progress: 0, acknowledged: 0 });
  renderFiles();
});
elements.upload.addEventListener("click", () => void uploadQueue());
elements.clear.addEventListener("click", () => {
  if (uploading) return;
  items = [];
  transferItems.clear();
  renderFiles();
});
elements.clearLog.addEventListener("click", () => elements.log.replaceChildren());

function addFiles(files: ArrayLike<File> | null): void {
  if (!files) return;
  for (const file of Array.from(files)) items.push({ id: ++nextItemID, name: file.name, size: file.size, file, state: "queued", progress: 0, acknowledged: 0 });
  elements.fileInput.value = "";
  renderFiles();
}

async function uploadQueue(): Promise<void> {
  if (uploading || socket.state !== SocketState.Open) return;
  uploading = true;
  renderFiles();
  for (const item of items.filter((entry) => entry.state === "queued" || entry.state === "failed")) {
    activeItem = item;
    item.state = "uploading";
    item.progress = 0;
    item.acknowledged = 0;
    if (item.transferID !== undefined) transferItems.delete(item.transferID);
    item.transferID = undefined;
    item.error = undefined;
    renderFiles();
    const started = performance.now();
    try {
      const file = item.file ?? generatedBytes(item.size);
      item.response = await socket.emit("file.upload", { file, name: item.name, size: item.size }, { transfer: true });
      item.state = "complete";
      item.progress = 1;
      log("upload", `${item.name} verified in ${((performance.now() - started) / 1_000).toFixed(2)} s`);
    } catch (error) {
      item.state = "failed";
      item.error = errorMessage(error);
      log("upload error", `${item.name}: ${item.error}`);
    }
    renderFiles();
  }
  activeItem = undefined;
  uploading = false;
  renderFiles();
}

function updateProgress(progress: TransferProgress): void {
  if (progress.direction === "receive") return;
  let item = transferItems.get(progress.transferID);
  if (!item) {
    if (!activeItem || activeItem.transferID !== undefined || progress.state !== "sending") return;
    item = activeItem;
    item.transferID = progress.transferID;
    transferItems.set(progress.transferID, item);
    log("transfer", `${item.name} started as ${progress.transferID}`);
  }
  if (item.state !== "uploading") return;
  item.acknowledged = progress.acknowledgedBytes;
  item.progress = progress.totalBytes ? progress.acknowledgedBytes / progress.totalBytes : 0;
  if (progress.state === "failed" || progress.state === "canceled") item.state = "failed";
  renderFiles();
}

function renderConnection(state = socket.state): void {
  elements.status.textContent = state;
  elements.statusDot.className = `status-dot ${state}`;
  elements.connection.textContent = state === SocketState.Open || state === SocketState.Connecting || state === SocketState.Reconnecting ? "Disconnect" : "Connect";
}

function renderFiles(): void {
  const queued = items.filter((item) => item.state === "queued").length;
  elements.queueSummary.textContent = items.length ? `${items.length} files · ${formatBytes(items.reduce((sum, item) => sum + item.size, 0))} · ${queued} queued` : "No files queued";
  elements.upload.disabled = uploading || socket.state !== SocketState.Open || !items.some((item) => item.state === "queued" || item.state === "failed");
  elements.clear.disabled = uploading || items.length === 0;
  elements.fileList.replaceChildren(...items.map(fileRow));
}

function fileRow(item: UploadItem): HTMLElement {
  const row = document.createElement("article");
  row.className = `file-row ${item.state}`;
  const result = item.response ? `SHA-256 ${item.response.sha256} · server ${item.response.elapsedMillis} ms` : item.error ?? `${formatBytes(item.acknowledged)} / ${formatBytes(item.size)}`;
  row.innerHTML = `<div class="file-meta"><strong></strong><span>${formatBytes(item.size)} · ${item.state}</span></div><progress max="1" value="${item.progress}"></progress><small></small>`;
  row.querySelector("strong")!.textContent = item.name;
  row.querySelector("small")!.textContent = result;
  return row;
}

function log(kind: string, message: string): void {
  const item = document.createElement("li");
  const time = document.createElement("time");
  time.textContent = new Date().toLocaleTimeString();
  const label = document.createElement("strong");
  label.textContent = kind;
  const body = document.createElement("span");
  body.textContent = message;
  item.append(time, label, body);
  elements.log.prepend(item);
  while (elements.log.children.length > 100) elements.log.lastElementChild?.remove();
}

function byID(id: string): HTMLElement { return document.getElementById(id)!; }
function input(id: string): HTMLInputElement { return byID(id) as HTMLInputElement; }
function button(id: string): HTMLButtonElement { return byID(id) as HTMLButtonElement; }
function form(id: string): HTMLFormElement { return byID(id) as HTMLFormElement; }
function select(id: string): HTMLSelectElement { return byID(id) as HTMLSelectElement; }
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function generatedBytes(size: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(size);
  crypto.getRandomValues(bytes.subarray(0, Math.min(size, 65_536)));
  return bytes;
}
function formatBytes(bytes: number): string {
  if (bytes < 1 << 10) return `${bytes} B`;
  if (bytes < 1 << 20) return `${(bytes / (1 << 10)).toFixed(1)} KiB`;
  return `${(bytes / (1 << 20)).toFixed(1)} MiB`;
}

renderConnection();
renderFiles();
