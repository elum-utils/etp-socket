# @elum/etp-socket

Browser ETP client with a Socket.IO-like API. WebSocket, ETP handshake,
authentication, reconnect, heartbeat and request timeouts run inside a dedicated
Web Worker. The application only receives typed events and request results.

```ts
import { io } from "@elum/etp-socket";

type ClientEvents = {
  "message.send": {
    request: { dialog: string; text: string };
    response: { id: string };
  };
};

type ServerEvents = {
  "message.new": { id: string; text: string };
};

const socket = io<ClientEvents, ServerEvents>({
  url: "wss://api.example.com/ws",
  auth: () => sessionStorage.getItem("token") ?? "",
  timeout: 10_000,
  reconnection: true,
});

socket.on("message.new", (message) => console.log(message));

const created = await socket.emit("message.send", {
  dialog: "dialog-id",
  text: "Hello",
});

socket.emit("message.send", { dialog: "dialog-id", text: "Hello" }, (error, response) => {
  if (error) return;
  console.log(response?.id);
});
```

`emit` creates an ETP request and resolves only a response carrying the same
request ID. The timeout is enforced inside the Worker. `disconnect()` keeps the
Worker for a later `connect()`, while `close()` terminates it and rejects pending
requests.

Server requests use the same event API and an acknowledgment callback:

```ts
socket.on("client.confirm", (request, respond) => {
  respond({ accepted: true });
});
```

`File`, `Blob`, `FormData`, arrays of files, and objects containing files are
automatically sent as ETP multipart transfers. Small JSON stays inline; large
JSON automatically switches to chunked transfer without changing the API.

```ts
await socket.emit("attach.upload", {
  dialog: "dialog-id",
  files: [fileA, fileB],
});
```

The client supports authentication identity, capability negotiation, request and
response correlation, ACK/NACK retry, receiver windows, SHA-256 verification,
cancellation, transfer progress, reconnect resume, heartbeat, protocol errors,
text frames, and graceful drain/close. Protocol limits are configurable:

```ts
const socket = io<ClientEvents, ServerEvents>({
  url: "wss://api.example.com/ws",
  auth: getToken,
  protocol: {
    chunkSize: 16 << 10,
    maxTransferBytes: 64 << 20,
    maxConcurrentTransfers: 16,
    maxInFlightChunks: 16,
    heartbeatInterval: 10_000,
    heartbeatTimeout: 20_000,
    ackTimeout: 2_000,
    retryLimit: 3,
    maxRequestsPerSecond: 200,
    maxFramesPerSecond: 2_000,
    maxBytesPerSecond: 64 << 20,
    checksum: true,
  },
});
```

During `HelloAck`, the Go server advertises its request, frame, and byte token
buckets. The worker paces its outbound queues automatically; file chunks consume
frame/byte quota but not logical-request quota, and realtime/control traffic is
scheduled ahead of bulk chunks. These client checks improve behavior only: the
server enforces every advertised limit even when a modified client ignores them.

## Browser demo

Run the local Go ETP server and Vite client together:

```bash
npm run demo
```

Open `http://127.0.0.1:5173`. The demo supports ping/pong requests, generated 8-128 MiB test files, regular file selection, chunk progress, SHA-256 verification, and ping requests during an active upload.

## Checks

```bash
npm run check
npm test
npm run test:coverage
npm run test:go
npm run build
npm run package:check
```

## Publishing

Package releases are published by `.github/workflows/publish.yml`. Create a
GitHub Release with a SemVer tag carrying a `v` prefix, for example `v0.0.1`.
The workflow writes that tag version into the npm manifest and lockfile, then
validates TypeScript, unit tests, the production build, and the npm tarball
before publishing. Go conformance remains available as `npm run test:go` and
requires a matching local `go-etp` checkout.

For the first release, add an npm granular access token with package publish
permission and 2FA bypass as the `NPM_TOKEN` repository secret. After the
package exists, configure npm Trusted Publishing for
`elum-utils/etp-socket` and workflow `publish.yml`; the workflow already grants
the required OIDC permission and publishes with provenance. The token secret can
then be removed.
