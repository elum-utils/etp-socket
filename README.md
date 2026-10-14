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

## Requests during reconnect

The worker queues requests made before the ETP handshake completes or while the
transport is disconnected. After authentication and `HelloAck`, it starts queued
requests in insertion order. Request timeouts begin when the first request frame
is passed to `WebSocket.send`, not while waiting for a connection. Calls retain
their original Promise or callback; the application does not need to catch
`socket is not connected` and re-emit them.

Reconnection retries are unlimited by default, using the configured exponential
backoff and `maxDelay`. Set `reconnection.attempts` to a finite number to stop and
reject waiting requests with a `connection` error when that limit is exhausted.
With `autoConnect: false`, requests wait for `connect()`. With reconnection
disabled, automatic retries are disabled and new offline requests wait for a
manual `connect()`.

`AbortSignal` cancels a queued request without sending it. `disconnect()` rejects
waiting requests with `closed`; `close()` rejects them with `terminated`.
Authentication rejection, an auth-provider failure, or a fatal protocol error
rejects the queue immediately and prevents new calls until an explicit
`connect()` starts a new attempt.

The queue is held in memory in this worker/session; it is not a persistent outbox.
It defaults to 256 requests and a conservative 64 MiB payload budget. Exceeding
these limits rejects the new request with a `protocol` error. Configure the
limits when constructing the socket:

```ts
const socket = io({
  url: "wss://api.example.com/ws",
  maxQueuedRequests: 256,
  maxQueuedBytes: 64 << 20,
});
```

On a transport loss the worker returns unsent requests (including rate-limited
frames) to the queue. An inline request already passed to `WebSocket.send` is
**not replayed**: the server may have committed it before the reply was lost. Its
Promise/callback receives a `closed` error. Application-level idempotency keys
are needed to retry those operations safely. Binary transfers continue using
negotiated ETP transfer resume; this is separate from replaying an RPC.

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
