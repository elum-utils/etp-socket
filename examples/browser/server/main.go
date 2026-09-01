package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	etp "github.com/elum-utils/go-etp"
	etpgorilla "github.com/elum-utils/go-etp/adapters/gorilla"
	"github.com/gorilla/websocket"
)

const address = "127.0.0.1:18992"

const demoRequestsPerSecond = 10
const demoBytesPerSecond = 2 << 20
const demoFramesPerSecond = 256

var progressBuckets sync.Map

func main() {
	config := etp.DefaultServerConfig()
	config.FlowControl.MaxTransferBytes = 512 << 20
	config.FlowControl.MaxChunkSize = 64 << 10
	config.RateLimit.MaxRequestsPerSecond = demoRequestsPerSecond
	config.RateLimit.RequestBurst = 5
	config.RateLimit.MaxBytesPerSecond = demoBytesPerSecond
	config.RateLimit.ByteBurst = 256 << 10
	config.RateLimit.MaxFramesPerSecond = demoFramesPerSecond
	config.RateLimit.FrameBurst = 64

	// The demo intentionally accepts generated 128 MiB files even when the host
	// temporary directory is nearly full. Production applications should stream
	// large bodies to their durable object store instead.
	app := etp.New(etp.Config{Session: config, MaxMemoryBody: 256 << 20})
	must(app.OnAuth(auth))
	must(app.On("ping", ping))
	must(app.On("file.upload", upload))
	must(app.OnConnect(func(_ context.Context, peer *etp.Peer) error {
		log.Printf("connected user=%s remote=%s", peer.Identity().UserID, peer.RemoteAddr())
		return nil
	}))
	must(app.OnDisconnect(func(_ context.Context, peer *etp.Peer, err error) {
		log.Printf("disconnected user=%s err=%v", peer.Identity().UserID, err)
	}))
	must(app.OnProgress(func(_ context.Context, _ *etp.Peer, progress etp.Progress) { logProgress(progress) }))
	must(app.OnProtocolEvent(func(_ context.Context, peer *etp.Peer, event etp.ProtocolEvent) {
		log.Printf("protocol user=%s code=%v message=%s limit_kind=%d limit=%d retry_ms=%d violations=%d", peer.RateLimitID(), event.Code, event.Message, event.LimitKind, event.Limit, event.RetryAfterMillis, event.Violations)
	}))
	must(app.OnError(func(ctx *etp.Context, err error) {
		log.Printf("handler event=%s request=%d error=%v", ctx.Event, ctx.RequestID, err)
	}))
	app.Compile()

	adapter := &etpgorilla.Adapter{Upgrader: websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			return r.Header.Get("Origin") == "http://127.0.0.1:5173"
		},
	}}
	mux := http.NewServeMux()
	mux.Handle("/socket", adapter.Handler(app))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })

	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("ETP demo server: ws://%s/socket", address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func auth(_ context.Context, peer *etp.Peer, request etp.AuthRequest) (etp.AuthResult, error) {
	if string(request.Payload) != "demo-token" {
		return etp.AuthResult{OK: false, Reason: "invalid demo token"}, nil
	}
	if err := peer.SetRateLimitID("browser-demo"); err != nil {
		return etp.AuthResult{}, err
	}
	return etp.AuthResult{OK: true, UserID: "browser-demo"}, nil
}

func ping(ctx *etp.Context) error {
	body, err := ctx.Bytes()
	if err != nil {
		return err
	}
	var request struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return fmt.Errorf("decode ping: %w", err)
	}
	return respondJSON(ctx, "ping", map[string]any{
		"message":    "pong",
		"echo":       request.Message,
		"serverTime": time.Now().Format(time.RFC3339Nano),
	})
}

func upload(ctx *etp.Context) error {
	started := time.Now()
	reader, err := ctx.Body.Open()
	if err != nil {
		return err
	}
	defer reader.Close()

	hash := sha256.New()
	size, err := io.Copy(hash, reader)
	if err != nil {
		return fmt.Errorf("read upload: %w", err)
	}
	name := ctx.Field("name")
	digest := hex.EncodeToString(hash.Sum(nil))
	log.Printf("uploaded name=%q size=%d sha256=%s", name, size, digest)
	return respondJSON(ctx, "file.upload", map[string]any{
		"name":          name,
		"size":          size,
		"sha256":        digest,
		"elapsedMillis": time.Since(started).Milliseconds(),
	})
}

func respondJSON(ctx *etp.Context, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = ctx.Respond(etp.SendOptions{Event: event, Data: data})
	return err
}

func logProgress(progress etp.Progress) {
	if progress.TotalBytes == 0 {
		return
	}
	done := max(progress.LocalWrittenBytes, progress.RemoteAckedBytes)
	bucket := done * 10 / progress.TotalBytes
	previous, loaded := progressBuckets.LoadOrStore(progress.TransferID, bucket)
	if loaded && bucket <= previous.(uint64) {
		return
	}
	if loaded {
		progressBuckets.Store(progress.TransferID, bucket)
	}
	log.Printf("transfer=%d progress=%d%% state=%v", progress.TransferID, min(bucket*10, 100), progress.State)
	if bucket >= 10 {
		progressBuckets.Delete(progress.TransferID)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
