package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"

	etp "github.com/elum-utils/go-etp"
	etpgorilla "github.com/elum-utils/go-etp/adapters/gorilla"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18991", "listen address")
	flag.Parse()

	app := etp.New(etp.Config{MaxMemoryBody: 4 << 10})
	if err := app.OnAuth(func(_ context.Context, _ *etp.Peer, request etp.AuthRequest) (etp.AuthResult, error) {
		if string(request.Payload) != "integration-token" {
			return etp.AuthResult{OK: false, Reason: "invalid integration token"}, nil
		}
		return etp.AuthResult{OK: true, UserID: "integration-client"}, nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := app.OnError(func(ctx *etp.Context, err error) {
		log.Printf("handler error event=%q request=%d: %v", ctx.Event, ctx.RequestID, err)
	}); err != nil {
		log.Fatal(err)
	}
	if err := app.On("echo", echo); err != nil {
		log.Fatal(err)
	}
	if err := app.On("multipart", multipart); err != nil {
		log.Fatal(err)
	}
	if err := app.On("client.confirm", clientConfirmed); err != nil {
		log.Fatal(err)
	}
	if err := app.OnConnect(func(ctx context.Context, peer *etp.Peer) error {
		if err := peer.Session().SendText("server-ready"); err != nil {
			return err
		}
		_, err := peer.Request(context.Background(), etp.SendOptions{Event: "client.confirm", Data: []byte(`{"challenge":"go"}`)})
		return err
	}); err != nil {
		log.Fatal(err)
	}
	app.Compile()

	server := &http.Server{Addr: *addr, Handler: (&etpgorilla.Adapter{}).Handler(app)}
	log.Printf("ETP_TEST_READY %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func multipart(ctx *etp.Context) error {
	body, err := ctx.Bytes()
	if err != nil {
		return err
	}
	if ctx.Field("dialog") != `"dialog-1"` || string(body) != "onetwo" {
		return errors.New("invalid multipart payload")
	}
	_, err = ctx.Respond(etp.SendOptions{Event: "multipart", Data: []byte(`{"ok":true}`)})
	return err
}

func clientConfirmed(ctx *etp.Context) error {
	body, err := ctx.Bytes()
	if err != nil {
		return err
	}
	if string(body) != `{"accepted":true}` {
		return errors.New("invalid client response")
	}
	_, err = ctx.Peer.Send(context.Background(), etp.SendOptions{Event: "client.confirmed", Data: []byte(`{"ok":true}`)})
	return err
}

func echo(ctx *etp.Context) error {
	log.Printf("echo event=%q request=%d transfer=%d bytes=%d", ctx.Event, ctx.RequestID, ctx.TransferID, ctx.Body.Size())
	reader, err := ctx.Body.Open()
	if err != nil {
		return err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	handle, err := ctx.Respond(etp.SendOptions{Event: "echo", Data: data})
	if err == nil && handle.TransferID != 0 {
		go func() { log.Printf("response transfer=%d done: %v", handle.TransferID, <-handle.Done()) }()
	}
	return err
}
