module github.com/elum-utils/etp-socket/integration/go-server

go 1.25.0

require (
	github.com/elum-utils/go-etp v0.0.0
	github.com/elum-utils/go-etp/adapters/gorilla v0.0.0
)

require github.com/gorilla/websocket v1.5.3 // indirect

replace github.com/elum-utils/go-etp => ../../../go-etp

replace github.com/elum-utils/go-etp/adapters/gorilla => ../../../go-etp/adapters/gorilla
