package swagger

//go:generate swag init --generalInfo swagger.go --output docs --dir .,../internal/httpapi,../api --parseInternal --parseDependency --generatedTime=false
//go:generate go run ./internal/swaggerhtml --spec docs/swagger.json --out docs/swagger.html

// @title           lockd API
// @version         0.0
// @description     lockd exposes exclusive lease management, atomic JSON checkpoints, and resilient work queues over HTTPS.
// @contact.name    Michel Blomgren
// @contact.email   sa6mwa@gmail.com
// @contact.url     https://pkt.systems
// @license.name    MIT
// @license.url     https://opensource.org/license/mit/
// @BasePath        /v1
// @schemes         https http
// @accept          json
// @produce         json
// @tag.name        lease
// @tag.description Lease acquisition, keepalive, release, and state management.
// @tag.name        queue
// @tag.description Queue enqueue, dequeue, acknowledgement, and lifecycle operations.
// @tag.name        system
// @tag.description Service health and readiness probes.
// @securityDefinitions.apikey  mTLS
// @in                          header
// @name                        X-Client-Cert
// @description                 Mutual TLS is required in production deployments; place holder header documents client certificate identity propagation.

// Package swagger provides go:generate hooks for producing OpenAPI assets.
type Package struct{}
