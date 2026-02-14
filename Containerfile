ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM golang:1.25.7 AS build
WORKDIR /src/lockd
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/lockd ./cmd/lockd

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /out/lockd /lockd
ENV LOCKD_CONFIG_DIR=/config \
    LOCKD_STORE=disk:///storage
WORKDIR /
VOLUME ["/config", "/storage"]
EXPOSE 9341
EXPOSE 9464
ENTRYPOINT ["/lockd","--bootstrap","/config"]
