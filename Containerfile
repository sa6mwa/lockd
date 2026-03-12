ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM golang:1.25.7 AS build
WORKDIR /src/lockd
COPY go.mod go.sum ./
RUN go mod download
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=.,target=/src/lockd,readonly \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -trimpath -ldflags="-s -w" -o /out/lockd ./cmd/lockd

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /out/lockd /opt/lockd/bin/lockd
ENV LOCKD_CONFIG_DIR=/opt/lockd/config \
    LOCKD_STORE=disk:///opt/lockd/storage
WORKDIR /opt/lockd/config
WORKDIR /opt/lockd/storage
WORKDIR /opt/lockd
VOLUME ["/opt/lockd/config", "/opt/lockd/storage"]
EXPOSE 9341
EXPOSE 9464
ENTRYPOINT ["/opt/lockd/bin/lockd"]
