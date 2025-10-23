FROM golang:1.25.2 AS build
WORKDIR /src/lockd
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /lockd ./cmd/lockd

FROM scratch
COPY --from=build /lockd /lockd
EXPOSE 8080
ENTRYPOINT ["/lockd"]
