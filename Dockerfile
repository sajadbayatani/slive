FROM golang:1.24 AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.date=$DATE" -o /bin/slive ./cmd/slive


FROM gcr.io/distroless/static-debian12

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

LABEL org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.source="https://github.com/sajadbayatani/slive" \
      org.opencontainers.image.created=$DATE

COPY --from=builder /bin/slive /slive

EXPOSE 8080

ENTRYPOINT ["/slive"]
