FROM golang:1.24 AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /bin/slive \
    ./cmd/server


FROM gcr.io/distroless/static-debian12

COPY --from=builder /bin/slive /slive

EXPOSE 8080

ENTRYPOINT ["/slive"]