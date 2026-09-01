APP_NAME := slive
GOMODCACHE ?= $(PWD)/.gocache/mod
export GOMODCACHE

.PHONY: test test-internal smoke baseline lint vet fmt build check clean docker-build docker-run

test:
	GOMODCACHE="$(PWD)/.gocache/mod" go test ./... -race -count=1

test-internal:
	GOMODCACHE="$(PWD)/.gocache/mod" go test -tags slive_internal ./... -race -count=1

smoke:
	GOMODCACHE="$(PWD)/.gocache/mod" go build ./...
	GOMODCACHE="$(PWD)/.gocache/mod" go run ./examples/basic-room 2>&1 | tee /tmp/basic-room.log; test $${PIPESTATUS[0]} -eq 0; grep -q "basic-room: exit 0" /tmp/basic-room.log; grep -q "rooms_active:" /tmp/basic-room.log
	GOMODCACHE="$(PWD)/.gocache/mod" go run ./examples/publish-subscribe 2>&1 | tee /tmp/publish-subscribe.log; test $${PIPESTATUS[0]} -eq 0; grep -q "publish-subscribe: exit 0" /tmp/publish-subscribe.log; grep -q "forwarder_subscribers:" /tmp/publish-subscribe.log
	GOMODCACHE="$(PWD)/.gocache/mod" go run ./examples/health 2>&1 | tee /tmp/health.log; test $${PIPESTATUS[0]} -eq 0; grep -q "health: exit 0" /tmp/health.log; grep -q "status=ok" /tmp/health.log

baseline:
	GOMODCACHE="$(PWD)/.gocache/mod" go test -tags slive_internal ./test/scale -run TestScaleCapacity -count=1 -update-baseline

lint:
	test -z "$$(gofmt -l . 2>&1 | grep -v '^\.gocache/' | grep -v '^\.qwen/')"

vet:
	GOMODCACHE="$(PWD)/.gocache/mod" go vet ./...

fmt:
	go fmt ./...

build:
	GOMODCACHE="$(PWD)/.gocache/mod" go build -o bin/$(APP_NAME) ./cmd/slive

check:
	go fmt ./...
	GOMODCACHE="$(PWD)/.gocache/mod" go vet ./...
	GOMODCACHE="$(PWD)/.gocache/mod" go test ./... -race -count=1

clean:
	rm -rf bin/

docker-build:
	docker build -t $(APP_NAME):local .

docker-run:
	docker run --rm -p 8080:8080 $(APP_NAME):local

run:
	GOMODCACHE="$(PWD)/.gocache/mod" go run ./cmd/slive
