APP_NAME := slive
GOMODCACHE ?= $(PWD)/.gocache/mod
export GOMODCACHE

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: test test-internal smoke baseline lint vet fmt build check clean docker-build docker-buildx docker-run version dist cover cover-html

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
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(APP_NAME) ./cmd/slive

version:
	@echo $(VERSION)

dist:
	rm -rf dist
	mkdir -p dist
	for GOOS in linux darwin; do \
	  for GOARCH in amd64 arm64; do \
	    echo "Building slive_$${GOOS}_$${GOARCH}..."; \
	    CGO_ENABLED=0 GOOS=$${GOOS} GOARCH=$${GOARCH} go build -trimpath -ldflags "$(LDFLAGS)" -o dist/slive_$${GOOS}_$${GOARCH}/slive ./cmd/slive; \
	    cp README.md LICENSE dist/slive_$${GOOS}_$${GOARCH}/; \
	    tar -czf dist/slive_$(VERSION)_$${GOOS}_$${GOARCH}.tar.gz -C dist/slive_$${GOOS}_$${GOARCH} slive README.md LICENSE; \
	    rm -rf dist/slive_$${GOOS}_$${GOARCH}; \
	  done; \
	done
	if command -v sha256sum >/dev/null 2>&1; then \
	  sha256sum dist/*.tar.gz > dist/checksums.txt; \
	else \
	  shasum -a 256 dist/*.tar.gz > dist/checksums.txt; \
	fi
	cat dist/checksums.txt

cover:
	go test ./... -coverprofile=coverage.out && go tool cover -func=coverage.out

cover-html:
	go tool cover -html=coverage.out -o coverage.html

check:
	go fmt ./...
	GOMODCACHE="$(PWD)/.gocache/mod" go vet ./...
	GOMODCACHE="$(PWD)/.gocache/mod" go test ./... -race -count=1

clean:
	rm -rf bin/

docker-build:
	docker build -t $(APP_NAME):local .

docker-buildx:
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t $(APP_NAME):$(VERSION) .

docker-run:
	docker run --rm -p 8080:8080 $(APP_NAME):local

run:
	GOMODCACHE="$(PWD)/.gocache/mod" go run ./cmd/slive
