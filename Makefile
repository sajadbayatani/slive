APP_NAME := slive

.PHONY: run
run:
	go run ./cmd/slive

.PHONY: build
build:
	go build -o bin/$(APP_NAME) ./cmd/slive

.PHONY: test
test:
	go test ./...

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: check
check:
	go fmt ./...
	go vet ./...
	go test ./...

.PHONY: clean
clean:
	rm -rf bin/

.PHONY: docker-build
docker-build:
	docker build -t $(APP_NAME):local .

.PHONY: docker-run
docker-run:
	docker run --rm -p 8080:8080 $(APP_NAME):local