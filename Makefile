.PHONY: build run test

build:
	go build -o bin/realtime ./cmd/realtime

run:
	go run ./cmd/realtime

test:
	go test ./... -v