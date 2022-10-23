all: build test

build:
	go build -race
	go build -o bin/ssdb ./cmd/ssdb

test:
	go test -race ./...
	git restore testdata

bench:
	go test -bench=.

cover:
	go test -coverprofile=coverage.out
	git restore testdata
	go tool cover -html=coverage.out
