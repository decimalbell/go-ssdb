all: build test

build:
	go build

test:
	go test ./...
	git restore testdata

cover:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out
