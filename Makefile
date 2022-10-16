all: build test

build:
	go build

test:
	go test ./...
	git restore testdata

cover:
	go test -coverprofile=coverage.out
	git restore testdata
	go tool cover -html=coverage.out
