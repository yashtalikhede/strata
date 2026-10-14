BINARY := bin/strata
VERSION ?= dev

.PHONY: build test vet fmt-check check

build:
	go build -ldflags "-X main.version=$(VERSION)" -o ./$(BINARY) ./cmd/strata

test:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; \
	fi

check: fmt-check vet test build
