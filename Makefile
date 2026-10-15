BINARY := bin/strata
VERSION ?= dev

.PHONY: build test test-short vet fmt-check check

build:
	go build -ldflags "-X main.version=$(VERSION)" -o ./$(BINARY) ./cmd/strata

test:
	go test -race ./...

# The same suite with the slow tests trimmed: fewer model operations and one
# process-kill round instead of four. Useful while iterating; check still runs
# the full thing.
test-short:
	go test -short -race ./...

vet:
	go vet ./...

fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; \
	fi

check: fmt-check vet test build
