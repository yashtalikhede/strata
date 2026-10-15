BINARY := bin/strata
VERSION ?= dev
GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
RESULTS := bench/results
FUZZTIME ?= 10s

.PHONY: build test test-short vet fmt-check check bench fuzz cover

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

# Per-package coverage. Not part of check: it cannot run with -race in one
# pass and the number is a thing to look at, not a gate.
cover:
	go test -cover ./...

# Each fuzz target in turn. Go only fuzzes one target per run, so these are
# separate invocations; -run '^$$' keeps the ordinary tests out of the way.
fuzz:
	go test -run '^$$' -fuzz=FuzzReplay -fuzztime=$(FUZZTIME) ./internal/wal
	go test -run '^$$' -fuzz=FuzzUnmarshal -fuzztime=$(FUZZTIME) ./internal/bloom
	go test -run '^$$' -fuzz=FuzzOpen -fuzztime=$(FUZZTIME) ./internal/sstable
	go test -run '^$$' -fuzz=FuzzManifestDecode -fuzztime=$(FUZZTIME) ./internal/db

# A full benchmark run, written to a file named for the platform so results
# from different machines sit side by side.
bench:
	go run ./cmd/strata bench \
		-md $(RESULTS)/$(GOOS)-$(GOARCH).md \
		-json $(RESULTS)/$(GOOS)-$(GOARCH).json
