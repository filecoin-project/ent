GO_BIN ?= go
all: install build gen
.PHONY: all

build:
	$(GO_BIN) build ./cmd/ent

install:
	$(GO_BIN) install ./cmd/ent

gen:
	$(GO_BIN) run ./gen/gen.go
.PHONY: gen