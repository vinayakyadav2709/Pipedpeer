.PHONY: build install test test-integration test-net fmt lint lab-up lab-down lab-fail lab-nat lab-emu-up lab-emu-down shim-check

build:
	cd src && CGO_ENABLED=0 go build -o ../bin/pipedpeer .

install:
	./scripts/install-pipedpeer.sh

test:
	./scripts/test.sh

test-integration:
	./scripts/test-integration.sh

# Network scenarios (E1-E5) against a fresh build; needs docker.
test-net: build
	PIPEDPEER=$(CURDIR)/bin/pipedpeer ./scripts/lab-net.sh

fmt:
	cd src && gofmt -w .

lint:
	cd src && go vet ./... && test -z "$$(gofmt -l .)"

# ruff + pytest over the job shim's Python; needs uv.
shim-check:
	./scripts/shim-check.sh

lab-up:
	./scripts/lab-up.sh

lab-down:
	./scripts/lab-down.sh

lab-fail:
	./scripts/lab-fail.sh

lab-nat:
	./scripts/lab-nat.sh

lab-emu-up: build
	PIPEDPEER=$(CURDIR)/bin/pipedpeer ./scripts/lab-emu.sh up

lab-emu-down:
	./scripts/lab-emu.sh down
