.PHONY: build install test test-integration fmt lint lab-up lab-down lab-fail lab-nat

build:
	./scripts/build.sh

install:
	./scripts/install-pipedpeer.sh

test:
	./scripts/test.sh

test-integration:
	./scripts/test-integration.sh

fmt:
	./scripts/fmt.sh

lint:
	./scripts/lint.sh

lab-up:
	./scripts/lab-up.sh

lab-down:
	./scripts/lab-down.sh

lab-fail:
	./scripts/lab-fail.sh

lab-nat:
	./scripts/lab-nat.sh
