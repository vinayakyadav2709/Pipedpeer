.PHONY: build install test test-integration fmt lint lab-up lab-down bootstrap

build:
	./scripts/build.sh

install:
	./scripts/install-pipedpeer.sh

test:
	./scripts/test.sh

test-integration:
	./scripts/test-integration.sh

bootstrap:
	sudo ./scripts/bootstrap/all.sh

fmt:
	./scripts/fmt.sh

lint:
	./scripts/lint.sh

lab-up:
	./scripts/lab-up.sh

lab-down:
	./scripts/lab-down.sh
