.PHONY: build console console-optional install test test-integration test-net fmt lint lab-up lab-down lab-fail lab-nat lab-emu-up lab-emu-down shim-check campaign-test campaign-lint campaign-test-files

build: console-optional
	cd src && CGO_ENABLED=0 go build -o ../bin/pipedpeer .

# Builds the `pipedpeer console` frontend into src/internal/console/web/dist,
# which the Go build embeds.
console:
	cd console && bun install --frozen-lockfile && bun run build

# Without bun the build still succeeds and `pipedpeer console` serves a stub
# page that says to run `make console`.
console-optional:
	@if command -v bun >/dev/null 2>&1; then \
		$(MAKE) console; \
	else \
		echo "bun not found: skipping the console frontend (pipedpeer console will serve a stub page)"; \
	fi

install:
	./scripts/install-pipedpeer.sh

test:
	./scripts/test.sh

test-integration:
	./scripts/test-integration.sh

# Network scenarios (E1-E6) against a fresh build; needs docker.
test-net: build
	PIPEDPEER=$(CURDIR)/bin/pipedpeer ./scripts/lab-net.sh

fmt:
	cd src && gofmt -w .

lint:
	cd src && go vet ./... && test -z "$$(gofmt -l .)"

# ruff + pytest over the job shim's Python; needs uv.
campaign-lint:
	shellcheck -x -S warning scripts/campaign/*.sh scripts/campaign/test/*.sh

# every campaign test file even when one fails, a few at once, each under its
# own HOME; the list itself is campaign-test-files and is not run directly
campaign-test:
	bash scripts/campaign/test/run_all.sh

campaign-test-files:
	bash scripts/campaign/test/run_all_test.sh
	bash scripts/campaign/test/lib_test.sh
	bash scripts/campaign/test/inventory_test.sh
	bash scripts/campaign/test/cluster_test.sh
	bash scripts/campaign/test/scan_test.sh
	bash scripts/campaign/test/install_test.sh
	bash scripts/campaign/test/metrics_test.sh
	bash scripts/campaign/test/upload_test.sh
	bash scripts/campaign/test/run_test.sh
	bash scripts/campaign/test/calibrate_test.sh
	bash scripts/campaign/test/stage_test.sh
	bash scripts/campaign/test/exp_test.sh
	bash scripts/campaign/test/ports_test.sh
	bash scripts/campaign/test/venv_test.sh
	bash scripts/campaign/test/lab_pc_test.sh
	bash scripts/campaign/test/install_warmup_test.sh
	bash scripts/campaign/test/cluster_dask_test.sh
	bash scripts/campaign/test/batch_run_test.sh
	bash scripts/campaign/test/e5_ready_test.sh
	bash scripts/campaign/test/e6_arms_test.sh
	bash scripts/campaign/test/e11_interval_test.sh
	bash scripts/campaign/test/pins_ray_dask_test.sh
	bash scripts/campaign/test/e17_18_test.sh
	bash scripts/campaign/test/e19_test.sh
	bash scripts/campaign/test/collect_test.sh
	bash scripts/campaign/test/collect_bug_test.sh
	bash scripts/campaign/test/cluster_cpus_test.sh
	bash scripts/campaign/test/preflight_test.sh
	bash scripts/campaign/test/wipe_test.sh
	python3 -m pytest -q scripts/campaign/test/test_analyse.py scripts/campaign/test/test_analyse_sweeps.py scripts/campaign/test/test_sweep.py scripts/campaign/test/test_cpu_sampler.py

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
