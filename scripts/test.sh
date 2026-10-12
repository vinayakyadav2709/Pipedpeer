#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# Detect container runtime (podman preferred, fallback to docker)
if command -v podman &>/dev/null; then
    RUNTIME="podman"
elif command -v docker &>/dev/null; then
    RUNTIME="docker"
else
    echo "Error: No container runtime found (podman/docker)"
    exit 1
fi

echo "==================== Running Pipedpeer Test Suite ===================="
mkdir -p "$repo_root/test_results"
rm -rf "$repo_root/test_results/integration_job_data"

if [ "$RUNTIME" = "podman" ]; then
    # Podman: Run tests directly on host (Go is available, no DinD needed)
    # Integration tests auto-skip when PIPEDPEER_INTEGRATION is not set
    echo "Running tests directly (runtime: $RUNTIME)..."
    cd src
    # Integration tests used to be unset here unconditionally, so `make test`
    # could never run them and a green result hid whatever they covered. They
    # are opt-in, not opt-out: export PIPEDPEER_INTEGRATION=1 to include them.
    go test -v -count=1 ./... > "$repo_root/test_results/unit_and_integration_tests.log" 2>&1
    test_exit=$?

    # A skip is not a pass. Print them, because a suite that quietly skipped
    # the tests carrying its central claim still prints ok.
    skipped="$(grep -cE '^\s*--- SKIP' "$repo_root/test_results/unit_and_integration_tests.log" || true)"
    if [[ "${skipped:-0}" -gt 0 ]]; then
        echo "Skipped $skipped test(s):"
        grep -E '^\s*--- SKIP' "$repo_root/test_results/unit_and_integration_tests.log" |
            sed -E 's/.*SKIP: ([^ ]+).*/  \1/' | sort -u
        echo "Run scripts/check-skips.sh to fail on a skipped distribution test."
    fi
    
    if [ $test_exit -ne 0 ]; then
        cat "$repo_root/test_results/unit_and_integration_tests.log"
        echo 'Tests failed!'
    else
        echo 'Tests passed successfully.'
    fi
else
    # Docker: use socket mount for DinD
    docker run --rm -v "$repo_root:/app" -w /app -v /var/run/docker.sock:/var/run/docker.sock golang:1.25 bash -c "
      apt-get update -qq && apt-get install -y docker.io openssh-server -qq
      # Set up passwordless SSH to localhost for app tests
      mkdir -p /run/sshd /root/.ssh
      ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N '' -q
      cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys
      chmod 600 /root/.ssh/authorized_keys
      echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
      /usr/sbin/sshd
      ssh-keyscan -H localhost >> /root/.ssh/known_hosts 2>/dev/null
      mkdir -p /usr/local/lib/docker/cli-plugins
      curl -sSL 'https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-linux-x86_64' -o /usr/local/lib/docker/cli-plugins/docker-compose
      chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
      
      echo 'Running unit and integration tests...'
      cd src
      PIPEDPEER_INTEGRATION=1 go test -v ./... > /app/test_results/unit_and_integration_tests.log 2>&1
      test_exit=\$?
      
      if [ \$test_exit -ne 0 ]; then
        cat /app/test_results/unit_and_integration_tests.log
        echo 'Tests failed!'
      else
        echo 'Tests passed successfully.'
      fi
      
      exit \$test_exit
    "
    test_exit=$?
fi

# A test that starts a server and does not stop it leaves it holding a port
# until somebody notices. Measured: 54 registry processes, the oldest running
# for over a day, from `go run` subprocesses whose wrapper was killed while
# the binary it spawned carried on. Counting them here is what turns that from
# something discovered by accident into something the suite reports.
leaked="$(ps -eo cmd= | grep -c '[p]ipedpeer registry --port' || true)"
if [[ "${leaked:-0}" -gt 0 ]]; then
	echo
	echo "LEAK: $leaked registry process(es) are still running after the suite."
	echo "      A test started one and did not stop it. Killing the wrapper of a"
	echo "      \`go run\` does not stop the binary it spawned - build it and run"
	echo "      that instead, so the process killed is the one serving."
	ps -eo pid=,etime=,cmd= | grep '[p]ipedpeer registry --port' | head -5
	test_exit=1
fi

echo
echo "==================== Test Summary ===================="
if [[ $test_exit -eq 0 ]]; then
	echo "Result:        PASS"
else
	echo "Result:        FAIL"
fi
echo "Logs saved to: test_results/unit_and_integration_tests.log"
echo "Job data saved to: test_results/integration_job_data/"
echo "======================================================"

exit "$test_exit"
