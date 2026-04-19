#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==================== Running Pipedpeer Test Suite in Docker ===================="
mkdir -p "$repo_root/test_results"
rm -rf "$repo_root/test_results/integration_job_data"

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
  cd cmd/pipedpeer
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
