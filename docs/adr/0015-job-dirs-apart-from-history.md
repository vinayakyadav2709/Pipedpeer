# The daemon's job dirs live apart from the job history

The daemon worked in `~/.local/share/pipedpeer/jobs`, the directory `pipedpeer jobs` keeps history in, and pruned it hourly at seven days, so history older than a week vanished. The daemon now works in `.../pipedpeer/work` (`8f5a11f`, `defaultJobDir` in `src/internal/daemonapi/execution.go`); history stays under `jobhistory.BaseDir` (`src/internal/jobhistory/history.go`). In-flight state from the old directory was not migrated.
