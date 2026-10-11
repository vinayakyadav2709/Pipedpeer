# No head node: the coordinator is a library in the CLI

Every machine runs the same daemon and the scheduler is linked into the short-lived CLI (`coordinator.New` in `src/main.go`: `run`, `runDDP`, `map`), so there is no scheduler process to run, fail or secure, and a machine is submitter and worker at once. The cost is that the submitting CLI must stay alive for the job ([system report 6.2](../system-report.md#62-limitations)).
