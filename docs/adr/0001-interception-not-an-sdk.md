# Distribute by intercepting Python primitives, never through an SDK

The product is `pipedpeer python main.py` on an unmodified script, so there is no `import pipedpeer` and no decorator API: a `sitecustomize` shim injected into the job's environment replaces `multiprocessing.Pool`, `ProcessPoolExecutor`, the joblib backend, numpy/pandas hot paths and torch DDP at runtime (`6e34c08`; always on since `3609328`, per-job opt-out `PIPEDPEER_SHIM=0`, `src/internal/daemonapi/execution.go` `handleJobExec`). Process pools are safe to intercept because Python already forces their work to be picklable and share-nothing; the program says what is parallel by calling the primitive.

## Considered Options

- An SDK: rejected by the owner; it breaks the zero-code-change thesis.
- Static analysis to split loops: proving iteration independence in Python is impractical, and a wrong guess corrupts results rather than losing a speedup.
