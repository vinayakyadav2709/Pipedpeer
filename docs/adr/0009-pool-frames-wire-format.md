# Pool work travels as frames: a JSON header and length-prefixed raw pickles

A `/v1/pool/map` body is one small JSON header line followed by 4-byte big-endian length-prefixed pickle frames, so large items skip base64 and a giant JSON parse (`e9c9a18`, `src/internal/daemonapi/pool.go` `putFrame`, `readFrame`). Legacy base64-in-JSON is still accepted and emitted when the submitter did not opt in. Every reply path must answer in the format it was asked in: the over-memory micro-chunk path once replied JSON to a frames caller, which read zero results and redid the work locally (`0cffbbb`).
