# Sandbox Benchmark Comparison

Date: 2026-08-25T13:57:02+05:30

## Methodology
- Warmup: 5 iterations, Measured: 20 iterations per sandbox per task
- Measures total wall-clock time from sandbox start to process exit
- All tests on same machine, same conditions

## Results

| Task | Metric | bwrap (ms) | crun (ms) | Diff (ms) | Diff (%) |
|---|---|---|---|---|---|
| trivial | Average | 5.090 | 22.382 | +17.292 | +339.71% |
|  | Minimum | 4.147 | 16.532 | +12.385 | |
|  | Maximum | 6.305 | 28.337 | +22.033 | |
| medium | Average | 8.153 | 23.519 | +15.366 | +188.48% |
|  | Minimum | 6.890 | 17.657 | +10.767 | |
|  | Maximum | 9.642 | 27.822 | +18.180 | |
| python | Average | 17.085 | 37.960 | +20.875 | +122.18% |
|  | Minimum | 14.859 | 31.232 | +16.373 | |
|  | Maximum | 19.811 | 46.572 | +26.761 | |

## Raw Data (all measured iterations)

| Task | Iter | bwrap (ms) | crun (ms) |
|---|---|---|---|
| trivial | 0 | 5.513 | 24.654 |
| trivial | 1 | 6.305 | 25.569 |
| trivial | 2 | 5.770 | 21.950 |
| trivial | 3 | 5.987 | 24.680 |
| trivial | 4 | 5.595 | 28.337 |
| trivial | 5 | 5.907 | 19.580 |
| trivial | 6 | 4.151 | 24.093 |
| trivial | 7 | 5.112 | 22.774 |
| trivial | 8 | 4.147 | 21.254 |
| trivial | 9 | 4.519 | 25.531 |
| trivial | 10 | 5.234 | 20.736 |
| trivial | 11 | 5.026 | 27.670 |
| trivial | 12 | 5.171 | 19.938 |
| trivial | 13 | 4.640 | 25.997 |
| trivial | 14 | 4.771 | 18.904 |
| trivial | 15 | 4.326 | 18.067 |
| trivial | 16 | 5.070 | 19.503 |
| trivial | 17 | 5.187 | 20.387 |
| trivial | 18 | 4.646 | 21.484 |
| trivial | 19 | 4.726 | 16.532 |
| medium | 0 | 7.577 | 20.402 |
| medium | 1 | 8.128 | 18.821 |
| medium | 2 | 7.172 | 20.473 |
| medium | 3 | 8.257 | 17.657 |
| medium | 4 | 9.642 | 21.957 |
| medium | 5 | 9.323 | 26.559 |
| medium | 6 | 7.093 | 26.076 |
| medium | 7 | 7.160 | 23.761 |
| medium | 8 | 6.890 | 21.912 |
| medium | 9 | 8.133 | 22.782 |
| medium | 10 | 7.947 | 27.755 |
| medium | 11 | 8.830 | 26.800 |
| medium | 12 | 7.752 | 23.593 |
| medium | 13 | 7.887 | 24.680 |
| medium | 14 | 7.431 | 19.583 |
| medium | 15 | 7.728 | 23.830 |
| medium | 16 | 9.286 | 21.286 |
| medium | 17 | 8.788 | 27.563 |
| medium | 18 | 8.788 | 27.822 |
| medium | 19 | 9.242 | 27.069 |
| python | 0 | 14.859 | 39.551 |
| python | 1 | 16.215 | 35.998 |
| python | 2 | 15.939 | 42.908 |
| python | 3 | 16.205 | 35.277 |
| python | 4 | 16.100 | 37.351 |
| python | 5 | 17.152 | 41.444 |
| python | 6 | 16.538 | 34.448 |
| python | 7 | 16.976 | 35.288 |
| python | 8 | 18.731 | 39.987 |
| python | 9 | 19.811 | 43.512 |
| python | 10 | 15.507 | 35.468 |
| python | 11 | 19.701 | 39.448 |
| python | 12 | 16.749 | 32.653 |
| python | 13 | 17.497 | 31.232 |
| python | 14 | 16.125 | 38.592 |
| python | 15 | 17.843 | 35.088 |
| python | 16 | 17.746 | 36.339 |
| python | 17 | 15.870 | 43.379 |
| python | 18 | 17.520 | 34.670 |
| python | 19 | 18.616 | 46.572 |
