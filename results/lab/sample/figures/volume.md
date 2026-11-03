Sample: 152 runs, 3 PCs, 5138 s of runs. Projection: 17 PCs, 318 min (258 min of experiments from the profile + 60 min setup and cleanup), 282 runs.

| source | files | records | bytes (sample) | bytes per run | projected day, raw | projected, gzip |
|---|---|---|---|---|---|---|
| runs.jsonl (one line per run; receipts inline) | 1 | 152 | 8.0 MB | 52.7 kB | 14.9 MB | 966.1 kB |
| pipedpeer record.json (steps per machine, chunk) | 58 | 58 | 38.3 kB | 252 B | 71.0 kB | 27.8 kB |
| run output.log | 161 | 161 | 20.1 kB | 133 B | 37.4 kB | 36.5 kB |
| runner logs (logs/<exp>.log) | 6 | 191 lines | 9.9 kB | 65 B | 18.4 kB | 4.9 kB |
| OTel host metrics (cpu, memory, network, load; 1 s) | 1 | 720630 datapoints | 182.8 MB | 1.2 MB | 3.7 GB | 118.0 MB |
| cgroup CPU (sample only; not on the lab) | 3 | 3340 datapoints | 1.0 MB | 6.8 kB | 200.3 MB | 8.1 MB |
| OTel logs (daemon log + runs.jsonl copy) | 1 | 2347 log records | 31.7 MB | 208.7 kB | 641.3 MB | 28.4 MB |
| clocks.txt | 1 | 7 lines | 593 B | 4 B | 3.4 kB | 1.8 kB |
| machines.csv | 1 | 4 lines | 508 B | 3 B | 2.9 kB | 1.5 kB |
| network.csv | 0 | 0 lines | 0 B | 0 B | 0 B | 0 B |

**Projected day: 4.5 GB raw, 155.5 MB gzipped, against the 8 GB R2 cap (57% / 2%).**
