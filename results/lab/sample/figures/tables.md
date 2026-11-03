Median wall s [IQR] per group. Warm-ups and failed runs excluded and counted.

### e1

| workload | system | N | mode | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|
| W1 | plain | 1 | strong | 8.40 [8.00-9.03] | 1.00 | 3 | 1 | 0 |
| W1 | pipedpeer | 1 | strong | 2.63 [2.44-2.98] | 3.19 | 3 | 1 | 0 |
| W1 | pipedpeer | 2 | strong | 3.25 [2.95-3.33] | 2.58 | 3 | 1 | 0 |
| W1 | ray | 1 | strong | 10.23 [9.97-10.47] | 0.82 | 3 | 1 | 0 |
| W1 | ray | 2 | strong | 8.21 [8.14-8.43] | 1.02 | 3 | 1 | 0 |
| W1 | dask | 1 | strong | 11.74 [11.21-12.30] | 0.72 | 3 | 1 | 0 |
| W1 | dask | 2 | strong | 8.05 [7.51-8.30] | 1.04 | 3 | 1 | 0 |
| W2 | plain | 1 | strong | 10.09 [8.59-12.47] | 1.00 | 3 | 1 | 0 |
| W2 | pipedpeer | 1 | strong | 11.83 [9.83-11.98] | 0.85 | 3 | 1 | 0 |
| W2 | pipedpeer | 2 | strong | 15.82 [15.14-16.50] | 0.64 | 2 | 1 | 1 |
| W2 | ray | 1 | strong | 13.03 [12.01-18.71] | 0.77 | 3 | 1 | 0 |
| W2 | ray | 2 | strong | 23.00 [20.60-41.76] | 0.44 | 3 | 1 | 0 |
| W2 | dask | 1 | strong | 17.88 [15.27-20.44] | 0.56 | 3 | 1 | 0 |
| W2 | dask | 2 | strong | 15.28 [13.02-15.98] | 0.66 | 3 | 1 | 0 |
| W3 | plain | 1 | strong | 7.96 [6.49-7.99] | 1.00 | 3 | 1 | 0 |
| W3 | pipedpeer | 1 | strong | 5.17 [5.17-6.67] | 1.54 | 3 | 1 | 0 |
| W3 | pipedpeer | 2 | strong | 6.96 [6.75-7.77] | 1.14 | 3 | 1 | 0 |
| W3 | ray | 1 | strong | 36.16 [32.61-39.72] | 0.22 | 2 | 1 | 1 |
| W3 | ray | 2 | strong | 18.05 [18.05-18.05] | 0.44 | 1 | 1 | 2 |
| W3 | dask | 1 | strong | 30.20 [23.94-31.31] | 0.26 | 3 | 1 | 0 |
| W3 | dask | 2 | strong | 14.89 [13.48-15.40] | 0.54 | 3 | 1 | 0 |

### e1b

| workload | system | N | mode | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|
| W1 | plain | 1 | weak | 12.27 [10.18-15.09] | 1.00 | 3 | 1 | 0 |
| W1 | pipedpeer | 1 | weak | 2.62 [2.54-2.96] | 1.00 | 3 | 1 | 0 |
| W1 | pipedpeer | 2 | weak | 10.76 [8.15-10.86] | 0.24 | 3 | 1 | 0 |
| W1 | ray | 1 | weak | 12.17 [11.39-17.80] | 1.00 | 3 | 1 | 0 |
| W1 | ray | 2 | weak | 21.99 [17.02-22.99] | 0.55 | 3 | 1 | 0 |
| W1 | dask | 1 | weak | 20.93 [16.58-23.87] | 1.00 | 3 | 1 | 0 |
| W1 | dask | 2 | weak | 25.89 [22.72-31.04] | 0.81 | 3 | 1 | 0 |

### e4

| workload | system | N | mode | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|
| W1 | pipedpeer | 2 | strong | 23.97 [23.10-24.89] | - | 3 | 1 | 0 |
| W1 | pipedpeer-headcount | 2 | strong | 22.07 [21.74-22.17] | - | 3 | 1 | 0 |
| W1 | ray | 2 | strong | 22.38 [19.31-26.64] | - | 3 | 1 | 0 |
| W1 | dask | 2 | strong | 21.71 [21.70-22.50] | - | 3 | 1 | 0 |

### e6

| workload | system | N | mode | fault | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|---|
| W1 | pipedpeer | 2 | strong |  | - | - | 0 | 1 | 0 |
| W1 | pipedpeer | 2 | strong | none | 4.38 [4.38-4.38] | - | 1 | 0 | 0 |
| W1 | pipedpeer | 2 | strong | kill -9 | 4.59 [4.59-4.59] | - | 1 | 0 | 0 |
| W1 | ray | 2 | strong |  | - | - | 0 | 1 | 0 |
| W1 | ray | 2 | strong | none | 7.90 [7.90-7.90] | - | 1 | 0 | 0 |
| W1 | ray | 2 | strong | kill -9 | 7.56 [7.56-7.56] | - | 1 | 0 | 0 |
| W1 | dask | 2 | strong |  | - | - | 0 | 1 | 0 |
| W1 | dask | 2 | strong | none | 7.18 [7.18-7.18] | - | 1 | 0 | 0 |
| W1 | dask | 2 | strong | kill -9 | 13.26 [13.26-13.26] | - | 1 | 0 | 0 |
| W1 | pipedpeer-ctrlc | 2 |  |  | 0.91 [0.91-0.91] | - | 1 | 0 | 0 |

### e7

| workload | system | N | mode | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|
| E7-trivial | pipedpeer-shim | 2 | strong | 0.30 [0.30-0.35] | - | 3 | 1 | 0 |
| E7-trivial | pipedpeer-noshim | 2 | strong | 0.13 [0.13-0.14] | - | 3 | 1 | 0 |

### e8

| workload | system | N | mode | variant | wall s, median [IQR] | speedup | used | warm-ups | failed |
|---|---|---|---|---|---|---|---|---|---|
| T1 | pipedpeer-exact | 2 | strong | mlp-c | 8.64 [8.64-8.64] | - | 1 | 1 | 0 |
| T1 | pipedpeer-fast | 2 | strong | mlp-c | 7.78 [7.78-7.78] | - | 1 | 1 | 0 |
| T1 | torchrun | 2 | strong | mlp-c | 11.41 [11.41-11.41] | - | 1 | 1 | 0 |
