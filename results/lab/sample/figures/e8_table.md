| variant | ranks | system | runs used | wall s, median [IQR] | val loss | loss vs 1 rank | bytes sent per rank |
|---|---|---|---|---|---|---|---|
| mlp-c | 2 | pipedpeer-exact | 1 | 8.64 [8.64-8.64] | 0.0993 | - | 19,235,383 |
| mlp-c | 2 | pipedpeer-fast | 1 | 7.78 [7.78-7.78] | 0.0997 | - | 8,500,196 |
| mlp-c | 2 | torchrun | 1 | 11.41 [11.41-11.41] | 0.0977 | - | 68,433,060 |
