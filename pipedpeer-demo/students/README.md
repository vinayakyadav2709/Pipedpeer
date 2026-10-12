# Student-style scripts

Plain scripts written the way a student would write them: no tuning, no
pipedpeer imports, default `Pool()` sizes. They stand in for "show it on my
own code" when a teacher has nothing to hand over, and they test that
pipedpeer copes with code nobody shaped for it.

Each one runs under plain `python3` too and prints `TOTAL <s>` at the end, so
the two runs compare directly:

    python3 primes.py               # this machine's cores
    pipedpeer run primes.py         # the cluster's cores

| Script | Parallel call pipedpeer picks up | Needs | Writes |
|---|---|---|---|
| monte_carlo_pi.py | `multiprocessing.Pool().map` | stdlib | - |
| primes.py | `ProcessPoolExecutor().map` | stdlib | - |
| word_count.py | `Pool().map` | stdlib | counts.txt |
| grid_search.py | `GridSearchCV(n_jobs=-1)` (joblib) | scikit-learn | - |
| image_blur.py | `Pool().map` over numpy tiles | numpy | blurred_preview.pgm |

Sizes are constants at the top of each file. They aim at 20-90 s with
`python3` on one 4-8 core laptop; real times come from the rehearsal.

Things worth knowing when showing them:

- primes.py has uneven ranges (higher numbers cost more), so the last ranges
  finish last on any machine.
- word_count.py and image_blur.py write files on whichever machine runs the
  script; with `--remote` they come back to the folder you ran from.
- image_blur.py ships about 50 MB of tiles to the other machines; on slow
  Wi-Fi that transfer is visible in the time.
- `--check-only` first shows what pipedpeer found in the file:
  `pipedpeer run grid_search.py --check-only`.
