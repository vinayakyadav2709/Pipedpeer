# Lab day: the operator's page

You type one line on every lab PC. The last PC, pc00, then runs the whole day on its own, about 4 hours for 17 PCs, and ends with a summary on its screen. You do not need a laptop. If something on screen is not described here, photograph it and go on: nothing you type can lose results.

Background, if you want it: [scripts/campaign/HEADLESS.md](../../scripts/campaign/HEADLESS.md).

## What to bring

- **The passphrase**, on paper, from `publish.sh`. Never keep it in the repo, in chat or in a photo.
- **The lab account**: user name and password of the account you log in with on the lab PCs. Use the same account on every PC.
- **This page, printed**, or open on your phone.
- **Your phone**, to photograph any `ERROR`.
- **Time**: about 30 minutes to type the line on 17 PCs, then about 3.5 hours unattended (the corrected estimate is in [HEADLESS.md](../../scripts/campaign/HEADLESS.md#time)).

## Before the lab (at home, the day before)

1. On `dev`, with the campaign branches merged and pushed:

   ```sh
   scripts/campaign/publish.sh
   ```

   Press Enter for a generated passphrase and write it on paper. Your Cloudflare upload credentials (`~/.config/pipedpeer-lab/ingest.env`) go into the encrypted bundle.
2. Commit the two files it lists, `landing/public/lab.sh` and `landing/public/lab-secrets.enc`, to `dev` and push. Wait for the Pages deploy to finish (GitHub, Actions tab).
3. Check the live site:

   ```sh
   scripts/campaign/publish.sh selftest
   scripts/campaign/ingest/ingest.sh selftest
   ```

   The first command asks for the passphrase and must end with no error. The second must print `selftest ok`. It leaves one 4 kB object under `selftest/` in the bucket.

## In the lab

### 1. Every PC except one: the worker line

Log in as the lab account, open a terminal, and type:

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash
```

If sudo asks, type the account's password. The PC may install the SSH server first, which takes up to a minute. Then it shows:

```text
LAB-PC host=lab-114 ip=10.20.1.114 user=student cores=8 ram_gib=16 machine_id=…

==================================================================
  DONE: this PC is ready, you can leave it.   (ip 10.20.1.114)
==================================================================
```

Leave it logged in or log out; either is fine. Go to the next PC.

### 2. The last PC (pc00): the head line

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash -s -- head
```

1. It asks `Campaign passphrase:`. Type it from the paper; nothing shows while you type. You have three tries.
2. It sets this PC up like the others, downloads the code, and shows:

   ```text
   The lab day runs on its own now; it ends with a summary here.
   You may close this terminal. …
   ```

3. Then the day's log scrolls. The first part waits for the other PCs:

   ```text
   [day 10:02:11] discover: 12 of 17 PCs answered
   ```

   The day waits for 17 PCs, pc00 included. If fewer PCs are free, type `head 9` instead of `head` at the end of the line, to wait for 9. If the count does not arrive within 30 minutes, the day goes on with the PCs it found.
4. After that, lines like these scroll for hours. That is normal:

   ```text
   [day 10:41:52] == exp.e1: run.sh e1 --inventory … --n 1,4,16 --repeats 3
   [10:42:03 e1] W1 pipedpeer n=4 repeat=2
   ```

   A line with `BLOCKER:` is a problem the day wrote down and stepped around, for example a PC that stopped answering. It does not stop the day.

You can close the terminal, log out, or let the screen lock. The day runs as a system service on pc00. Do not switch pc00 off and do not unplug it.

### 3. Looking in: `status`

On pc00, in any terminal:

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash -s -- status
```

```text
unit pipedpeer-lab-day: active
lab day on lab-101, started 10:02
phase:    run
now:      experiment e4
progress: 17 of 25 steps done
ETA:      about 13:40 (98 min left, estimated)
problems so far: 1 (results/lab/BLOCKERS.md)
```

`sudo journalctl -fu pipedpeer-lab-day` shows the full log.

### 4. If the day stops: resume

If pc00 rebooted, lost power, or the head screen says `The lab day stopped before the end`, type the **head line again** on pc00. It asks for the passphrase again and continues from the last finished step. An experiment that was cut off runs again from its start. Its earlier lines are kept but marked, and the analysis does not count them twice.

### 5. The end screen

The day cleans every worker, uploads everything, checks the upload count, cleans pc00 last and prints:

```text
======================================================================
  PipedPeer lab day: summary (2026-11-20 14:02)

  PCs: 17 found of 17 expected (pc00 included)

  Experiments:
    e1    ok        432 runs, 0 failed
    …
  Results in Cloudflare R2 (bucket pipedpeer-lab):
    raw/        results/lab: runs.jsonl, CSVs, figures, logs, pcap/
    raw/otel/   telemetry (host metrics, traces, daemon logs)
    local:  2113 objects in …
    remote: 2113 objects accepted by the ingest Worker

  ALL DONE, the lab PCs are back as they were. You can leave.
======================================================================
```

| Last line | What to do |
|---|---|
| `ALL DONE, the lab PCs are back as they were. You can leave.` | Leave. |
| `DONE, but these PCs are not fully back as they were:` and one line per PC | Photograph it. On each PC listed, type `curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh \| sudo bash -s -- undo`, then leave. |
| `pc00 is NOT restored yet: its results are not confirmed in R2.` | The upload did not finish. Check that pc00 has internet, then type the head line again. pc00 keeps every result until the upload count matches. |

`status` shows the same summary after the end.

## When something says ERROR

lab.sh prints `[lab] ERROR: …` and stops on that PC. Running a line again is always safe. Photograph the screen, then:

| Message | Meaning | What to do |
|---|---|---|
| `run with sudo` | The line was typed without `sudo` | Type it again exactly as above. |
| `this copy of lab.sh is not published yet` | `publish.sh` was not run, or its commit is not on Pages | Stop; the at-home steps are not done. |
| `wrong passphrase` | Three wrong tries on pc00 | Type the head line again and use the paper. |
| `cannot download …` | No internet on this PC | Check the cable or Wi-Fi, then type the line again. Workers download nothing else; pc00 needs internet all day. |
| `cannot tell this PC's IP (no default route)` | The PC is not on a network | As above. |
| `setup already ran for <user>; run undo first` | The PC was set up under another account | Log in as the account you used before, or type the `undo` line first. |
| `… not found on this PC; the head needs it` | pc00 lacks a basic tool | Pick another PC as pc00 and type the head line there instead; type the worker line on this one. |
| `ERROR: line N: … failed` | Anything else | Type the same line again. If it fails twice, skip this PC: the day works with the PCs that answered. |

## No visits: when the lab already has SSH

If every lab PC already runs an SSH server and the lab account has the same password on all of them, one command on pc00 replaces the walk round. On pc00, before the head line:

```sh
curl -fsSL https://codeload.github.com/vinayakyadav2709/Pipedpeer/tar.gz/<commit> | tar -xz
bash Pipedpeer-<commit>/scripts/campaign/bootstrap-ssh.sh <lab subnet, e.g. 10.20.1.0/24> --user <lab account>
```

`<commit>` is the commit `publish.sh` printed; write it on the paper next to the passphrase. The script asks for the account's password once, and runs the worker line on every PC that accepts it. It prints one `LAB-PC` line per PC, or `ERROR` with the reason. Then type the head line on pc00 as above, and delete the `Pipedpeer-<commit>` directory.

## After the lab (at home)

1. Rotate the ingest token, so the copy in the published bundle stops working. In `scripts/campaign/ingest`:

   ```sh
   new=$(openssl rand -hex 32)
   printf %s "$new" | npx wrangler secret put INGEST_TOKEN --profile personal
   sed -i "s/^INGEST_TOKEN=.*/INGEST_TOKEN=$new/" ~/.config/pipedpeer-lab/ingest.env
   ```

2. Delete the campaign SSH key: `rm ~/.config/pipedpeer-lab/campaign_ed25519 ~/.config/pipedpeer-lab/campaign_ed25519.pub`.
3. Remove the bundle from `dev`: `git rm landing/public/lab-secrets.enc`, restore the placeholders with `git checkout <commit before publish> -- landing/public/lab.sh`, commit and push. Git history keeps the old bundle, but it now holds a deleted key and a dead token.
4. Read the data back with `npx wrangler r2 object get pipedpeer-lab/<key> --remote --profile personal`, or ask your agent to fetch `raw/` and start the analysis.
