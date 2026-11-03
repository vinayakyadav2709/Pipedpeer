# Ingest: lab data to R2, inside the free tier

All campaign data reaches the R2 bucket `pipedpeer-lab` on the operator's
personal Cloudflare account through one Worker, `pipedpeer-lab-ingest`
(`worker.js`). The Worker holds the only write access to the bucket and refuses
any upload that would cross the free tier, so a runaway trace cannot cost money.

Why a Worker: the Cloudflare CLI login (`wrangler`) can create buckets and
deploy Workers but cannot mint R2 S3 keys. A Worker also checks the budget on
the server, before anything is written.

## Caps

| | R2 free tier (per month) | Cap here |
|---|---|---|
| Storage | 10 GB-month | 8 GB stored (`CAP_BYTES`) |
| Class A ops (writes) | 1,000,000 | 200,000 uploads, 2 writes each (`CAP_PUTS`) |
| Class B ops (reads) | 10,000,000 | 1 read per upload |
| Worker requests | 100,000/day (free plan, never billed) | 50,000/day (`CAP_DAY`) |
| One object | 100 MB request body | 95 MB; `ingest.sh` splits at 90 MB |

The counters live in `_usage.json` in the bucket; overwriting a key counts its
bytes again, so they only ever overstate. A refused upload (HTTP 507) writes
nothing, and `ingest.sh` leaves the file local and reports it. The rest of the
account (`square-erp-catalog-*`) held 193 kB when this was set up.

## Use

```sh
scripts/campaign/ingest/ingest.sh selftest            # round-trip one small object
scripts/campaign/ingest/ingest.sh sync results/lab raw   # new or changed files, text gzipped
scripts/campaign/ingest/ingest.sh sync --final results/lab raw   # include files still being written
scripts/campaign/ingest/ingest.sh usage               # bytes, uploads, caps
scripts/campaign/ingest/ingest.sh count results/lab   # local files vs synced
```

Credentials: `~/.config/pipedpeer-lab/ingest.env` (mode 600) with `INGEST_URL`
and `INGEST_TOKEN`; never in the repo. Without it every command warns and exits 0.

## Deploy (operator's machine)

```sh
cd scripts/campaign/ingest
npx wrangler r2 bucket create pipedpeer-lab --profile personal   # once
npx wrangler deploy --profile personal
# new token: write ingest.env, then
printf %s "$INGEST_TOKEN" | npx wrangler secret put INGEST_TOKEN --profile personal
```

After the lab: rotate the token (`secret put` a new one) so the published
bundle's copy stops working. Read data back with
`npx wrangler r2 object get pipedpeer-lab/<key> --remote --profile personal`.
