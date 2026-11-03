// Ingest Worker for the lab campaign: the only way data reaches the
// pipedpeer-lab R2 bucket, so the free-tier budget is enforced here, on the
// server, whatever the lab PCs do.
//
// Why a Worker and not S3 keys: the operator's Cloudflare CLI login can create
// buckets and deploy Workers but cannot mint R2 S3 credentials, and a Worker
// can refuse an upload before it costs anything.
//
// R2 free tier (per account, per month): 10 GB-month storage, 1M Class A
// (writes, lists), 10M Class B (reads). Workers free: 100k requests/day; a
// Workers free plan cannot be billed, R2 can. So the caps below cover R2 and
// stay well inside it:
//   - stored bytes  <= CAP_BYTES  (default 8 GB; kept for the month = 8 GB-month)
//   - uploads       <= CAP_PUTS   (each upload is 2 Class A: object + usage file)
//   - requests/day  <= CAP_DAY    (stays under the Workers daily limit)
//   - one object    <= 95 MB      (Workers free plan request body limit is 100 MB)
// Overwriting a key counts its bytes again: the count only ever overstates.
//
// Routes (all need "Authorization: Bearer <INGEST_TOKEN>"):
//   PUT /o/<key>   store the body (Content-Length required)
//   GET /usage     the counters and the caps
//   GET /o/<key>   read an object back (for the self-test)

const USAGE_KEY = "_usage.json";
const MAX_OBJECT = 95 * 1000 * 1000;
const KEY_RE = /^[A-Za-z0-9][A-Za-z0-9._=\/-]{0,1000}$/;

function json(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2) + "\n", {
    status,
    headers: { "content-type": "application/json" },
  });
}

function caps(env) {
  return {
    bytes: Number(env.CAP_BYTES || 8e9),
    puts: Number(env.CAP_PUTS || 200000),
    day: Number(env.CAP_DAY || 50000),
  };
}

async function readUsage(env) {
  const obj = await env.BUCKET.get(USAGE_KEY);
  const u = obj ? await obj.json() : {};
  return {
    bytes: u.bytes || 0,
    puts: u.puts || 0,
    objects: u.objects || 0,
    day: u.day || "",
    day_requests: u.day_requests || 0,
    rejected: u.rejected || 0,
  };
}

function safeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

export default {
  async fetch(request, env) {
    const auth = request.headers.get("authorization") || "";
    if (!env.INGEST_TOKEN || !safeEqual(auth, "Bearer " + env.INGEST_TOKEN)) {
      return json({ error: "unauthorized" }, 401);
    }
    const url = new URL(request.url);
    const limit = caps(env);

    if (request.method === "GET" && url.pathname === "/usage") {
      return json({ usage: await readUsage(env), caps: limit });
    }

    if (url.pathname.startsWith("/o/")) {
      const key = decodeURIComponent(url.pathname.slice(3));
      if (!KEY_RE.test(key) || key.includes("..") || key.startsWith("_")) {
        return json({ error: "bad key" }, 400);
      }

      if (request.method === "GET") {
        const obj = await env.BUCKET.get(key);
        return obj ? new Response(obj.body) : json({ error: "not found" }, 404);
      }

      if (request.method === "PUT") {
        const len = Number(request.headers.get("content-length"));
        if (!Number.isFinite(len) || len < 0) return json({ error: "Content-Length required" }, 411);
        if (len > MAX_OBJECT) return json({ error: "object over 95 MB; split it" }, 413);

        const u = await readUsage(env);
        const today = new Date().toISOString().slice(0, 10);
        if (u.day !== today) {
          u.day = today;
          u.day_requests = 0;
        }
        let refuse = "";
        if (u.bytes + len > limit.bytes) refuse = "storage cap";
        else if (u.puts + 1 > limit.puts) refuse = "upload count cap";
        else if (u.day_requests + 1 > limit.day) refuse = "daily request cap";
        if (refuse) {
          // Not recorded: a refused upload must cost no R2 write at all.
          return json({ error: "refused: " + refuse, usage: u, caps: limit }, 507);
        }

        const existed = (await env.BUCKET.head(key)) !== null;
        await env.BUCKET.put(key, request.body, {
          httpMetadata: { contentType: request.headers.get("content-type") || "application/octet-stream" },
        });
        u.bytes += len;
        u.puts += 1;
        u.day_requests += 1;
        if (!existed) u.objects += 1;
        await env.BUCKET.put(USAGE_KEY, JSON.stringify(u), {
          httpMetadata: { contentType: "application/json" },
        });
        return json({ ok: true, key, bytes: len, usage: u, caps: limit });
      }
    }

    return json({ error: "not found" }, 404);
  },
};
