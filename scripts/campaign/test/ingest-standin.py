#!/usr/bin/env python3
"""A local stand-in for the ingest Worker (ingest/worker.js), for rehearsals:
the same routes, token check, caps and usage counters, with a directory in
place of the R2 bucket. headless-test.sh points the lab day's INGEST_URL here,
so rehearsal data never reaches the real bucket.

  ingest-standin.py <bind address> <port> <bucket dir>

INGEST_TOKEN (required), CAP_BYTES, CAP_PUTS and CAP_DAY come from the
environment, with the Worker's defaults. Routes, all behind
"Authorization: Bearer <INGEST_TOKEN>":
  PUT /o/<key>   store the body (Content-Length required)
  GET /usage     the counters and the caps
  GET /o/<key>   read an object back
"""
import datetime
import hmac
import http.server
import json
import os
import re
import sys
import threading
import urllib.parse

USAGE_KEY = "_usage.json"
MAX_OBJECT = 95 * 1000 * 1000
KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=/-]{0,1000}$")
LOCK = threading.Lock()


def caps():
    return {"bytes": float(os.environ.get("CAP_BYTES", 8e9)),
            "puts": float(os.environ.get("CAP_PUTS", 200000)),
            "day": float(os.environ.get("CAP_DAY", 50000))}


class Handler(http.server.BaseHTTPRequestHandler):
    bucket = "."

    def log_message(self, format, *args):  # noqa: A002 (the base class names it so)
        sys.stderr.write("[ingest-standin] %s %s\n" % (self.command, self.path))

    def reply(self, body, status=200):
        data = (json.dumps(body, indent=2) + "\n").encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def usage(self):
        try:
            with open(os.path.join(self.bucket, USAGE_KEY)) as f:
                u = json.load(f)
        except (OSError, ValueError):
            u = {}
        return {k: u.get(k, d) for k, d in (("bytes", 0), ("puts", 0), ("objects", 0), ("day", ""),
                                             ("day_requests", 0), ("rejected", 0))}

    def authorised(self):
        want = "Bearer " + os.environ["INGEST_TOKEN"]
        return hmac.compare_digest(self.headers.get("authorization", ""), want)

    def key(self):
        key = urllib.parse.unquote(self.path.split("?")[0][3:])
        if not KEY_RE.match(key) or ".." in key or key.startswith("_"):
            return None
        return key

    def do_GET(self):
        if not self.authorised():
            return self.reply({"error": "unauthorized"}, 401)
        if self.path == "/usage":
            with LOCK:
                return self.reply({"usage": self.usage(), "caps": caps()})
        if self.path.startswith("/o/"):
            key = self.key()
            if key is None:
                return self.reply({"error": "bad key"}, 400)
            path = os.path.join(self.bucket, key)
            if not os.path.isfile(path):
                return self.reply({"error": "not found"}, 404)
            data = open(path, "rb").read()
            self.send_response(200)
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            return self.wfile.write(data)
        return self.reply({"error": "not found"}, 404)

    def do_PUT(self):
        if not self.authorised():
            return self.reply({"error": "unauthorized"}, 401)
        if not self.path.startswith("/o/"):
            return self.reply({"error": "not found"}, 404)
        key = self.key()
        if key is None:
            return self.reply({"error": "bad key"}, 400)
        try:
            n = int(self.headers.get("content-length") or "")
        except (TypeError, ValueError):
            return self.reply({"error": "Content-Length required"}, 411)
        if n > MAX_OBJECT:
            return self.reply({"error": "object over 95 MB; split it"}, 413)
        body = self.rfile.read(n)
        limit = caps()
        with LOCK:
            u = self.usage()
            today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
            if u["day"] != today:
                u["day"], u["day_requests"] = today, 0
            refuse = ""
            if u["bytes"] + n > limit["bytes"]:
                refuse = "storage cap"
            elif u["puts"] + 1 > limit["puts"]:
                refuse = "upload count cap"
            elif u["day_requests"] + 1 > limit["day"]:
                refuse = "daily request cap"
            if refuse:
                return self.reply({"error": "refused: " + refuse, "usage": u, "caps": limit}, 507)
            path = os.path.join(self.bucket, key)
            existed = os.path.exists(path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path + ".part", "wb") as f:
                f.write(body)
            os.replace(path + ".part", path)
            u["bytes"] += n
            u["puts"] += 1
            u["day_requests"] += 1
            if not existed:
                u["objects"] += 1
            with open(os.path.join(self.bucket, USAGE_KEY), "w") as f:
                json.dump(u, f)
        return self.reply({"ok": True, "key": key, "bytes": n, "usage": u, "caps": limit})


def main():
    bind, port, bucket = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    if not os.environ.get("INGEST_TOKEN"):
        sys.exit("INGEST_TOKEN is not set")
    os.makedirs(bucket, exist_ok=True)
    Handler.bucket = bucket
    http.server.ThreadingHTTPServer((bind, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
