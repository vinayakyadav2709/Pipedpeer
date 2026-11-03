// A minimal Chrome DevTools Protocol client: launch headless Chromium, open a
// page, send commands. No npm dependencies (Node 22's global WebSocket).
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const CANDIDATES = ["chromium-browser", "chromium", "google-chrome", "google-chrome-stable", "chrome"];

function which(cmd) {
  for (const dir of (process.env.PATH || "").split(path.delimiter)) {
    const p = path.join(dir, cmd);
    try { fs.accessSync(p, fs.constants.X_OK); return p; } catch { /* next */ }
  }
  return null;
}

export async function launch() {
  const bin = process.env.CHROME || CANDIDATES.map(which).find(Boolean);
  if (!bin) throw new Error("no chromium found; set CHROME=/path/to/chrome");
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), "story-chrome-"));
  const proc = spawn(bin, ["--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "--no-first-run",
    "--no-default-browser-check", "--hide-scrollbars", "--mute-audio", "--disable-gpu", "--allow-file-access-from-files",
    "--font-render-hinting=none", "about:blank"], { stdio: ["ignore", "ignore", "pipe"] });
  const port = await new Promise((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error("chromium did not start")), 20000);
    proc.stderr.on("data", (d) => {
      buf += d;
      const m = buf.match(/DevTools listening on ws:\/\/[^:]+:(\d+)\//);
      if (m) { clearTimeout(timer); resolve(+m[1]); }
    });
    proc.on("exit", (c) => reject(new Error("chromium exited " + c)));
  });
  return {
    port,
    async page() {
      const r = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method: "PUT" });
      const { webSocketDebuggerUrl } = await r.json();
      return connect(webSocketDebuggerUrl);
    },
    async close() {
      proc.kill("SIGTERM");
      await new Promise((r) => setTimeout(r, 300));
      fs.rmSync(profile, { recursive: true, force: true });
    },
  };
}

async function connect(url) {
  const ws = new WebSocket(url);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  let id = 0;
  const pending = new Map(), waiters = [];
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
    } else if (msg.method) {
      for (const w of waiters.filter((w) => w.method === msg.method)) { waiters.splice(waiters.indexOf(w), 1); w.resolve(msg.params); }
      if (msg.method === "Runtime.exceptionThrown") console.error("page error:", msg.params.exceptionDetails.exception?.description || msg.params.exceptionDetails.text);
      if (msg.method === "Runtime.consoleAPICalled" && msg.params.type === "error") console.error("console:", msg.params.args.map((a) => a.value ?? a.description).join(" "));
    }
  };
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const n = ++id;
    pending.set(n, { resolve, reject });
    ws.send(JSON.stringify({ id: n, method, params }));
  });
  const once = (method) => new Promise((resolve) => waiters.push({ method, resolve }));
  await send("Page.enable");
  await send("Runtime.enable");
  return {
    send,
    async goto(u) { const load = once("Page.loadEventFired"); await send("Page.navigate", { url: u }); await load; },
    async eval(expr) {
      const r = await send("Runtime.evaluate", { expression: expr, awaitPromise: true, returnByValue: true });
      if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
      return r.result.value;
    },
    async viewport(width, height, scale = 1) {
      await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: scale, mobile: false });
    },
    async shot(file, clip, format = "png") {
      const r = await send("Page.captureScreenshot", { format, quality: format === "jpeg" ? 92 : undefined, captureBeyondViewport: true, clip: clip ? { ...clip, scale: 1 } : undefined });
      fs.writeFileSync(file, Buffer.from(r.data, "base64"));
    },
    async pdf(file, opts) {
      const r = await send("Page.printToPDF", { printBackground: true, preferCSSPageSize: true, ...opts });
      fs.writeFileSync(file, Buffer.from(r.data, "base64"));
    },
    close() { ws.close(); },
  };
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
