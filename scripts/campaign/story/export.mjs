// Renders results/lab/story/index.html in headless Chromium and writes:
//   screens/NN-<section>-<mode>.png   one screenshot per section, dark and light
//   export/<chart>-<mode>.svg|png|pdf  every chart; light for the paper, dark for slides
//   export/hero-<mode>.png             the hero at 1920x1080
//   export/hero.mp4, export/hero.gif   the hero replay at 1080p (ffmpeg)
//
//   node export.mjs results/lab/story/index.html [--no-video] [--only screens|charts|video]
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { launch, sleep } from "./cdp.mjs";

const args = process.argv.slice(2);
const html = path.resolve(args.find((a) => !a.startsWith("--")) || "results/lab/story/index.html");
const only = (args[args.indexOf("--only") + 1] || "").split(",").filter((x) => x && args.includes("--only"));
const want = (k) => !only.length || only.includes(k);
const root = path.dirname(html);
const screens = path.join(root, "screens"), out = path.join(root, "export");
fs.mkdirSync(screens, { recursive: true });
fs.mkdirSync(out, { recursive: true });
const url = (q) => pathToFileURL(html).href + (q ? "?" + q : "");

async function open(browser, q, w = 1440, h = 900, scale = 2) {
  const p = await browser.page();
  await p.viewport(w, h, scale);
  await p.goto(url(q));
  await p.eval("new Promise(r => { const t = () => window.__story && window.__story.ready ? r(true) : setTimeout(t, 50); t(); })");
  await sleep(300);
  return p;
}

// Every section, settled: chapters revealed, replays at their end, hero paused mid-run.
async function settle(p) {
  await p.eval(`(() => {
    document.querySelectorAll('.chapter').forEach(s => s.classList.add('in'));
    document.documentElement.style.scrollBehavior = 'auto';
    return true; })()`);
  // Scroll through once so each replay's on-screen trigger fires, then park them at the end.
  const h = await p.eval("document.body.scrollHeight");
  for (let y = 0; y < h; y += 600) { await p.eval(`scrollTo(0, ${y})`); await sleep(40); }
  await sleep(200);
  await p.eval(`(() => { for (const r of window.__story.replays()) if (r) r.seek(1e9); window.__story.hero.seek(${heroStill()}); scrollTo(0, 0); return true; })()`);
  await sleep(200);
}
const heroStill = () => "window.__story.hero.range[0] + (window.__story.hero.range[1] - window.__story.hero.range[0]) * 0.32";

async function shootSections(browser, mode) {
  const p = await open(browser, "theme=" + mode, 1440, 900, 2);
  await settle(p);
  const boxes = await p.eval(`[...document.querySelectorAll('#top, .chapter')].map(s => { const r = s.getBoundingClientRect(); return { id: s.id, x: 0, y: r.top + scrollY, width: innerWidth, height: Math.ceil(r.height) }; })`);
  const files = [];
  for (const [i, b] of boxes.entries()) {
    const f = path.join(screens, `${String(i).padStart(2, "0")}-${b.id}-${mode}.png`);
    await p.shot(f, b);
    files.push(f);
  }
  p.close();
  return files;
}

async function exportCharts(browser, mode) {
  const p = await open(browser, "theme=" + mode, 1280, 900, 3);
  await settle(p);
  const items = await p.eval(`(() => {
    const seen = {};
    return [...document.querySelectorAll('svg[data-export]')].map(svg => {
      let name = svg.getAttribute('data-export'); seen[name] = (seen[name] || 0) + 1; if (seen[name] > 1) name += '-' + seen[name];
      const c = svg.cloneNode(true);
      c.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
      c.querySelectorAll('[clip-path]').forEach(n => n.removeAttribute('clip-path'));
      const bg = c.querySelector('rect.bg'); if (bg) bg.setAttribute('opacity', 1);
      c.querySelectorAll('[style*="cursor"]').forEach(n => n.style.removeProperty('cursor'));
      const r = svg.getBoundingClientRect();
      return { name, svg: c.outerHTML, w: +svg.getAttribute('width'), h: +svg.getAttribute('height'), x: r.left + scrollX, y: r.top + scrollY };
    }); })()`);
  for (const it of items) {
    const base = path.join(out, `${it.name}-${mode}`);
    fs.writeFileSync(base + ".svg", '<?xml version="1.0" encoding="UTF-8"?>\n' + it.svg);
  }
  p.close();
  // PNG and PDF from the standalone SVGs, so the files match exactly.
  const q = await browser.page();
  for (const it of items) {
    const base = path.join(out, `${it.name}-${mode}`);
    await q.viewport(Math.ceil(it.w), Math.ceil(it.h), 3);
    const page = path.join(out, `.${it.name}-${mode}.html`);
    fs.writeFileSync(page, `<!doctype html><style>@page{size:${it.w}px ${it.h}px;margin:0}html,body{margin:0;background:transparent}svg{display:block}</style>${it.svg}`);
    await q.goto(pathToFileURL(page).href);
    await q.shot(base + ".png", { x: 0, y: 0, width: it.w, height: it.h });
    await q.pdf(base + ".pdf", {});
    fs.rmSync(page);
  }
  q.close();
  return items.map((i) => i.name);
}

async function video(browser, mode, { stillOnly = false } = {}) {
  const p = await open(browser, `export=hero&theme=${mode}`, 1920, 1080, 1);
  const [t0, t1] = await p.eval("window.__story.hero.range");
  await p.eval(`window.__story.hero.seek(${t0} + (${t1} - ${t0}) * 0.32)`);
  await p.shot(path.join(out, `hero-${mode}.png`), { x: 0, y: 0, width: 1920, height: 1080 });
  if (stillOnly) return p.close();
  const fps = 30, rate = 0.5, hold = 2.0;
  const frames = fs.mkdtempSync(path.join(os.tmpdir(), "story-frames-"));
  const n = Math.ceil(((t1 - t0) / rate) * fps);
  for (let i = 0; i <= n; i++) {
    await p.eval(`window.__story.hero.seek(${t0 + (i / fps) * rate})`);
    await p.shot(path.join(frames, `f${String(i).padStart(5, "0")}.png`), { x: 0, y: 0, width: 1920, height: 1080 });
  }
  p.close();
  const mp4 = path.join(out, "hero.mp4"), gif = path.join(out, "hero.gif");
  const ff = (a) => { const r = spawnSync("ffmpeg", ["-hide_banner", "-loglevel", "error", "-y", ...a], { stdio: "inherit" }); if (r.status) throw new Error("ffmpeg failed"); };
  ff(["-framerate", String(fps), "-i", path.join(frames, "f%05d.png"), "-vf", `tpad=stop_mode=clone:stop_duration=${hold}`,
    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", "-preset", "slow", "-movflags", "+faststart", mp4]);
  ff(["-i", mp4, "-vf", "fps=15,scale=960:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=96[p];[b][p]paletteuse=dither=sierra2_4a", gif]);
  fs.rmSync(frames, { recursive: true, force: true });
  return [mp4, gif];
}

const browser = await launch();
try {
  if (want("screens")) for (const m of ["dark", "light"]) console.log(`screens ${m}:`, (await shootSections(browser, m)).length);
  if (want("charts")) for (const m of ["light", "dark"]) console.log(`charts ${m}:`, (await exportCharts(browser, m)).join(" "));
  if (want("video")) {
    await video(browser, "light", { stillOnly: true });
    if (args.includes("--no-video")) await video(browser, "dark", { stillOnly: true });
    else console.log("video:", (await video(browser, "dark")).join(" "));
  }
} finally {
  await browser.close();
}
