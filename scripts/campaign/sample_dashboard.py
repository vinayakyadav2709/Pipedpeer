"""The explore dashboard for sample_figures.py --html: one self-contained
HTML file (no server, no network), the model inlined as JSON, plain SVG and
JavaScript. Views: the effort-vs-speedup frontier, a scaling view with an N
slider, small multiples, the parallel-efficiency heatmap, and a run picker
with that run's per-PC CPU heatmap, swimlane, network bytes and an animated
cluster replay. Hover any mark for its numbers; every chart has a table.
"""
import json

PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PipedPeer lab campaign: explore</title>
<style>
.viz-root{color-scheme:light;--surface-1:#fcfcfb;--page:#f9f9f7;--text-primary:#0b0b0b;--text-secondary:#52514e;
 --muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--ring:rgba(11,11,11,.10);
 --s-plain:#898781;--s-pipedpeer:#2a78d6;--s-ray:#eb6834;--s-dask:#1baf7a;
 --qhi:#ffffff;--qlo:#0b0b0b;--q0:#f4f8fd;--q1:#cde2fb;--q2:#9ec5f4;--q3:#6da7ec;--q4:#3987e5;--q5:#256abf;--q6:#184f95;--q7:#0d366b}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])) .viz-root{color-scheme:dark;
 --surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#f0efec;--text-secondary:#c3c2b7;--grid:#2c2c2a;--axis:#383835;
 --ring:rgba(255,255,255,.10);--s-pipedpeer:#3987e5;--s-ray:#d95926;--s-dask:#199e70;
 --qhi:#0b0b0b;--qlo:#f0efec;--q0:#1f2329;--q1:#104281;--q2:#184f95;--q3:#1c5cab;--q4:#256abf;--q5:#3987e5;--q6:#6da7ec;--q7:#b7d3f6}}
:root[data-theme="dark"] .viz-root{color-scheme:dark;--surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#f0efec;
 --text-secondary:#c3c2b7;--grid:#2c2c2a;--axis:#383835;--ring:rgba(255,255,255,.10);--s-pipedpeer:#3987e5;
 --s-ray:#d95926;--s-dask:#199e70;--qhi:#0b0b0b;--qlo:#f0efec;--q0:#1f2329;--q1:#104281;--q2:#184f95;--q3:#1c5cab;--q4:#256abf;--q5:#3987e5;
 --q6:#6da7ec;--q7:#b7d3f6}
*{box-sizing:border-box}
body{margin:0;font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
.viz-root{background:var(--page);color:var(--text-primary);min-height:100vh;padding:24px 28px 60px}
header{display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;max-width:1180px;margin:0 auto 8px}
h1{font-size:20px;margin:0;font-weight:600}
h2{font-size:15px;margin:0 0 2px;font-weight:600}
.sub{color:var(--text-secondary);font-size:13px;margin:0 0 12px}
.banner{max-width:1180px;margin:0 auto 18px;padding:8px 12px;border-radius:8px;border:1px solid var(--ring);
 color:var(--text-secondary);font-size:13px;background:var(--surface-1)}
.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px;max-width:1180px;margin:0 auto}
.card{background:var(--surface-1);border:1px solid var(--ring);border-radius:12px;padding:16px 18px;min-width:0}
.c12{grid-column:span 12}.c7{grid-column:span 7}.c5{grid-column:span 5}.c6{grid-column:span 6}.c4{grid-column:span 4}
@media (max-width:900px){.c7,.c5,.c6,.c4{grid-column:span 12}}
svg{display:block;overflow:visible}
svg text{fill:var(--text-secondary);font-size:11px}
.axis line,.axis path{stroke:var(--axis)}
.gridl{stroke:var(--grid);stroke-width:1}
.controls{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin:6px 0 10px;font-size:13px;color:var(--text-secondary)}
select,button,input[type=range]{font:inherit;color:var(--text-primary)}
select,button{background:var(--surface-1);border:1px solid var(--axis);border-radius:6px;padding:4px 8px}
button{cursor:pointer}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--text-secondary);margin:2px 0 6px}
.legend span{display:inline-flex;align-items:center;gap:6px}
.key{width:14px;height:3px;border-radius:2px;display:inline-block}
#tip{position:fixed;pointer-events:none;z-index:9;background:var(--surface-1);color:var(--text-primary);
 border:1px solid var(--ring);border-radius:8px;padding:7px 10px;font-size:12px;box-shadow:0 4px 14px rgba(0,0,0,.12);
 opacity:0;transition:opacity .08s;max-width:320px}
#tip b{font-weight:600}#tip .r{display:flex;gap:8px;align-items:center;white-space:nowrap}
#tip .m{color:var(--text-secondary)}
table{border-collapse:collapse;font-size:12px;width:100%;font-variant-numeric:tabular-nums}
th,td{padding:4px 8px;border-bottom:1px solid var(--grid);text-align:right}
th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}
th{color:var(--text-secondary);font-weight:600}
details summary{cursor:pointer;color:var(--text-secondary);font-size:12px;margin-top:8px}
.hit{fill:transparent;cursor:crosshair}
.stat{display:flex;gap:28px;flex-wrap:wrap;margin:4px 0 0}
.stat div{display:flex;flex-direction:column}.stat .v{font-size:22px;font-weight:600}
.stat .l{font-size:12px;color:var(--text-secondary)}
</style></head>
<body><div class="viz-root">
<header><h1>PipedPeer lab campaign: explore</h1>
<span class="sub" id="meta"></span>
<button id="theme" style="margin-left:auto">Toggle theme</button></header>
<div class="banner" id="label"></div>
<div class="grid">
 <section class="card c7"><h2>Effort vs speedup</h2>
  <p class="sub">Lines of the script changed to run distributed (port-lines.csv) against the speedup over plain Python at the largest N. Up and to the left is better. Whiskers: IQR.</p>
  <div class="legend" id="lg-frontier"></div><div id="frontier"></div>
  <details><summary>Table</summary><div id="t-frontier"></div></details></section>
 <section class="card c5"><h2>Scaling at one N</h2>
  <p class="sub">Speedup over plain at the chosen N, per workload. Drag the slider.</p>
  <div class="controls">N = <input type="range" id="nslider" min="0" max="0" step="1"><b id="nval"></b></div>
  <div class="legend" id="lg-scale"></div><div id="scale"></div></section>
 <section class="card c12"><h2>Strong scaling, every workload</h2>
  <p class="sub">Speedup over plain at N = 1 against N, a panel per workload; bands are the IQR over repeats; the dotted line is ideal.</p>
  <div class="legend" id="lg-multi"></div><div id="multi" style="display:flex;gap:18px;flex-wrap:wrap"></div></section>
 <section class="card c6"><h2>Parallel efficiency</h2>
  <p class="sub">Speedup / N, workload by N, a panel per system. Darker is closer to ideal.</p>
  <div id="eff" style="display:flex;gap:18px;flex-wrap:wrap"></div></section>
 <section class="card c6"><h2>Other experiments</h2>
  <p class="sub">E4 uneven machines, E6 a worker killed, E7 interception overhead: median wall, IQR whiskers.</p>
  <div id="others"></div></section>
 <section class="card c12"><h2>One run, every PC</h2>
  <p class="sub">The representative run (median wall) of each group. CPU is busy cores per PC from the OTel host metrics; the swimlane is where pipedpeer's chunks computed (receipt steps; Ray and Dask record none, so CPU activity stands in); bars are bytes each PC sent and received during the run.</p>
  <div class="controls"><label>Run <select id="runpick"></select></label>
   <button id="play">&#9654; Replay</button><span id="clock"></span></div>
  <div class="stat" id="runstat"></div>
  <div id="heat"></div><div id="lanes"></div><div id="net"></div></section>
 <section class="card c12"><h2>All groups</h2>
  <p class="sub">Median wall s [IQR]; warm-ups and failed runs excluded and counted.</p><div id="t-all"></div></section>
</div><div id="tip" role="tooltip"></div></div>
<script id="model" type="application/json">__DATA__</script>
<script>
"use strict";
const M = JSON.parse(document.getElementById("model").textContent);
const DIST = ["pipedpeer", "ray", "dask"], SYS = ["plain", ...DIST];
const NS = "http://www.w3.org/2000/svg";
const LAB = M.labels;
const col = s => `var(--s-${SYS.includes(s) ? s : "plain"})`;
const fam = s => s.startsWith("pipedpeer") ? "pipedpeer" : (SYS.includes(s) ? s : "plain");
const SHAPE = {plain: "diamond", pipedpeer: "circle", ray: "square", dask: "triangle"};
const $ = id => document.getElementById(id);
function el(tag, attrs, parent) {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}
function txt(parent, x, y, s, attrs) { const t = el("text", Object.assign({x, y}, attrs || {}), parent); t.textContent = s; return t; }
function svg(host, w, h) { host.textContent = ""; return el("svg", {width: w, height: h, viewBox: `0 0 ${w} ${h}`, role: "img"}, host); }
const fmt = (v, d = 2) => v == null || isNaN(v) ? "–" : Number(v).toFixed(d);
// Tooltip: values lead, labels follow; built with textContent (labels are data).
const tip = $("tip");
function showTip(ev, title, rows) {
  tip.textContent = "";
  const h = document.createElement("div"); const b = document.createElement("b"); b.textContent = title; h.appendChild(b); tip.appendChild(h);
  for (const [color, value, label] of rows) {
    const r = document.createElement("div"); r.className = "r";
    if (color) { const k = document.createElement("span"); k.className = "key"; k.style.background = color; r.appendChild(k); }
    const v = document.createElement("b"); v.textContent = value; r.appendChild(v);
    const l = document.createElement("span"); l.className = "m"; l.textContent = label; r.appendChild(l);
    tip.appendChild(r);
  }
  const x = Math.min(ev.clientX + 14, innerWidth - tip.offsetWidth - 8), y = Math.min(ev.clientY + 14, innerHeight - tip.offsetHeight - 8);
  tip.style.left = x + "px"; tip.style.top = y + "px"; tip.style.opacity = 1;
}
function hideTip() { tip.style.opacity = 0; }
function hover(node, fn) { node.addEventListener("pointermove", e => fn(e)); node.addEventListener("pointerleave", hideTip); }
function marker(g, s, x, y, r, hollow) {
  const c = col(s), a = {fill: hollow ? "var(--surface-1)" : c, stroke: hollow ? c : "var(--surface-1)", "stroke-width": 2};
  const sh = SHAPE[fam(s)];
  if (sh === "circle") return el("circle", Object.assign({cx: x, cy: y, r}, a), g);
  if (sh === "square") return el("rect", Object.assign({x: x - r * .9, y: y - r * .9, width: r * 1.8, height: r * 1.8, rx: 1.5}, a), g);
  if (sh === "triangle") return el("path", Object.assign({d: `M${x},${y - r * 1.1}L${x + r * 1.05},${y + r * .75}L${x - r * 1.05},${y + r * .75}Z`}, a), g);
  return el("path", Object.assign({d: `M${x},${y - r * 1.1}L${x + r},${y}L${x},${y + r * 1.1}L${x - r},${y}Z`}, a), g);
}
function legend(host, systems) {
  host.textContent = "";
  for (const s of systems) {
    const sp = document.createElement("span"); const k = document.createElement("span"); k.className = "key"; k.style.background = col(s);
    sp.appendChild(k); sp.appendChild(document.createTextNode(LAB[s] || s)); host.appendChild(sp);
  }
}
function scale(d0, d1, r0, r1) { const f = v => r0 + (v - d0) / ((d1 - d0) || 1) * (r1 - r0); f.inv = p => d0 + (p - r0) / ((r1 - r0) || 1) * (d1 - d0); return f; }
function ticks(lo, hi, n) {
  const span = hi - lo || 1, step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const step = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= step0) || mag * 10;
  const out = []; for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) out.push(+v.toFixed(10)); return out;
}
function yaxis(g, y, lo, hi, x0, x1, label) {
  for (const t of ticks(lo, hi, 4)) { el("line", {x1: x0, x2: x1, y1: y(t), y2: y(t), class: "gridl"}, g); txt(g, x0 - 6, y(t) + 4, t, {"text-anchor": "end"}); }
  if (label) txt(g, x0 - 34, (y(lo) + y(hi)) / 2, label, {transform: `rotate(-90 ${x0 - 34} ${(y(lo) + y(hi)) / 2})`, "text-anchor": "middle"});
}
function table(host, head, rows) {
  const t = document.createElement("table"), tr = document.createElement("tr");
  for (const h of head) { const th = document.createElement("th"); th.textContent = h; tr.appendChild(th); }
  t.appendChild(tr);
  for (const r of rows) { const row = document.createElement("tr"); for (const c of r) { const td = document.createElement("td"); td.textContent = c; row.appendChild(td); } t.appendChild(row); }
  host.textContent = ""; host.appendChild(t);
}
// ---- data
const E1 = (M.groups.e1 || []).filter(r => r.median_wall_s != null);
const base = {}; E1.filter(r => r.system === "plain" && r.n === 1).forEach(r => base[r.workload] = r.median_wall_s);
const sp = r => base[r.workload] ? base[r.workload] / r.median_wall_s : null;
const spLo = r => base[r.workload] && r.q3_wall_s ? base[r.workload] / r.q3_wall_s : null;
const spHi = r => base[r.workload] && r.q1_wall_s ? base[r.workload] / r.q1_wall_s : null;
const WLS = [...new Set(E1.map(r => r.workload))].sort();
const NSET = [...new Set(E1.map(r => r.n))].sort((a, b) => a - b);
const NMAX = Math.max(...NSET, 1);
// ---- frontier
function drawFrontier() {
  const lines = {}; (M.port_lines || []).forEach(p => lines[p.workload + "|" + p.system] = p.lines);
  const pts = E1.filter(r => r.n === NMAX && DIST.includes(r.system) && sp(r) && (r.workload + "|" + r.system) in lines)
    .map(r => ({r, x: lines[r.workload + "|" + r.system], y: sp(r)}));
  legend($("lg-frontier"), DIST);
  const W = 640, H = 330, L = 56, R = 20, T = 14, B = 40;
  const s = svg($("frontier"), W, H);
  if (!pts.length) { txt(s, 20, 40, "No e1 speedups with port-lines.csv yet."); return; }
  const xmax = Math.max(...pts.map(p => p.x)) + 2, ymax = Math.max(NMAX, ...pts.map(p => spHi(p.r) || p.y)) * 1.12;
  const x = scale(-1, xmax, L, W - R), y = scale(0, ymax, H - B, T);
  yaxis(s, y, 0, ymax, L, W - R, "speedup at N = " + NMAX);
  for (const t of ticks(0, xmax, 6)) txt(s, x(t), H - B + 16, t, {"text-anchor": "middle"});
  txt(s, (L + W - R) / 2, H - 6, "lines of the script changed", {"text-anchor": "middle"});
  el("line", {x1: L, x2: W - R, y1: y(1), y2: y(1), stroke: "var(--axis)", "stroke-width": 1.5}, s);
  txt(s, W - R, y(1) - 5, "plain Python, 1 PC", {"text-anchor": "end", fill: "var(--muted)"});
  el("line", {x1: L, x2: W - R, y1: y(NMAX), y2: y(NMAX), stroke: "var(--axis)", "stroke-dasharray": "2 4"}, s);
  txt(s, W - R, y(NMAX) - 5, "ideal " + NMAX + "×", {"text-anchor": "end", fill: "var(--muted)"});
  const seen = {};
  pts.sort((a, b) => a.x - b.x || a.r.workload.localeCompare(b.r.workload));
  for (const p of pts) {
    const k = p.x + p.r.system, dx = (seen[k] = (seen[k] || 0) + 1) - 1, px = x(p.x) + dx * 14, py = y(p.y);
    const g = el("g", {}, s);
    if (spLo(p.r)) el("line", {x1: px, x2: px, y1: y(spLo(p.r)), y2: y(spHi(p.r)), stroke: col(p.r.system), "stroke-width": 2, opacity: .45, "stroke-linecap": "round"}, g);
    marker(g, p.r.system, px, py, 6);
    txt(g, px + 10, py + 4, p.r.workload);
    el("circle", {cx: px, cy: py, r: 14, class: "hit"}, g);
    hover(g, e => showTip(e, `${LAB[p.r.system]}, ${p.r.workload}`, [[col(p.r.system), fmt(p.y) + "×", `speedup at N=${NMAX}`],
      [null, `${fmt(spLo(p.r))}–${fmt(spHi(p.r))}`, "IQR"], [null, p.x, "lines changed"], [null, fmt(p.r.median_wall_s) + " s", "median wall"]]));
  }
  table($("t-frontier"), ["workload", "system", "lines changed", "speedup", "IQR"], pts.map(p => [p.r.workload, LAB[p.r.system], p.x, fmt(p.y), `${fmt(spLo(p.r))}–${fmt(spHi(p.r))}`]));
}
// ---- scaling at one N (slider)
function drawScale() {
  const n = NSET[+$("nslider").value] || 1; $("nval").textContent = n;
  legend($("lg-scale"), DIST);
  const W = 460, H = 300, L = 46, R = 10, T = 10, B = 34;
  const s = svg($("scale"), W, H);
  const rows = E1.filter(r => r.n === n && DIST.includes(r.system) && sp(r));
  const ymax = Math.max(n, ...E1.filter(r => DIST.includes(r.system) && sp(r)).map(r => spHi(r) || sp(r))) * 1.1;
  const y = scale(0, ymax, H - B, T); yaxis(s, y, 0, ymax, L, W - R, "speedup");
  el("line", {x1: L, x2: W - R, y1: y(n), y2: y(n), stroke: "var(--axis)", "stroke-dasharray": "2 4"}, s);
  const band = (W - L - R) / Math.max(1, WLS.length), bw = Math.min(24, band / 4.5);
  WLS.forEach((wl, i) => {
    const cx = L + band * (i + .5); txt(s, cx, H - B + 16, wl, {"text-anchor": "middle"});
    DIST.forEach((sy, j) => {
      const r = rows.find(r => r.workload === wl && r.system === sy); if (!r) return;
      const x0 = cx + (j - 1) * (bw + 2) - bw / 2, v = sp(r), top = y(v), h = y(0) - top;
      const g = el("g", {}, s);
      el("path", {d: `M${x0},${y(0)}V${top + 4}Q${x0},${top} ${x0 + 4},${top}H${x0 + bw - 4}Q${x0 + bw},${top} ${x0 + bw},${top + 4}V${y(0)}Z`, fill: col(sy)}, g);
      if (spLo(r)) el("line", {x1: x0 + bw / 2, x2: x0 + bw / 2, y1: y(spLo(r)), y2: y(spHi(r)), stroke: "var(--text-secondary)", "stroke-width": 1}, g);
      el("rect", {x: x0 - 2, y: T, width: bw + 4, height: H - B - T, class: "hit"}, g);
      hover(g, e => showTip(e, `${wl} at N = ${n}`, [[col(sy), fmt(v) + "×", LAB[sy]], [null, fmt(r.median_wall_s) + " s", "median wall"], [null, `${r.runs_used} used, ${r.failed_excluded} failed`, "runs"]]));
    });
  });
}
// ---- small multiples
function drawMulti() {
  legend($("lg-multi"), DIST); const host = $("multi"); host.textContent = "";
  for (const wl of WLS) {
    const d = document.createElement("div"); host.appendChild(d);
    const W = 360, H = 230, L = 44, R = 64, T = 22, B = 34, s = svg(d, W, H);
    txt(s, L, 12, wl, {style: "font-weight:600;fill:var(--text-primary)"});
    const mine = E1.filter(r => r.workload === wl && DIST.includes(r.system) && sp(r));
    const ns = NSET, lx = v => Math.log2(v);
    const x = scale(lx(ns[0]), lx(ns[ns.length - 1]) || 1, L, W - R);
    const ymax = Math.max(ns[ns.length - 1], ...mine.map(r => spHi(r) || sp(r))) * 1.1, y = scale(0, ymax, H - B, T);
    yaxis(s, y, 0, ymax, L, W - R, "speedup");
    ns.forEach(n => txt(s, x(lx(n)), H - B + 16, n, {"text-anchor": "middle"}));
    txt(s, (L + W - R) / 2, H - 4, "N", {"text-anchor": "middle"});
    el("path", {d: ns.map((n, i) => (i ? "L" : "M") + x(lx(n)) + "," + y(n)).join(""), stroke: "var(--axis)", "stroke-dasharray": "2 4", fill: "none"}, s);
    const ends = [];
    for (const sy of DIST) {
      const pts = mine.filter(r => r.system === sy).sort((a, b) => a.n - b.n); if (!pts.length) continue;
      const up = pts.map(r => [x(lx(r.n)), y(spHi(r) || sp(r))]), dn = pts.map(r => [x(lx(r.n)), y(spLo(r) || sp(r))]).reverse();
      el("path", {d: "M" + up.concat(dn).map(p => p.join(",")).join("L") + "Z", fill: col(sy), opacity: .12}, s);
      el("path", {d: pts.map((r, i) => (i ? "L" : "M") + x(lx(r.n)) + "," + y(sp(r))).join(""), stroke: col(sy), "stroke-width": 2, fill: "none", "stroke-linejoin": "round", "stroke-linecap": "round"}, s);
      const last = pts[pts.length - 1]; ends.push({x: x(lx(last.n)) + 9, y: y(sp(last)) + 4, s: LAB[sy]});
      for (const r of pts) {
        const g = el("g", {}, s); marker(g, sy, x(lx(r.n)), y(sp(r)), 4.5);
        el("circle", {cx: x(lx(r.n)), cy: y(sp(r)), r: 12, class: "hit"}, g);
        hover(g, e => showTip(e, `${wl}, N = ${r.n}`, [[col(sy), fmt(sp(r)) + "×", LAB[sy]], [null, `${fmt(spLo(r))}–${fmt(spHi(r))}`, "IQR"], [null, fmt(r.median_wall_s) + " s", "median wall"]]));
      }
    }
    // End labels: spread apart where lines converge (12 px apart), each tied
    // to its line end by a hairline leader.
    ends.sort((a, b) => a.y - b.y);
    const ys = ends.map(e => e.y);
    for (let i = 1; i < ys.length; i++) ys[i] = Math.max(ys[i], ys[i - 1] + 12);
    ends.forEach((e, i) => { if (ys[i] !== e.y) el("line", {x1: e.x - 6, y1: e.y - 4, x2: e.x + 2, y2: ys[i] - 4, stroke: "var(--axis)"}, s); txt(s, e.x + 3, ys[i], e.s); });
  }
}
// ---- efficiency heatmap
function q(v) { const i = Math.max(0, Math.min(7, Math.round(v * 7))); return `var(--q${i})`; }
function drawEff() {
  const host = $("eff"); host.textContent = "";
  for (const sy of DIST) {
    const rows = E1.filter(r => r.system === sy && sp(r)); if (!rows.length) continue;
    const d = document.createElement("div"); host.appendChild(d);
    const cw = 52, ch = 30, L = 40, T = 24, W = L + cw * NSET.length + 4, H = T + ch * WLS.length + 26, s = svg(d, W, H);
    txt(s, L, 14, LAB[sy], {style: "font-weight:600;fill:var(--text-primary)"});
    WLS.forEach((wl, i) => {
      txt(s, L - 6, T + ch * i + ch / 2 + 4, wl, {"text-anchor": "end"});
      NSET.forEach((n, j) => {
        const r = rows.find(r => r.workload === wl && r.n === n), g = el("g", {}, s);
        const v = r ? sp(r) / n : null;
        el("rect", {x: L + cw * j + 1, y: T + ch * i + 1, width: cw - 2, height: ch - 2, rx: 4, fill: v == null ? "var(--grid)" : q(v)}, g);
        txt(g, L + cw * j + cw / 2, T + ch * i + ch / 2 + 4, v == null ? "–" : fmt(v), {"text-anchor": "middle", style: `fill:${v != null && v > .55 ? "var(--qhi)" : "var(--qlo)"}`});
        if (r) hover(g, e => showTip(e, `${LAB[sy]}, ${wl}, N = ${n}`, [[col(sy), fmt(v), "efficiency (speedup / N)"], [null, fmt(sp(r)) + "×", "speedup"]]));
      });
    });
    NSET.forEach((n, j) => txt(s, L + cw * j + cw / 2, H - 8, "N=" + n, {"text-anchor": "middle"}));
  }
}
// ---- other experiments: E4, E6, E7 as dot plots with IQR
function drawOthers() {
  const sets = [];
  for (const [exp, title, key] of [["e4", "E4 uneven PCs", r => r.system], ["e6", "E6 kill one worker", r => r.system + (r.fault && r.fault !== "none" ? " (killed)" : " (no fault)")], ["e7", "E7 trivial.py", r => r.system.replace("pipedpeer-", "shim: ").replace("noshim", "off").replace("shim: shim", "shim: on")]]) {
    const rows = (M.groups[exp] || []).filter(r => r.median_wall_s != null && !String(r.system).endsWith("ctrlc"));
    if (rows.length) sets.push({exp, title, rows, key});
  }
  const lineH = 22, W = 520, L = 190, R = 50;
  let H = 10; sets.forEach(s => H += 22 + s.rows.length * lineH + 8);
  const s = svg($("others"), W, Math.max(H, 40));
  if (!sets.length) { txt(s, 10, 24, "No e4, e6 or e7 runs yet."); return; }
  const xmax = Math.max(...sets.flatMap(t => t.rows.map(r => r.q3_wall_s || r.median_wall_s))) * 1.1, x = scale(0, xmax, L, W - R);
  let yy = 10;
  for (const set of sets) {
    txt(s, 0, yy + 14, set.title, {style: "font-weight:600;fill:var(--text-primary)"}); yy += 22;
    for (const r of set.rows) {
      const g = el("g", {}, s), cy = yy + lineH / 2, c = col(fam(r.system));
      txt(g, L - 8, cy + 4, set.key(r), {"text-anchor": "end"});
      el("line", {x1: L, x2: W - R, y1: cy, y2: cy, class: "gridl"}, g);
      el("line", {x1: x(r.q1_wall_s), x2: x(r.q3_wall_s), y1: cy, y2: cy, stroke: c, "stroke-width": 3, "stroke-linecap": "round", opacity: .5}, g);
      marker(g, fam(r.system), x(r.median_wall_s), cy, 5, r.system.includes("headcount") || r.system.includes("noshim"));
      txt(g, x(r.q3_wall_s) + 8, cy + 4, fmt(r.median_wall_s, 1) + " s");
      el("rect", {x: 0, y: yy, width: W, height: lineH, class: "hit"}, g);
      hover(g, e => showTip(e, `${set.title}: ${set.key(r)}`, [[c, fmt(r.median_wall_s) + " s", "median wall"], [null, `${fmt(r.q1_wall_s)}–${fmt(r.q3_wall_s)}`, "IQR"], [null, `${r.runs_used} used, ${r.failed_excluded} failed`, "runs"]]));
      yy += lineH;
    }
    yy += 8;
  }
}
// ---- one run: CPU heatmap, swimlane, network, replay
const RUNS = M.runs.filter(r => M.timelines[r.run_id]);
function runLabel(r) { return `${r.experiment} ${r.workload} · ${LAB[r.system] || r.system} · N=${r.n}${r.variant ? " · " + r.variant : ""}${r.fault && r.fault !== "none" ? " · " + r.fault : ""} · ${fmt(r.wall_s, 1)} s`; }
let playT = null, playing = null;
function currentRun() { return RUNS.find(r => r.run_id === $("runpick").value); }
function drawRun() {
  const r = currentRun(); if (!r) { $("heat").textContent = "No run timelines (no OTel samples during runs)."; return; }
  const t = M.timelines[r.run_id], pcs = M.pcs, pad = t.pad_s, span = t.wall_s + 2 * pad;
  const st = $("runstat"); st.textContent = "";
  const stats = [[fmt(t.wall_s, 2) + " s", "wall"], [(t.hosts || []).length, "PCs in the run"],
    [r.receipt ? `${r.receipt.remote_items || 0} / ${(r.receipt.local_items || 0) + (r.receipt.remote_items || 0)}` : "–", "items run remotely"],
    [fmt(Object.values(t.net).reduce((a, v) => a + v.tx + v.rx, 0) / 1e6, 1) + " MB", "network, all PCs"]];
  for (const [v, l] of stats) { const d = document.createElement("div"); const a = document.createElement("span"); a.className = "v"; a.textContent = v; const b = document.createElement("span"); b.className = "l"; b.textContent = l; d.append(a, b); st.appendChild(d); }
  const W = 1120, L = 60, R = 70, rowH = Math.max(16, Math.min(34, 300 / pcs.length)), T = 18;
  const x = scale(0, span, L, W - R);
  const vmax = Math.max(1, ...Object.values(t.cpu).flat().map(p => p[1]));
  // heatmap
  let H = T + rowH * pcs.length + 30, s = svg($("heat"), W, H);
  txt(s, L, 12, "busy cores per PC (darker = busier; scale 0–" + fmt(vmax, 1) + "); CPU source: " + [...new Set(Object.values(t.cpu_source || {}))].join(", ") + (Object.values(t.cpu_source || {}).includes("host") && M.cpu_source_note ? " — " + M.cpu_source_note : ""), {style: "fill:var(--text-primary)"});
  pcs.forEach((pc, i) => {
    txt(s, L - 8, T + rowH * i + rowH / 2 + 4, pc + ((t.hosts || []).includes(pc) ? " •" : ""), {"text-anchor": "end"});
    const ser = t.cpu[pc] || []; let prev = null;
    for (const [tb, v] of ser) {
      const ta = prev == null ? tb - 1 : prev; prev = tb; if (tb < 0 || ta > span) continue;
      const g = el("g", {}, s);
      el("rect", {x: x(Math.max(0, ta)), y: T + rowH * i + 1, width: Math.max(1, x(Math.min(span, tb)) - x(Math.max(0, ta)) - .5), height: rowH - 2, fill: q(v / vmax)}, g);
      hover(g, e => showTip(e, `${pc} at ${fmt(tb - pad, 1)} s`, [[col(r.system), fmt(v) + " cores", "busy"], [null, (t.cpu_source || {})[pc] || "host", "CPU source"]]));
    }
  });
  for (const v of [pad, pad + t.wall_s]) el("line", {x1: x(v), x2: x(v), y1: T - 4, y2: T + rowH * pcs.length + 2, stroke: "var(--text-primary)", "stroke-width": 1}, s);
  for (const tv of ticks(-pad, t.wall_s + pad, 8)) txt(s, x(tv + pad), H - 10, tv + " s", {"text-anchor": "middle"});
  const head = el("line", {x1: x(0), x2: x(0), y1: T - 4, y2: T + rowH * pcs.length + 2, stroke: col(r.system), "stroke-width": 2, opacity: 0}, s);
  const bars = pcs.map((pc, i) => el("rect", {x: W - R + 8, y: T + rowH * i + 3, width: 0, height: rowH - 6, rx: 2, fill: col(r.system)}, s));
  // swimlane
  const lanes = t.steps || [];
  H = T + rowH * pcs.length + 30; const s2 = svg($("lanes"), W, H);
  txt(s2, L, 12, lanes.length ? "each bar is a chunk computing (receipt), one sub-row per concurrent chunk; grey ticks = queue, send, receive, env; wash = the script" : "no step record for this system: CPU busy ≥ 0.5 core shown instead", {style: "fill:var(--text-primary)"});
  pcs.forEach((pc, i) => {
    txt(s2, L - 8, T + rowH * i + rowH / 2 + 4, pc, {"text-anchor": "end"});
    el("line", {x1: L, x2: W - R, y1: T + rowH * (i + .5), y2: T + rowH * (i + .5), class: "gridl"}, s2);
  });
  if (lanes.length) {
    for (const st of [...lanes].sort((a, b) => (b.step === "script") - (a.step === "script"))) {
      const i = pcs.indexOf(st.pc); if (i < 0) continue;
      const work = st.step === "compute", bg = st.step === "script", g = el("g", {}, s2);
      const k = Math.max(1, (t.lane_rows || {})[st.pc] || 1), sub = (rowH - 4) / k;
      const y0 = bg ? T + rowH * i + 1 : work ? T + rowH * i + 2 + sub * (st.row || 0) : T + rowH * (i + 1) - 3;
      const hgt = bg ? rowH - 2 : work ? Math.max(1.5, sub - 1) : 2;
      el("rect", {x: x(st.t + pad), y: y0, width: Math.max(1.5, x(st.t + st.d + pad) - x(st.t + pad)), height: hgt, rx: Math.min(3, hgt / 2), fill: work || bg ? col(r.system) : "var(--muted)", opacity: bg ? .12 : 1}, g);
      hover(g, e => showTip(e, `${st.pc}: ${st.step}${st.chunk >= 0 ? " chunk " + st.chunk : ""}`, [[work ? col(r.system) : "var(--muted)", fmt(st.d, 3) + " s", "duration"], [null, fmt(st.t, 2) + " s", "start"], ...(st.bytes ? [[null, (st.bytes / 1e3).toFixed(0) + " kB", "bytes"]] : [])]));
    }
  } else {
    pcs.forEach((pc, i) => { let prev = null; for (const [tb, v] of t.cpu[pc] || []) { const ta = prev == null ? tb - 1 : prev; prev = tb; if (v < .5 || ta < pad || ta > pad + t.wall_s) continue; el("rect", {x: x(ta), y: T + rowH * i + 3, width: x(tb) - x(ta), height: rowH - 6, rx: 3, fill: col(r.system), opacity: Math.min(1, .25 + v / vmax)}, s2); } });
  }
  for (const v of [pad, pad + t.wall_s]) el("line", {x1: x(v), x2: x(v), y1: T - 4, y2: T + rowH * pcs.length + 2, stroke: "var(--text-primary)", "stroke-width": 1}, s2);
  // network
  const nets = pcs.map(pc => t.net[pc] || {tx: 0, rx: 0}), nmax = Math.max(1, ...nets.map(v => v.tx + v.rx));
  H = T + rowH * pcs.length + 26; const s3 = svg($("net"), W, H);
  txt(s3, L, 12, "MB sent (solid) and received (light) during the run", {style: "fill:var(--text-primary)"});
  const xn = scale(0, nmax, L, L + 520);
  pcs.forEach((pc, i) => {
    const v = nets[i], g = el("g", {}, s3), y0 = T + rowH * i + 3;
    txt(g, L - 8, y0 + rowH / 2 + 1, pc, {"text-anchor": "end"});
    el("rect", {x: L, y: y0, width: Math.max(0, xn(v.tx) - L), height: rowH - 6, fill: col(r.system), rx: 2}, g);
    el("rect", {x: xn(v.tx) + (v.tx ? 2 : 0), y: y0, width: Math.max(0, xn(v.tx + v.rx) - xn(v.tx)), height: rowH - 6, fill: col(r.system), opacity: .35, rx: 2}, g);
    txt(g, xn(v.tx + v.rx) + 8, y0 + rowH / 2 + 1, fmt((v.tx + v.rx) / 1e6, 2) + " MB");
    el("rect", {x: 0, y: y0 - 3, width: W, height: rowH, class: "hit"}, g);
    hover(g, e => showTip(e, pc, [[col(r.system), fmt(v.tx / 1e6, 3) + " MB", "sent"], [col(r.system), fmt(v.rx / 1e6, 3) + " MB", "received"]]));
  });
  playing = {head, bars, x, span, pad, t, pcs, vmax, W, R};
}
function frame(ts) {
  const p = playing; if (!p) return;
  p.head.setAttribute("x1", p.x(ts)); p.head.setAttribute("x2", p.x(ts)); p.head.setAttribute("opacity", 1);
  p.pcs.forEach((pc, i) => { const ser = p.t.cpu[pc] || []; const hit = ser.find(s => s[0] >= ts); p.bars[i].setAttribute("width", hit ? hit[1] / p.vmax * (p.R - 14) : 0); });
  const rel = ts - p.pad; $("clock").textContent = `t = ${rel >= 0 ? "+" : ""}${rel.toFixed(1)} s · ${rel < 0 ? "before start" : rel > p.t.wall_s ? "done" : "running"}`;
}
$("play").onclick = () => {
  if (playT) { clearInterval(playT); playT = null; $("play").textContent = "▶ Replay"; return; }
  const p = playing; if (!p) return; let ts = 0; $("play").textContent = "❚❚ Pause";
  const step = Math.max(.05, p.span / 240);
  playT = setInterval(() => { frame(ts); ts += step; if (ts > p.span) { clearInterval(playT); playT = null; $("play").textContent = "▶ Replay"; } }, 1000 / 30);
};
function drawAll() {
  const host = $("t-all"), rows = [];
  for (const exp of Object.keys(M.groups).sort()) for (const r of M.groups[exp])
    rows.push([exp + " " + r.workload, r.system, r.n, r.variant || r.fault || r.mode || "", r.median_wall_s == null ? "–" : `${fmt(r.median_wall_s)} [${fmt(r.q1_wall_s)}–${fmt(r.q3_wall_s)}]`, r.runs_used, r.warmups_excluded, r.failed_excluded]);
  table(host, ["group", "system", "N", "variant", "wall s, median [IQR]", "used", "warm-ups", "failed"], rows);
}
// ---- boot
$("label").textContent = M.label || "";
$("meta").textContent = `${M.runs.length} runs · ${M.pcs.length} PCs`;
$("nslider").max = Math.max(0, NSET.length - 1); $("nslider").value = Math.max(0, NSET.length - 1);
$("nslider").oninput = drawScale;
const pick = $("runpick");
const order = r => [r.experiment, r.workload, SYS.indexOf(fam(r.system)), r.n];
RUNS.sort((a, b) => { const x = order(a), y = order(b); for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return x[i] < y[i] ? -1 : 1; return 0; });
for (const r of RUNS) { const o = document.createElement("option"); o.value = r.run_id; o.textContent = runLabel(r); pick.appendChild(o); }
const pref = RUNS.find(r => r.experiment === "e1" && r.system === "pipedpeer" && r.n === NMAX); if (pref) pick.value = pref.run_id;
pick.onchange = () => { if (playT) { clearInterval(playT); playT = null; $("play").textContent = "▶ Replay"; } drawRun(); };
$("theme").onclick = () => { const d = document.documentElement, dark = d.dataset.theme ? d.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches; d.dataset.theme = dark ? "light" : "dark"; };
drawFrontier(); drawScale(); drawMulti(); drawEff(); drawOthers(); drawRun(); drawAll();
</script></body></html>
"""


def write(model, path):
    data = json.dumps(model, separators=(",", ":"), default=str).replace("</", "<\\/")
    with open(path, "w") as f:
        f.write(PAGE.replace("__DATA__", data))
