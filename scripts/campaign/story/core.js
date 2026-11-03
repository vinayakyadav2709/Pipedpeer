// The page's shared state and helpers: the model, the theme tokens (both
// modes stepped from one validated palette), formatting, the tooltip, table
// views, placeholders, and the registry every chart draws through so a theme
// switch or a resize redraws everything from the same model.
const M = JSON.parse(document.getElementById("story-data").textContent);
const SYS = ["pipedpeer", "ray", "dask"];
const ALL = ["plain", "pipedpeer", "ray", "dask"];
const NAME = { plain: "plain Python", pipedpeer: "PipedPeer", ray: "Ray", dask: "Dask" };
// Marker shape per system: identity never rests on colour alone.
const SHAPE = { plain: d3.symbolDiamond, pipedpeer: d3.symbolCircle, ray: d3.symbolSquare, dask: d3.symbolTriangle };
const FAMILY = { "pipedpeer-shim": "pipedpeer", "pipedpeer-noshim": "plain", "pipedpeer-exact": "pipedpeer",
  "pipedpeer-fast": "pipedpeer", "pipedpeer-headcount": "pipedpeer", torchrun: "ray" };
const fam = (s) => (NAME[s] ? s : FAMILY[s] || "plain");

// validate_palette.js --pairs all: slots 1-3 pass in both modes against these surfaces.
const THEMES = {
  dark: {
    mode: "dark", page: "#0b0b0a", surface: "#141413", raised: "#1b1b19", cell: "#22221f",
    ink: "#f0efec", ink2: "#c3c2b7", muted: "#8f8d86", grid: "#252523", axis: "#3a3a37",
    border: "rgba(255,255,255,0.08)", good: "#0ca30c", bad: "#e66767",
    sys: { plain: "#8f8d86", pipedpeer: "#3987e5", ray: "#d95926", dask: "#199e70" },
  },
  light: {
    mode: "light", page: "#f4f4f1", surface: "#fcfcfb", raised: "#ffffff", cell: "#ecebe6",
    ink: "#0b0b0b", ink2: "#52514e", muted: "#76746e", grid: "#e6e5df", axis: "#c3c2b7",
    border: "rgba(11,11,11,0.10)", good: "#006300", bad: "#c22f2f",
    sys: { plain: "#898781", pipedpeer: "#2a78d6", ray: "#eb6834", dask: "#1baf7a" },
  },
};
let THEME = THEMES[localStorage.getItem("story-theme") || (new URLSearchParams(location.search).get("theme")) || "dark"] || THEMES.dark;
const T = () => THEME;
const C = (s) => THEME.sys[fam(s)];
const FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';

const STAMP = M.meta.emulated ? "Emulated · container lab · sample sizes · not a result" : "";

// --- formatting ------------------------------------------------------------
const f1 = d3.format(".1f"), f2 = d3.format(".2f"), f0 = d3.format(",d");
const fmtS = (s) => (s == null ? "–" : s >= 100 ? f0(Math.round(s)) + " s" : s >= 10 ? f1(s) + " s" : f2(s) + " s");
const fmtX = (x) => (x == null ? "–" : f2(x) + "×");
const fmtB = (b) => (b == null ? "–" : b >= 1e9 ? f1(b / 1e9) + " GB" : b >= 1e6 ? f1(b / 1e6) + " MB" : b >= 1e3 ? f0(Math.round(b / 1e3)) + " kB" : f0(b) + " B");
const pcSort = (a, b) => (+(a.match(/\d+$/) || [1e9])[0]) - (+(b.match(/\d+$/) || [1e9])[0]) || d3.ascending(a, b);

// --- DOM -------------------------------------------------------------------
function el(tag, attrs, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null) continue;
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const k of kids.flat()) if (k != null) e.append(k.nodeType ? k : document.createTextNode(String(k)));
  return e;
}
const $ = (sel, root = document) => root.querySelector(sel);

// --- tooltip: one for the page, values lead, text only -----------------------
const tip = (() => {
  const box = el("div", { class: "tip", role: "status" });
  document.body.append(box);
  return {
    show(ev, title, rows) {
      box.replaceChildren(el("div", { class: "tip-title", text: title }),
        ...(rows || []).map(([color, value, label]) => el("div", { class: "tip-row" },
          color ? el("span", { class: "tip-key", style: "background:" + color }) : null,
          el("b", { text: value }), el("span", { text: label || "" }))));
      box.style.opacity = 1;
      const r = box.getBoundingClientRect();
      const x = Math.min(innerWidth - r.width - 12, ev.clientX + 14), y = ev.clientY - r.height - 12;
      box.style.transform = `translate(${x}px, ${y < 8 ? ev.clientY + 16 : y}px)`;
    },
    hide() { box.style.opacity = 0; },
  };
})();

// --- table view: every chart has one ---------------------------------------
function tableView(cols, rows) {
  const t = el("table", { class: "data" },
    el("thead", {}, el("tr", {}, cols.map((c) => el("th", { text: c[0] })))),
    el("tbody", {}, rows.map((r) => el("tr", {}, cols.map((c) => el("td", { text: c[1](r) }))))));
  return el("details", { class: "table-view" }, el("summary", { text: "Table" }), el("div", { class: "table-wrap" }, t));
}

// --- placeholder: a section with no data yet --------------------------------
function placeholder(target, what, needs) {
  target.replaceChildren(el("div", { class: "placeholder" },
    el("div", { class: "ph-grid", "aria-hidden": "true" }),
    el("div", { class: "ph-body" },
      el("div", { class: "ph-kicker", text: "Lab day fills this" }),
      el("div", { class: "ph-what", text: what }),
      needs ? el("div", { class: "ph-needs", text: needs }) : null)));
}

// --- legend: always present for two or more series ----------------------------
function legend(systems, opts = {}) {
  return el("div", { class: "legend" }, systems.map((s) => {
    const sw = el("span", { class: "lg-mark" });
    sw.append(markerSVG(s, opts.line));
    return el("span", { class: "lg-item" }, sw, el("span", { text: opts.label ? opts.label(s) : NAME[fam(s)] || s }));
  }));
}
function markerSVG(s, line) {
  // Legends live in the HTML, outside any redraw: CSS variables carry the theme.
  const v = `var(--c-${fam(s)})`;
  const svg = d3.create("svg").attr("width", line ? 22 : 12).attr("height", 12);
  if (line) svg.append("line").attr("x1", 1).attr("x2", 21).attr("y1", 6).attr("y2", 6).attr("stroke", v).attr("stroke-width", 2).attr("stroke-linecap", "round");
  svg.append("path").attr("transform", `translate(${line ? 11 : 6},6)`).attr("d", d3.symbol(SHAPE[fam(s)], 40)())
    .attr("fill", fam(s) === "plain" ? "var(--surface)" : v).attr("stroke", fam(s) === "plain" ? v : "none").attr("stroke-width", 1.5);
  return svg.node();
}

// --- registry: charts draw from the model; theme or width changes redraw ------
const REG = [];
function chart(target, draw) {
  const item = { target, draw };
  REG.push(item);
  draw(target);
  return item;
}
function redrawAll() { for (const r of REG) r.draw(r.target); }
function setTheme(mode) {
  THEME = THEMES[mode];
  localStorage.setItem("story-theme", mode);
  document.documentElement.dataset.theme = mode;
  redrawAll();
}
let resizeTimer, lastW = innerWidth;
addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => { if (Math.abs(innerWidth - lastW) > 8) { lastW = innerWidth; redrawAll(); } }, 120);
});

// --- scroll: sections fade in; a section's animation starts when it is on screen
const ON_ENTER = new Map();
const io = new IntersectionObserver((entries) => {
  for (const e of entries) {
    if (!e.isIntersecting) continue;
    e.target.classList.add("in");
    const fn = ON_ENTER.get(e.target);
    if (fn) { ON_ENTER.delete(e.target); fn(); }
  }
}, { threshold: 0.25 });
function onEnter(node, fn) { ON_ENTER.set(node, fn); io.observe(node); }

// A clock for the scroll-triggered replays (join, churn, staircase): t runs
// 0 -> duration over seconds; draw(t) is pure, so a scrub is just draw(t).
function replay(duration, seconds, draw) {
  let t0 = null, raf = null;
  const api = {
    t: duration, playing: false,
    play() {
      cancelAnimationFrame(raf); t0 = null; api.playing = true;
      const step = (now) => {
        if (t0 == null) t0 = now;
        api.t = Math.min(duration, ((now - t0) / 1000 / seconds) * duration);
        draw(api.t);
        if (api.t < duration && api.playing) raf = requestAnimationFrame(step); else api.playing = false;
      };
      raf = requestAnimationFrame(step);
    },
    stop() { api.playing = false; cancelAnimationFrame(raf); },
    seek(t) { api.stop(); api.t = t; draw(t); },
  };
  return api;
}

// Linear interpolation into [[t, v], ...] sorted by t.
function interp(series, t) {
  if (!series || !series.length) return 0;
  if (t <= series[0][0]) return series[0][1];
  const i = d3.bisector((d) => d[0]).left(series, t);
  if (i >= series.length) return series[series.length - 1][1];
  const [ta, va] = series[i - 1], [tb, vb] = series[i];
  return va + ((vb - va) * (t - ta)) / Math.max(1e-9, tb - ta);
}
