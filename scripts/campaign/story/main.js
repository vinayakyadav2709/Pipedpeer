// The page: masthead, the hero replay, then one chapter per question, each a
// headline number with its evidence chart under it. Prose that quotes a
// number reads it from the model, so the lab day's data rewrites it.
(function main() {
  document.documentElement.dataset.theme = T().mode;
  const app = $("#app");
  const meta = M.meta;
  const exportHero = new URLSearchParams(location.search).get("export") === "hero";
  if (exportHero) document.body.classList.add("export-hero");

  // --- top bar: brand, stamp, theme switch, reading progress -------------------
  const toggle = el("button", { class: "theme-toggle", "aria-label": "Switch light or dark", onclick: () => {
    setTheme(T().mode === "dark" ? "light" : "dark"); Hero.redraw(); toggle.textContent = T().mode === "dark" ? "☀ Light" : "☾ Dark";
  }, text: T().mode === "dark" ? "☀ Light" : "☾ Dark" });
  const bar = el("header", { class: "topbar" }, el("a", { class: "brand", href: "#top" }, el("span", { class: "brand-dot" }), "PipedPeer", el("span", { class: "brand-sub", text: "lab campaign" })),
    STAMP ? el("div", { class: "topstamp", text: STAMP }) : el("div"), toggle, el("div", { class: "progress" }));
  addEventListener("scroll", () => { bar.lastChild.style.transform = `scaleX(${scrollY / Math.max(1, document.body.scrollHeight - innerHeight)})`; }, { passive: true });

  // --- masthead + hero ---------------------------------------------------------
  const H = M.hero;
  const lines = (s) => (M.port_lines || []).filter((p) => p.system === s).map((p) => p.lines);
  const heroBox = el("div", { class: "hero-host" });
  const caveat = meta.emulated && H ? el("aside", { class: "callout" }, el("div", { class: "callout-k", text: "Read this before the race" }),
    el("p", { text: `These are containers on one laptop, and each one sees all ${d3.max(Object.values(meta.cores)) || "its"} of the host's cores. Ray, Dask and plain Python were held to 2 processes; PipedPeer's shim widened its local pool to every core it could see. That is most of why PipedPeer finishes first here. On real PCs every system sees the same cores; E4, where taskset caps all four the same way, is the fair comparison in this sample, and there they land within ${E4spread()}.` })) : null;
  const mast = el("section", { class: "mast", id: "top" },
    el("div", { class: "kicker", text: `Lab campaign · ${meta.pcs.length} PCs · ${meta.runs} runs${meta.label ? " · " + meta.label : ""}` }),
    el("h1", {}, "One unmodified script.", el("br"), el("span", { class: "dim", text: "Every machine in the room." })),
    el("p", { class: "dek", text: `pipedpeer run main.py builds the script's environment, ships it to the PCs it finds and spreads its Pool, joblib, pandas, numpy and torch work across them. Ray and Dask need the script rewritten: ${d3.min(lines("ray")) ?? "?"}–${d3.max(lines("ray")) ?? "?"} lines for Ray, ${d3.min(lines("dask")) ?? "?"}–${d3.max(lines("dask")) ?? "?"} for Dask. Here is the same job on all three, replayed from the runs' own telemetry.` }),
    meta.emulated ? el("div", { class: "banner" }, el("b", { text: "Emulated data. " }), `Every number on this page comes from the container sample and its rehearsal: ${meta.pcs.length} containers with 1 GB each, at sample sizes. None is a result. The lab day's PCs regenerate the page with one command, and every chart and sentence is rewritten from that data.`) : null,
    heroBox, caveat);

  // --- chapters ------------------------------------------------------------------
  const chapters = [
    ["scaling", "01", "Speedup", "Speedup with nothing changed",
      "Strong scaling: the same work on more PCs. Speedup is plain Python's median wall on one PC over each system's median wall at N. Drag N; the shaded band is slower than plain.", Scaling],
    ["frontier", "02", "Effort", "Zero lines changed",
      "What a speedup costs to get. Each mark is a workload: the lines changed to run it on that system (port-lines.csv) against its speedup at the largest N. Upper left is better.", Frontier],
    ["join", "03", "Elastic join", "A PC joins while the job runs",
      "E12. The job starts on a few PCs and more join mid-run. The stack should step up after each join; a joiner that stays flat at zero was never given work.", Join],
    ["churn", "04", "Churn", "PCs die while the job runs",
      "E13. Every few seconds a PC is killed, then restarted. The work it held has to go somewhere else, and the answer still has to be right.", Churn],
    ["staircase", "05", "Setup", "From curl to cluster",
      "E14. One row per PC: the install line, SSH, the install finishing, joining, and the first work it ran. The number is the time until every PC has run something.", Staircase],
    ["network", "06", "Latency", "Add network delay",
      "E15. netem adds round-trip time between every pair of PCs. Each line is a system's wall at that delay over its own wall at 0 ms: flat is good.", Network],
    ["losses", "07", "Honest", "Where it loses",
      "The same data, read for the cases that go against PipedPeer. These are not footnotes; each one is a thing to fix or a limit to state.", Losses],
    ["inside", "08", "Inside a run", "Where the work ran",
      "Pick any representative run (the median-wall repeat of its group). Bars are the receipt's compute steps per PC; the wash behind them is busy cores from OTel.", Lanes],
  ];
  const nav = el("nav", { class: "chapnav", "aria-label": "Chapters" }, chapters.map(([id, num, short]) => el("a", { href: "#" + id }, el("span", { text: num }), short)));
  const body = chapters.map(([id, num, short, title, lede, mod]) => {
    const host = el("div", { class: "chapter-body" });
    const sec = el("section", { class: "chapter", id }, el("div", { class: "chapter-head" },
      el("div", { class: "chapter-num", text: `${num} · ${short}` }), el("h2", { text: title }), el("p", { class: "lede", text: lede })), host);
    return { sec, host, mod, id };
  });
  const method = el("section", { class: "chapter method", id: "method" }, el("div", { class: "chapter-head" },
    el("div", { class: "chapter-num", text: "Method" }), el("h2", { text: "How this page was made" })),
  el("div", { class: "cols" },
    el("div", {}, el("h4", { text: "Data" }), el("p", { text: `${meta.runs} runs (${meta.failed} failed, excluded and counted) across ${meta.experiments.map((e) => e.toUpperCase()).join(", ")}; about ${Math.round(meta.span_min)} minutes of runs. Per-PC CPU from OTel ${[...new Set(Object.values(meta.cpu_source))].join(" and ")} samples, 1 s apart, interpolated in the replays. Chunk placement from the pipedpeer receipt and timeline.py's chunks.json. Medians with IQR over repeats; warm-ups excluded.` })),
    el("div", {}, el("h4", { text: "Regenerate" }), el("pre", {}, el("code", { text: meta.command })), el("p", { text: `Generated ${meta.generated}. One file, no network: data, d3 v7.9.0 and the scripts are inlined. Every chart has a Table under it.` })),
    el("div", {}, el("h4", { text: "Colour" }), el("p", { text: "PipedPeer, Ray and Dask take the first three slots of one categorical palette, checked for colour-vision separation in both modes. Each system also has its own marker shape (● ■ ▲, plain ◇) and direct labels, so no reading depends on colour alone." }))));

  if (exportHero) {
    if (H) heroBox.dataset.title = `PipedPeer · Ray · Dask — ${H.workload} on ${H.n} worker PC${H.n > 1 ? "s" : ""}`;
    heroBox.dataset.stamp = STAMP;
    app.replaceChildren(heroBox);
    mountHero(heroBox, true);
    return;
  }
  app.replaceChildren(bar, mast, nav, ...body.map((b) => b.sec), method,
    el("footer", { class: "foot", text: STAMP ? STAMP + "." : "PipedPeer lab campaign." }));
  mountHero(heroBox, false);
  for (const b of body) {
    try { b.mod.mount(b.host); } catch (e) { console.error(b.id, e); placeholder(b.host, "This section could not be drawn from the data.", String(e.message || e)); }
    io.observe(b.sec);
  }
  const extra = el("div", { class: "chapter-sub" });
  body.find((b) => b.id === "churn").host.append(extra);
  E6.mount(extra);
  const energy = el("div", { class: "chapter-sub" }, el("div", { class: "mini-title", text: "Energy" }), el("div"));
  body.find((b) => b.id === "inside").host.append(energy);
  Energy.mount(energy.lastChild);
  // Chapter nav highlights the section on screen.
  const navIO = new IntersectionObserver((es) => es.forEach((e) => {
    if (e.isIntersecting) nav.querySelectorAll("a").forEach((a) => a.classList.toggle("on", a.getAttribute("href") === "#" + e.target.id));
  }), { rootMargin: "-45% 0px -50% 0px" });
  body.forEach((b) => navIO.observe(b.sec));

  function mountHero(box, still) {
    Hero.mount(box);
    window.__story = { hero: Hero, ready: true, sections: chapters.map((c) => c[0]), replays: () => [Join.rp, Churn.rp, Staircase.rp] };
    if (!still && Hero.ok) setTimeout(() => Hero.play(), 500);
  }
  function E4spread() {
    const g = (M.groups.e4 || []).filter((r) => r.median_wall_s != null);
    if (!g.length) return "a few percent of each other";
    return `${f1(d3.min(g, (r) => r.median_wall_s))}–${f1(d3.max(g, (r) => r.median_wall_s))} s`;
  }
})();
