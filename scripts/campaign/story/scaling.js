// Strong scaling (E1) and the lines-of-code frontier. Speedup is plain at
// N = 1 over the system's median wall at N, from analyse.group's rows.
const E1 = (M.groups.e1 || []).filter((r) => r.mode !== "weak");
const WORKLOADS = [...new Set(E1.map((r) => r.workload))].sort();
const NS = [...new Set(E1.filter((r) => r.system !== "plain").map((r) => r.n))].sort((a, b) => a - b);
const NMAX = d3.max(NS) || 1;
const sp = (w, s, n) => (E1.find((r) => r.workload === w && r.system === s && r.n === n) || {}).speedup_vs_plain;
const row = (w, s, n) => E1.find((r) => r.workload === w && r.system === s && r.n === n);

const Scaling = {
  n: NMAX,
  mount(root) {
    if (!E1.length) return placeholder(root, "Strong scaling: speedup over plain Python at N = 1, 4 and 16, per workload.", "Needs E1 lines in runs.jsonl.");
    const head = el("div", { class: "fig-row" });
    const ctrl = el("div", { class: "slider" },
      el("label", { for: "nslider", text: "N, PCs in the run" }),
      el("input", { id: "nslider", type: "range", min: 0, max: NS.length - 1, step: 1, value: NS.length - 1,
        oninput: (e) => { this.n = NS[+e.target.value]; out.textContent = "N = " + this.n; this.update(); } }),
      el("output", { text: "N = " + NMAX }));
    const out = ctrl.querySelector("output");
    const grid = el("div", { class: "multiples" });
    root.replaceChildren(head, ctrl, legend(SYS.concat("plain"), { line: true, label: (s) => (s === "plain" ? "ideal (N×)" : NAME[s]) }), grid,
      tableView([["Workload", (r) => r.workload], ["System", (r) => NAME[fam(r.system)] || r.system], ["N", (r) => r.n],
        ["Wall s, median [IQR]", (r) => (r.median_wall_s == null ? "–" : `${f2(r.median_wall_s)} [${f2(r.q1_wall_s)}–${f2(r.q3_wall_s)}]`)],
        ["Speedup", (r) => fmtX(r.speedup_vs_plain)], ["Used / failed", (r) => `${r.runs_used} / ${r.failed_excluded}`]], E1));
    this.head = head;
    this.cells = WORKLOADS.map((w) => {
      const c = el("div", { class: "multiple" });
      grid.append(c);
      return { w, c, item: chart(c, (t) => this.drawOne(t, w)) };
    });
    this.update();
  },
  update() {
    const n = this.n;
    const best = (s) => d3.max(WORKLOADS, (w) => sp(w, s, n));
    this.head.replaceChildren(...SYS.map((s) => figure(fmtX(best(s)), `${NAME[s]}: best speedup at N = ${n}, any workload`, { mark: s })));
    for (const c of this.cells || []) c.item.draw(c.c);
  },
  drawOne(target, w) {
    const f = frame(target, { height: 230, margin: { left: 40, right: 64, top: 30, bottom: 30 }, minWidth: 220, name: `scaling-${w}` });
    const x = (NS.length > 2 ? d3.scaleLog().base(2) : d3.scaleLinear()).domain([1, NMAX]).range([0, f.w]);
    const ymax = Math.max(NMAX, d3.max(SYS, (s) => d3.max(NS, (n) => sp(w, s, n) || 0)) || 1) * 1.08;
    const y = d3.scaleLinear([0, ymax], [f.h, 0]).nice();
    text(f.svg, 0, 16, w, { size: 13, weight: 650, fill: T().ink });
    // Below 1: slower than plain on one PC.
    f.g.append("rect").attr("x", 0).attr("width", f.w).attr("y", y(1)).attr("height", f.h - y(1)).attr("fill", T().ink).attr("opacity", 0.035);
    axisLeft(f, y, { ticks: 4, format: (v) => v + "×" });
    axisBottom(f, x, { values: NS.length ? [1].concat(NS.filter((n) => n > 1)) : [1], format: d3.format("d") });
    f.g.append("line").attr("x1", x(1)).attr("x2", x(NMAX)).attr("y1", y(1)).attr("y2", y(Math.min(NMAX, ymax))).attr("stroke", T().muted).attr("stroke-width", 1).attr("opacity", 0.6);
    text(f.g, f.w - 4, f.h - 6, "slower than plain", { size: 10, fill: T().muted, anchor: "end" });
    const nx = x(this.n);
    f.g.append("line").attr("x1", nx).attr("x2", nx).attr("y1", 0).attr("y2", f.h).attr("stroke", T().ink2).attr("opacity", 0.35);
    const labels = [];
    for (const s of SYS) {
      const pts = [1].concat(NS).filter((v, i, a) => a.indexOf(v) === i).map((n) => [n, sp(w, s, n)]).filter((p) => p[1] != null);
      if (!pts.length) continue;
      f.g.append("path").attr("d", d3.line().x((p) => x(p[0])).y((p) => y(p[1]))(pts)).attr("fill", "none")
        .attr("stroke", C(s)).attr("stroke-width", 2).attr("stroke-linejoin", "round").attr("stroke-linecap", "round");
      for (const [n, v] of pts) {
        const r = row(w, s, n);
        marker(f.g, s, x(n), y(v), n === this.n ? 90 : 46);
        hit(f.g, x(n), y(v), 12, (ev) => tip.show(ev, `${w} · ${NAME[s]} · N = ${n}`, [[C(s), fmtX(v), "speedup vs plain at N = 1"],
          [null, fmtS(r.median_wall_s), `median wall, ${r.runs_used} runs${r.failed_excluded ? `, ${r.failed_excluded} failed` : ""}`]]));
      }
      const at = pts.find((p) => p[0] === this.n);
      if (at) labels.push({ s, y: y(at[1]), v: at[1] });
    }
    // End labels at the slider's N; leader lines when they would collide.
    labels.sort((a, b) => a.y - b.y);
    for (let i = 1; i < labels.length; i++) labels[i].ly = Math.max(labels[i].y, (labels[i - 1].ly ?? labels[i - 1].y) + 13);
    for (const l of labels) {
      const ly = l.ly ?? l.y;
      if (Math.abs(ly - l.y) > 2) f.g.append("line").attr("x1", nx + 6).attr("x2", nx + 12).attr("y1", l.y).attr("y2", ly).attr("stroke", T().axis);
      text(f.g, Math.min(f.w + 4, nx + 14), ly, fmtX(l.v), { size: 11, baseline: "middle", fill: T().ink2, weight: l.s === "pipedpeer" ? 650 : 400 });
    }
    const failed = SYS.filter((s) => { const r = row(w, s, this.n); return r && r.failed_excluded; });
    if (failed.length) text(f.svg, f.width - 8, 16, "failed runs: " + failed.map((s) => NAME[s] + " " + row(w, s, this.n).failed_excluded).join(", "), { size: 10.5, anchor: "end", fill: T().muted });
  },
};

const Frontier = {
  mount(root) {
    const pl = M.port_lines || [];
    const pts = [];
    for (const w of WORKLOADS) for (const s of SYS) {
      const v = sp(w, s, NMAX), l = pl.find((p) => p.workload === w && p.system === s);
      if (v != null && l) pts.push({ w, s, v, lines: l.lines });
    }
    if (!pts.length) return placeholder(root, "Lines changed to the script against speedup at the largest N.", "Needs port-lines.csv and E1 speedups.");
    const lines = (s) => pl.filter((p) => p.system === s).map((p) => p.lines);
    root.replaceChildren(el("div", { class: "fig-row" },
      figure("0", "lines changed for PipedPeer, every workload", { mark: "pipedpeer", noStamp: true }),
      figure(`${d3.min(lines("ray"))}–${d3.max(lines("ray"))}`, "lines changed for Ray", { mark: "ray", noStamp: true }),
      figure(`${d3.min(lines("dask"))}–${d3.max(lines("dask"))}`, "lines changed for Dask", { mark: "dask", noStamp: true })),
    el("div", { class: "chart-host" }), legend(SYS),
    tableView([["Workload", (p) => p.w], ["System", (p) => NAME[p.s]], ["Lines changed", (p) => p.lines], [`Speedup at N = ${NMAX}`, (p) => fmtX(p.v)]], pts));
    chart(root.querySelector(".chart-host"), (t) => this.draw(t, pts));
  },
  draw(target, pts) {
    const f = frame(target, { height: 380, margin: { left: 52, right: 30, top: 24, bottom: 46 }, name: "frontier" });
    const x = d3.scaleLinear([-0.6, d3.max(pts, (p) => p.lines) + 1.5], [0, f.w]);
    const y = d3.scaleLinear([0, Math.max(1.2, d3.max(pts, (p) => p.v) * 1.12)], [f.h, 0]).nice();
    axisLeft(f, y, { ticks: 5, format: (v) => v + "×", label: `speedup at N = ${NMAX}` });
    axisBottom(f, x, { ticks: 8, format: d3.format("d"), label: "lines changed in the script" });
    f.g.append("line").attr("x1", 0).attr("x2", f.w).attr("y1", y(1)).attr("y2", y(1)).attr("stroke", T().muted).attr("opacity", 0.5);
    text(f.g, f.w, y(1) - 6, "plain Python on one PC", { size: 10.5, fill: T().muted, anchor: "end" });
    // The no-edit column.
    f.g.append("rect").attr("x", x(-0.45)).attr("width", x(0.45) - x(-0.45)).attr("y", 0).attr("height", f.h).attr("rx", 6)
      .attr("fill", C("pipedpeer")).attr("opacity", 0.08);
    text(f.g, x(0) + 14, 12, "no edits", { size: 11, weight: 650, fill: T().ink2 });
    const sorted = pts.slice().sort((a, b) => a.v - b.v);
    const placed = [];
    for (const p of sorted) {
      const px = x(p.lines), py = y(p.v);
      marker(f.g, p.s, px, py, 80);
      hit(f.g, px, py, 12, (ev) => tip.show(ev, `${p.w} · ${NAME[p.s]}`, [[C(p.s), fmtX(p.v), `speedup at N = ${NMAX}`], [null, String(p.lines), "lines changed"]]));
      let ly = py;
      while (placed.some((q) => Math.abs(q.x - px) < 30 && Math.abs(q.y - ly) < 12)) ly -= 12;
      placed.push({ x: px, y: ly });
      text(f.g, px + 9, ly, p.w, { size: 10.5, baseline: "middle", fill: T().muted });
    }
  },
};
