// Where PipedPeer loses (drawn as plainly as where it wins), the swimlanes
// of one run, network bytes per PC, and energy.

// A horizontal dot plot: one row per item, median dot, IQR whisker.
function dotPlot(target, items, { name, unit = "s", height, highlight, ref } = {}) {
  const rowH = 34;
  const f = frame(target, { height: height || items.length * rowH + 48, margin: { left: 132, right: 66, top: 10, bottom: 34 }, minWidth: 260, name });
  const hiV = d3.max(items, (d) => d.hi ?? d.v);
  const x = d3.scaleLinear([0, hiV * 1.1], [0, f.w]).nice();
  axisBottom(f, x, { grid: true, ticks: 5, format: (v) => v + (unit === "×" ? "×" : unit === "ms" ? " ms" : " s") });
  if (ref != null) f.g.append("line").attr("x1", x(ref)).attr("x2", x(ref)).attr("y1", 0).attr("y2", f.h).attr("stroke", T().ink2).attr("opacity", 0.6);
  items.forEach((d, i) => {
    const yy = i * rowH + rowH / 2;
    const strong = highlight && highlight(d);
    text(f.g, -12, yy, d.label, { size: 12, anchor: "end", baseline: "middle", fill: strong ? T().ink : T().ink2, weight: strong ? 650 : 400 });
    if (d.lo != null && d.hi != null) f.g.append("line").attr("x1", x(d.lo)).attr("x2", x(d.hi)).attr("y1", yy).attr("y2", yy).attr("stroke", C(d.s)).attr("stroke-width", 2).attr("stroke-linecap", "round").attr("opacity", 0.6);
    for (const p of d.points || []) f.g.append("circle").attr("cx", x(p)).attr("cy", yy).attr("r", 3).attr("fill", C(d.s)).attr("opacity", 0.45);
    if (d.v == null) { text(f.g, 6, yy, d.missing || "no usable run", { size: 11, baseline: "middle", fill: T().muted }); return; }
    const mk = marker(f.g, d.s, x(d.v), yy, strong ? 100 : 64);
    if (d.hollow) mk.attr("fill", T().raised).attr("stroke", C(d.s)).attr("stroke-width", 1.5);
    text(f.g, x(Math.max(d.hi ?? d.v, ...(d.points || []))) + 10, yy, d.text || (unit === "×" ? fmtX(d.v) : unit === "ms" ? f0(Math.round(d.v)) + " ms" : fmtS(d.v)), { size: 11.5, baseline: "middle", fill: T().ink2, weight: strong ? 650 : 400 });
    hit(f.g, x(d.v), yy, 14, (ev) => tip.show(ev, d.label, [[C(d.s), d.text || String(d.v), d.note || ""]]));
  });
  return f;
}

const Losses = {
  mount(root) {
    const cards = [this.w2(), this.e4(), this.e7(), this.joiner()].filter(Boolean);
    if (!cards.length) return placeholder(root, "Where PipedPeer loses: the slower workloads, the uneven-PC split, the overhead per job.", "Needs E1, E4, E7 and E12 lines.");
    root.replaceChildren(el("div", { class: "loss-grid" }, cards.map((c) => {
      const host = el("div", { class: "chart-host" });
      const node = el("article", { class: "loss" }, el("div", { class: "loss-tag" }, el("span", { class: "loss-icon", text: "▼" }), c.tag),
        el("h3", { text: c.title }), el("p", { text: c.body }), host, c.table, STAMP ? el("div", { class: "stamp", text: STAMP }) : null);
      setTimeout(() => chart(host, c.draw));
      return node;
    })));
  },
  // The workload where PipedPeer is slowest relative to plain at the largest N.
  w2() {
    const cand = WORKLOADS.map((w) => ({ w, v: sp(w, "pipedpeer", NMAX) })).filter((d) => d.v != null).sort((a, b) => a.v - b.v)[0];
    if (!cand || cand.v >= 1) return null;
    const items = ["plain"].concat(SYS).map((s) => {
      const r = row(cand.w, s, s === "plain" ? 1 : NMAX) || {};
      return { s, label: `${NAME[s]}${s === "plain" ? ", N = 1" : ""}`, v: r.speedup_vs_plain, note: `median wall ${fmtS(r.median_wall_s)}, ${r.runs_used || 0} used, ${r.failed_excluded || 0} failed` };
    });
    const pr = row(cand.w, "pipedpeer", NMAX) || {};
    return {
      tag: `${cand.w} at N = ${NMAX}`, title: `Slower than plain Python on ${cand.w}`,
      body: `At N = ${NMAX} PipedPeer reaches ${fmtX(cand.v)} of plain's speed on one PC${pr.failed_excluded ? `, and ${pr.failed_excluded} of its runs failed` : ""}. ${cand.w === "W2" ? "W2 ships large arrays; in a 1 GB container one run hit the OOM killer (exit 137)." : ""}`,
      draw: (t) => dotPlot(t, items, { name: `loss-${cand.w}`, unit: "×", ref: 1, highlight: (d) => d.s === "pipedpeer" }),
      table: tableView([["System", (d) => d.label], ["Speedup", (d) => fmtX(d.v)], ["Note", (d) => d.note]], items),
    };
  },
  e4() {
    const G = M.groups.e4 || [];
    if (!G.length) return null;
    const lab = { pipedpeer: "PipedPeer, measured split", "pipedpeer-headcount": "PipedPeer, even split", ray: "Ray", dask: "Dask" };
    const items = G.filter((r) => r.median_wall_s != null).map((r) => ({ s: fam(r.system), label: lab[r.system] || r.system, v: r.median_wall_s,
      lo: r.q1_wall_s, hi: r.q3_wall_s, hollow: r.system === "pipedpeer-headcount", sys: r.system, note: `median [IQR] ${f2(r.q1_wall_s)}–${f2(r.q3_wall_s)} s, ${r.runs_used} runs` }))
      .sort((a, b) => a.v - b.v);
    const me = items.find((d) => d.sys === "pipedpeer");
    const rank = items.indexOf(me) + 1;
    return {
      tag: "E4 · uneven PCs", title: rank === items.length ? "Its measured split came last" : `Its measured split ranked ${rank} of ${items.length}`,
      body: `Every system capped the same way (taskset). PipedPeer's speed-measured split took ${fmtS(me && me.v)}, slower than the even split it replaced and than ${items.filter((d) => d.v < (me ? me.v : 0) && d.s !== "pipedpeer").map((d) => d.label).join(" and ") || "none"}.`,
      draw: (t) => dotPlot(t, items, { name: "loss-e4", highlight: (d) => d.sys === "pipedpeer" }),
      table: tableView([["Variant", (d) => d.label], ["Median wall", (d) => fmtS(d.v)], ["IQR", (d) => `${f2(d.lo)}–${f2(d.hi)}`]], items),
    };
  },
  e7() {
    const R = M.e7 || [];
    const on = R.filter((r) => r.system === "pipedpeer-shim"), off = R.filter((r) => r.system === "pipedpeer-noshim");
    if (!on.length || !off.length) return null;
    const mOn = d3.median(on, (r) => r.wall), mOff = d3.median(off, (r) => r.wall);
    const items = [{ s: "plain", label: "shim off", v: mOff * 1000, points: off.map((r) => r.wall * 1000), hollow: true },
      { s: "pipedpeer", label: "shim on", v: mOn * 1000, points: on.map((r) => r.wall * 1000) }];
    return {
      tag: "E7 · overhead", title: `+${f0(Math.round((mOn - mOff) * 1000))} ms on every job`,
      body: `A trivial script, whole job, shim on against shim off: the interception costs ${f0(Math.round((mOn - mOff) * 1000))} ms per job here (${on.length} and ${off.length} repeats). On a 10 s job that is ${d3.format(".1%")((mOn - mOff) / 10)}; on a 0.2 s job it is most of the job.`,
      draw: (t) => dotPlot(t, items, { name: "loss-e7", unit: "ms", highlight: (d) => d.s === "pipedpeer" }),
      table: tableView([["Shim", (d) => d.label], ["Median ms", (d) => f0(Math.round(d.v))], ["Repeats, ms", (d) => d.points.map((p) => Math.round(p)).join(", ")]], items),
    };
  },
  joiner() {
    const j = (M.join || []).find((x) => x.system === "pipedpeer");
    if (!j) return null;
    const pcs = Object.keys(j.per_pc).sort(pcSort);
    const tot = d3.sum(pcs, (pc) => d3.sum(j.per_pc[pc]));
    const parts = pcs.map((pc) => ({ pc, v: d3.sum(j.per_pc[pc]), late: (j.cohorts[pc] || 0) > 0 }));
    const late = d3.sum(parts.filter((p) => p.late), (p) => p.v);
    return {
      tag: "E12 · elastic join", title: `The joiner ran ${d3.format(".0%")(late / tot)} of the items`,
      body: `PipedPeer listed the new PC ${j.events[0] && j.events[0].seen != null ? f1(j.events[0].seen - j.events[0].t) + " s after it joined" : "late"}, then gave it ${f0(late)} of ${f0(tot)} items. Dask gave its joiner far more, though its run took ${fmtS(((M.join || []).find((x) => x.system === "dask") || {}).wall)}.`,
      draw: (t) => this.segBar(t, parts, tot),
      table: tableView([["PC", (p) => p.pc], ["Items", (p) => f0(p.v)], ["Share", (p) => d3.format(".1%")(p.v / tot)], ["Joined mid-run", (p) => (p.late ? "yes" : "no")]], parts),
    };
  },
  // Tall segmented bar: one whole, split by PC; joiners solid, the rest light.
  segBar(target, parts, tot) {
    const f = frame(target, { height: 96, margin: { left: 0, right: 0, top: 18, bottom: 28 }, minWidth: 260, name: "loss-joiner" });
    const x = d3.scaleLinear([0, tot], [0, f.w]);
    let acc = 0;
    for (const p of parts) {
      const w = Math.max(0, x(p.v) - 2);
      if (w > 0) f.g.append("rect").attr("x", x(acc)).attr("width", w).attr("height", 28).attr("rx", 4).attr("fill", C("pipedpeer")).attr("opacity", p.late ? 1 : 0.28);
      const lab = `${p.pc} ${d3.format(".0%")(p.v / tot)}`;
      if (w > lab.length * 7 + 12) text(f.g, x(acc) + 8, 14, lab, { size: 11.5, baseline: "middle", fill: p.late ? "#ffffff" : T().ink, weight: 600 });
      else text(f.g, x(acc) + w / 2, 46, lab, { size: 11, anchor: "middle", fill: T().ink2, weight: 600 });
      f.g.append("rect").attr("x", x(acc)).attr("width", Math.max(w, 24)).attr("height", 28).attr("fill", "transparent")
        .on("pointermove", (ev) => tip.show(ev, p.pc, [[C("pipedpeer"), f0(p.v) + " items", d3.format(".1%")(p.v / tot) + (p.late ? ", joined mid-run" : "")]])).on("pointerleave", () => tip.hide());
      acc += p.v;
    }
    text(f.g, 0, -6, "items run, by PC", { size: 11, fill: T().muted });
  },
};

const Lanes = {
  mount(root) {
    const L = (M.lanes || []).filter((l) => l.compute.length || Object.keys(l.cpu).length);
    if (!L.length) return placeholder(root, "Swimlanes: each chunk's compute step on each PC.", "Needs receipts (steps) or OTel CPU per run.");
    const pick = el("select", { class: "pick", "aria-label": "Run", onchange: () => this.show(+pick.value) });
    L.forEach((l, i) => pick.append(el("option", { value: i, text: `${l.experiment.toUpperCase()} · ${l.workload} · ${NAME[fam(l.system)] || l.system}${l.system !== fam(l.system) ? " (" + l.system + ")" : ""} · N = ${l.n}${l.fault && l.fault !== "none" ? " · " + l.fault : ""} · ${fmtS(l.wall)}` })));
    const best = Math.max(0, L.findIndex((l) => l.system === "pipedpeer" && l.compute.length && l.n === d3.max(L, (q) => q.n) && l.experiment === "e1"));
    pick.value = best;
    this.L = L;
    this.cur = L[best];
    this.host = el("div", { class: "chart-host" });
    this.net = el("div", { class: "chart-host" });
    root.replaceChildren(el("div", { class: "toolbar" }, pick), el("div", { class: "lanes-split" },
      el("div", {}, el("div", { class: "mini-title", text: "Where the work ran" }), this.host),
      el("div", {}, el("div", { class: "mini-title", text: "Bytes on the wire, per PC" }), this.net)));
    this.lane = chart(this.host, (t) => this.drawLanes(t));
    this.bytes = chart(this.net, (t) => this.drawNet(t));
    this.show(best);
  },
  show(i) { this.cur = this.L[i]; this.lane.draw(this.host); this.bytes.draw(this.net); },
  drawLanes(target) {
    const l = this.cur;
    if (!l) return;
    const pcs = [...new Set(M.meta.pcs.concat(l.compute.map((c) => c[0])))].sort(pcSort);
    const sub = (pc) => (l.rows && l.rows[pc]) || 1;
    const rowH = 16, gap = 12;
    const heights = pcs.map((pc) => Math.max(1, sub(pc)) * rowH + 22);
    const f = frame(target, { height: d3.sum(heights) + gap * pcs.length + 44, margin: { left: 56, right: 18, top: 8, bottom: 36 }, name: "swimlanes" });
    const tmax = Math.max(l.wall || 0, d3.max(l.compute, (c) => c[2] + c[3]) || 0);
    const x = d3.scaleLinear([0, tmax], [0, f.w]).nice();
    axisBottom(f, x, { grid: true, ticks: 8, format: (v) => v + " s" });
    const peak = d3.max(Object.values(l.cpu), (s) => d3.max(s, (p) => p[1])) || 1;
    let y0 = 0;
    pcs.forEach((pc, i) => {
      const hh = heights[i];
      text(f.g, -10, y0 + 12, pc, { size: 11, anchor: "end", baseline: "middle", fill: l.hosts.includes(pc) ? T().ink2 : T().muted, weight: 600 });
      // CPU wash behind the lanes: every system has it.
      const cpu = (l.cpu[pc] || []).filter((p) => p[0] >= -0.5 && p[0] <= tmax + 0.5);
      if (cpu.length) {
        const y = d3.scaleLinear([0, peak], [y0 + hh - 4, y0 + 2]);
        f.g.append("path").attr("d", d3.area().curve(d3.curveMonotoneX).x((p) => x(Math.max(0, p[0]))).y0(y0 + hh - 4).y1((p) => y(p[1]))(cpu)).attr("fill", C(l.system)).attr("opacity", 0.1);
        f.g.append("path").attr("d", d3.line().curve(d3.curveMonotoneX).x((p) => x(Math.max(0, p[0]))).y((p) => y(p[1]))(cpu)).attr("fill", "none").attr("stroke", C(l.system)).attr("stroke-width", 1).attr("opacity", 0.5);
      }
      for (const [cpc, r, t, d] of l.compute.filter((c) => c[0] === pc)) {
        const bx = x(t), bw = Math.max(2, x(t + d) - x(t) - 2), by = y0 + 4 + r * rowH;
        f.g.append("rect").attr("x", bx).attr("y", by).attr("width", bw).attr("height", rowH - 4).attr("rx", 3).attr("fill", C(l.system)).attr("opacity", 0.9)
          .on("pointermove", (ev) => tip.show(ev, `${cpc} · compute`, [[C(l.system), fmtS(d), `from ${f2(t)} s`]])).on("pointerleave", () => tip.hide());
      }
      if (!l.compute.length && !cpu.length) text(f.g, 6, y0 + 12, "no samples", { size: 10.5, fill: T().muted, baseline: "middle" });
      y0 += hh + gap;
    });
    text(f.g, -f.m.left + 4, f.h + 30, l.compute.length ? "bars: the receipt's compute steps · wash: busy cores (OTel)" : "busy cores per PC (OTel); this system records no per-task timeline here", { size: 10.5, fill: T().muted });
  },
  drawNet(target) {
    const l = this.cur;
    const pcs = Object.keys(l.net || {}).sort(pcSort);
    if (!pcs.length) return placeholder(target, "Bytes sent and received per PC during this run.", "Needs OTel system.network.io for the run's window.");
    const rowH = 30;
    const f = frame(target, { height: pcs.length * rowH + 60, margin: { left: 48, right: 70, top: 8, bottom: 40 }, minWidth: 240, name: "net-bytes" });
    const x = d3.scaleLinear([0, d3.max(pcs, (pc) => l.net[pc].tx + l.net[pc].rx) || 1], [0, f.w]).nice();
    axisBottom(f, x, { grid: true, ticks: 4, format: (v) => fmtB(v) });
    pcs.forEach((pc, i) => {
      const yy = i * rowH + 6, { tx, rx } = l.net[pc];
      text(f.g, -10, yy + 9, pc, { size: 11, anchor: "end", baseline: "middle", fill: T().ink2, weight: 600 });
      const wt = Math.max(0, x(tx) - 2), wr = Math.max(0, x(tx + rx) - x(tx));
      f.g.append("rect").attr("x", 0).attr("y", yy).attr("width", wt).attr("height", 18).attr("rx", 3).attr("fill", C(l.system));
      f.g.append("rect").attr("x", x(tx)).attr("y", yy).attr("width", wr).attr("height", 18).attr("rx", 3).attr("fill", C(l.system)).attr("opacity", 0.4);
      text(f.g, x(tx + rx) + 8, yy + 9, fmtB(tx + rx), { size: 11, baseline: "middle", fill: T().ink2 });
      f.g.append("rect").attr("x", 0).attr("y", yy - 4).attr("width", Math.max(x(tx + rx), 24)).attr("height", 26).attr("fill", "transparent")
        .on("pointermove", (ev) => tip.show(ev, pc, [[C(l.system), fmtB(tx), "sent"], [C(l.system), fmtB(rx), "received"]])).on("pointerleave", () => tip.hide());
    });
    text(f.g, -f.m.left + 4, f.h + 32, "solid: sent · light: received", { size: 10.5, fill: T().muted });
  },
};

const E6 = {
  mount(root) {
    const R = (M.e6 || []).filter((r) => SYS.includes(r.system));
    if (!R.length) return root.remove();
    const items = [];
    for (const s of SYS) {
      const none = R.find((r) => r.system === s && r.fault === "none"), kill = R.find((r) => r.system === s && r.fault && r.fault !== "none");
      if (none) items.push({ s, label: `${NAME[s]}, no fault`, v: none.wall_s, hollow: true });
      if (kill) items.push({ s, label: `${NAME[s]}, one kill`, v: kill.wall_s, text: `${fmtS(kill.wall_s)} (${kill.wall_s - (none ? none.wall_s : kill.wall_s) >= 0 ? "+" : "−"}${f1(Math.abs(kill.wall_s - (none ? none.wall_s : kill.wall_s)))} s)${kill.result_ok ? " ✓" : " ✕"}` });
    }
    const host = el("div", { class: "chart-host" });
    root.replaceChildren(el("div", { class: "mini-title", text: "E6 · one worker killed at 30% of the run (hollow: no fault)" }), host,
      tableView([["Run", (d) => d.label], ["Wall", (d) => d.text || fmtS(d.v)]], items));
    chart(host, (t) => dotPlot(t, items, { name: "e6-kill", highlight: (d) => d.s === "pipedpeer" }));
  },
};

const Energy = {
  mount(root) {
    if (!M.energy) placeholder(root, "Energy per job: joules from RAPL on each PC, per system, at the same N.", "The lab day's telemetry has no energy scraper yet; this section stays empty until it does.");
  },
};
