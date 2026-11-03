// The headline experiments: E12 elastic join, E13 churn, E14 curl to
// cluster, E15 network delay. Join and churn replay on scroll: a playhead
// sweeps the run and a strip of PC tiles lights with each PC's items per
// second, going dark while it is killed.
const series = (vals, bin) => vals.map((v, i) => [i * bin, v]);
const atBin = (vals, bin, t) => vals[Math.min(vals.length - 1, Math.max(0, Math.floor(t / bin)))] || 0;

// A strip of PC tiles at the left of a timeline panel; returns update(t).
function tileStrip(g, pcs, y0, rowH, opts) {
  const tiles = pcs.map((pc, i) => {
    const y = y0 + i * rowH;
    const gg = g.append("g").attr("transform", `translate(0,${y})`);
    const box = gg.append("rect").attr("width", 62).attr("height", rowH - 6).attr("rx", 6).attr("fill", T().raised).attr("stroke", T().border);
    const dot = gg.append("rect").attr("x", 7).attr("y", (rowH - 6) / 2 - 5).attr("width", 10).attr("height", 10).attr("rx", 2.5).attr("fill", T().cell);
    const lab = gg.append("text").attr("x", 23).attr("y", (rowH - 6) / 2).attr("dominant-baseline", "middle")
      .attr("font-size", 11).attr("font-weight", 600).attr("fill", T().ink2).text(pc);
    return { pc, gg, box, dot, lab };
  });
  return (t) => {
    for (const k of tiles) {
      const st = opts.state(k.pc, t); // {present, down, level 0..1}
      k.gg.attr("opacity", st.present ? 1 : 0.28);
      k.dot.attr("fill", st.down ? T().page : st.level > 0.02 ? opts.color : T().cell).attr("opacity", st.down ? 1 : 0.35 + 0.65 * st.level);
      k.box.attr("stroke", st.down ? T().bad : st.flash ? opts.color : T().border).attr("stroke-width", st.down || st.flash ? 1.5 : 1);
      k.lab.text(st.down ? `✕ ${k.pc}` : k.pc).attr("fill", st.down ? T().bad : T().ink2);
    }
  };
}

const Join = {
  mount(root) {
    const J = M.join || [];
    if (!J.length) return placeholder(root, "Elastic join: PCs added while a job runs, and whether each system gives them work.",
      "Needs E12 lines (experiment e12) with a per-PC timeline and join events.");
    const share = (j) => {
      const tot = d3.sum(Object.values(j.per_pc), (v) => d3.sum(v));
      const late = d3.sum(Object.entries(j.per_pc).filter(([pc]) => (j.cohorts[pc] || 0) > 0), ([, v]) => d3.sum(v));
      return tot ? late / tot : null;
    };
    const host = el("div", { class: "chart-host" });
    root.replaceChildren(el("div", { class: "fig-row" }, J.map((j) => figure(j.result_ok === false ? "failed" : d3.format(".0%")(share(j) || 0),
      `${NAME[j.system]}: share of items the joiner${j.events.flatMap((e) => e.pcs).length > 1 ? "s" : ""} ran · wall ${fmtS(j.wall)}`, { mark: j.system }))),
    host, el("div", { class: "replay-ctl" }, el("button", { class: "ctl", text: "↻ Replay", onclick: () => this.rp && this.rp.play() })),
    tableView([["System", (j) => NAME[j.system]], ["Start → final PCs", (j) => `${j.hosts.length} → ${j.hosts_final.length}`],
      ["Join at", (j) => j.events.map((e) => `${f1(e.t)} s (+${e.pcs.join(", ")})`).join("; ")],
      ["Seen by system", (j) => j.events.map((e) => (e.seen == null ? "not reported" : f1(e.seen) + " s")).join("; ")],
      ["Joiner's share", (j) => d3.format(".1%")(share(j) || 0)], ["Wall", (j) => fmtS(j.wall)], ["Source", (j) => j.source || "–"]], J));
    chart(host, (t) => this.draw(t, J));
    onEnter(host, () => this.rp && this.rp.play());
  },
  draw(target, J) {
    const rowH = 30, panelGap = 30;
    const pcsOf = (j) => [...new Set(j.hosts_final.concat(Object.keys(j.per_pc)))].sort(pcSort);
    const heights = J.map((j) => Math.max(110, pcsOf(j).length * rowH + 30));
    const f = frame(target, { height: d3.sum(heights) + panelGap * (J.length - 1) + 60, margin: { left: 80, right: 150, top: 20, bottom: 36 }, name: "e12-join" });
    const tmax = d3.max(J, (j) => j.wall || j.per_pc && d3.max(Object.values(j.per_pc), (v) => v.length * j.bin_s));
    const x = d3.scaleLinear([0, tmax], [0, f.w]);
    const ymax = d3.max(J, (j) => d3.max(d3.range(d3.max(Object.values(j.per_pc), (v) => v.length)), (i) => d3.sum(Object.values(j.per_pc), (v) => v[i] || 0)));
    const updates = [];
    const clipId = "joinclip" + Math.random().toString(36).slice(2, 7);
    const clip = f.svg.append("defs").append("clipPath").attr("id", clipId).append("rect").attr("x", 0).attr("y", -10).attr("height", f.h + 20).attr("width", f.w);
    let y0 = 0;
    J.forEach((j, idx) => {
      const ph = heights[idx];
      const g = f.g.append("g").attr("transform", `translate(0,${y0})`);
      const y = d3.scaleLinear([0, ymax * 1.05], [ph - 26, 4]);
      const n = d3.max(Object.values(j.per_pc), (v) => v.length);
      const cohorts = [...new Set(Object.values(j.cohorts))].sort();
      let base = d3.range(n).map(() => 0);
      const layers = cohorts.map((k) => {
        const pcs = Object.keys(j.per_pc).filter((pc) => (j.cohorts[pc] || 0) === k);
        const vals = d3.range(n).map((i) => d3.sum(pcs, (pc) => j.per_pc[pc][i] || 0));
        const lo = base, hi = base.map((b, i) => b + vals[i]);
        base = hi;
        return { k, pcs, lo, hi };
      });
      g.append("line").attr("x1", 0).attr("x2", f.w).attr("y1", ph - 26).attr("y2", ph - 26).attr("stroke", T().axis);
      const plot = g.append("g").attr("clip-path", `url(#${clipId})`);
      for (const L of layers) {
        const pts = d3.range(n + 1).map((i) => [i * j.bin_s, L.lo[Math.min(i, n - 1)], L.hi[Math.min(i, n - 1)]]);
        plot.append("path").attr("d", d3.area().curve(d3.curveStepAfter).x((d) => x(d[0])).y0((d) => y(d[1])).y1((d) => y(d[2]))(pts))
          .attr("fill", C(j.system)).attr("opacity", L.k === 0 ? 0.3 : 0.9);
        plot.append("path").attr("d", d3.line().curve(d3.curveStepAfter).x((d) => x(d[0])).y((d) => y(d[2]))(pts))
          .attr("fill", "none").attr("stroke", L.k === 0 ? C(j.system) : T().surface).attr("stroke-width", L.k === 0 ? 1.5 : 2);
      }
      for (const e of j.events) {
        rule(g, x(e.t), ph - 26, `+${e.pcs.join(", ")} joins`, { y0: 0 });
        if (e.seen != null) {
          g.append("path").attr("d", d3.symbol(d3.symbolTriangle, 36)()).attr("transform", `translate(${x(e.seen)},${ph - 20})`).attr("fill", T().ink2);
          text(g, x(e.seen) + 6, ph - 14, "listed", { size: 10, fill: T().muted });
        }
      }
      text(g, f.w + 12, 14, NAME[j.system], { size: 13, weight: 650, fill: T().ink });
      text(g, f.w + 12, 31, `wall ${fmtS(j.wall)}`, { size: 11.5, fill: T().ink2 });
      text(g, f.w + 12, 47, j.result_ok === false ? "✕ wrong or failed" : "✓ result correct", { size: 11.5, fill: j.result_ok === false ? T().bad : T().good });
      text(g, f.w + 12, 63, `${j.n_start ?? j.hosts.length}→${j.n_final ?? j.hosts_final.length} workers`, { size: 11, fill: T().muted });
      const pcs = pcsOf(j);
      const tot = Object.fromEntries(Object.entries(j.per_pc).map(([pc, v]) => [pc, d3.sum(v)]));
      const maxTot = d3.max(Object.values(tot)) || 1;
      const joinAt = (pc) => { const e = j.events.find((ev) => ev.pcs.includes(pc)); return e ? e.t : null; };
      const strip = tileStrip(g.append("g").attr("transform", `translate(${-f.m.left + 8},0)`), pcs, 0, rowH, {
        color: C(j.system),
        state: (pc, t) => {
          const ja = joinAt(pc);
          const v = j.per_pc[pc] ? atBin(j.per_pc[pc], j.bin_s, t) : 0;
          const peak = j.per_pc[pc] ? d3.max(j.per_pc[pc]) || 1 : 1;
          if (t >= (j.wall || 1e9)) return { present: true, level: (tot[pc] || 0) / maxTot };
          return { present: ja == null || t >= ja, level: v / peak, flash: ja != null && t >= ja && t < ja + 3 };
        },
      });
      const head = g.append("line").attr("y1", 0).attr("y2", ph - 26).attr("stroke", T().ink).attr("opacity", 0.55);
      updates.push((t) => { strip(t); head.attr("x1", x(Math.min(t, tmax))).attr("x2", x(Math.min(t, tmax))).attr("opacity", t >= tmax ? 0 : 0.55); });
      y0 += ph + panelGap;
    });
    const ax = f.g.append("g").attr("transform", `translate(0,${y0 - panelGap})`).call(d3.axisBottom(x).ticks(8).tickSizeOuter(0).tickFormat((v) => v + " s"));
    styleAxis(ax);
    text(f.g, -f.m.left + 8, y0 - panelGap + 30, "items finished per second, stacked: first PCs (light), joiners (solid)", { size: 11, fill: T().muted });
    const draw = (t) => { clip.attr("width", x(Math.min(t, tmax))); updates.forEach((u) => u(t)); };
    const was = this.rp;
    this.rp = replay(tmax, 7, draw);
    draw(was && was.playing ? 0 : tmax);
    if (was && was.playing) this.rp.play();
  },
};

const Churn = {
  CUT: 75,
  mount(root) {
    const K = M.churn || [];
    if (!K.length) return placeholder(root, "Churn: PCs killed and restarted every few seconds while the job runs.",
      "Needs E13 lines (experiment e13, fault churn) with kill and restart events.");
    const host = el("div", { class: "chart-host" });
    root.replaceChildren(el("div", { class: "fig-row" }, K.map((k) => figure(k.result_ok ? "✓ correct" : "✕ failed",
      `${NAME[k.system]}: ${k.kills} kill${k.kills === 1 ? "" : "s"}, ${f0(k.items_rerun || 0)} items recomputed, ${fmtS(k.wall)}${k.error ? " · " + k.error.replace(/^exit (\d+):.*/, "exit $1") : ""}`, { mark: k.system, cls: k.result_ok ? "good" : "bad" }))),
    host, el("div", { class: "replay-ctl" }, el("button", { class: "ctl", text: "↻ Replay", onclick: () => this.rp && this.rp.play() })),
    tableView([["System", (k) => NAME[k.system]], ["Result", (k) => (k.result_ok ? "correct" : "failed")], ["Wall", (k) => fmtS(k.wall)],
      ["No-churn wall (E1b)", (k) => fmtS(k.no_churn_wall)], ["Kills", (k) => k.kills], ["Items recomputed", (k) => f0(k.items_rerun || 0)],
      ["Items counted / total", (k) => `${f0(k.items_counted || 0)} / ${f0(k.items_total || 0)}`], ["Error", (k) => k.error || "–"]], K));
    chart(host, (t) => this.draw(t, K));
    onEnter(host, () => this.rp && this.rp.play());
  },
  draw(target, K) {
    const rowH = 30, gap = 34;
    const pcsOf = (k) => [...new Set(k.hosts.concat(Object.keys(k.per_pc)))].sort(pcSort);
    const heights = K.map((k) => pcsOf(k).length * rowH + 8);
    const f = frame(target, { height: d3.sum(heights) + gap * (K.length - 1) + 64, margin: { left: 80, right: 170, top: 22, bottom: 40 }, name: "e13-churn" });
    const tmax = Math.min(this.CUT, d3.max(K, (k) => k.wall));
    const x = d3.scaleLinear([0, tmax], [0, f.w]);
    const clipId = "churnclip" + Math.random().toString(36).slice(2, 7);
    const defs = f.svg.append("defs");
    const clip = defs.append("clipPath").attr("id", clipId).append("rect").attr("y", -10).attr("height", f.h + 20).attr("width", f.w);
    const hatch = "hatch" + clipId;
    defs.append("pattern").attr("id", hatch).attr("width", 6).attr("height", 6).attr("patternUnits", "userSpaceOnUse").attr("patternTransform", "rotate(45)")
      .append("line").attr("x1", 0).attr("y1", 0).attr("x2", 0).attr("y2", 6).attr("stroke", T().bad).attr("stroke-width", 1.2).attr("opacity", 0.55);
    const updates = [];
    let y0 = 0;
    K.forEach((k, idx) => {
      const g = f.g.append("g").attr("transform", `translate(0,${y0})`);
      const pcs = pcsOf(k);
      const peak = d3.max(Object.values(k.per_pc), (v) => d3.max(v)) || 1;
      // Down windows per PC: kill -> next restart.
      const down = {};
      for (const pc of pcs) {
        const ev = k.events.filter((e) => e.pc === pc).sort((a, b) => a.t - b.t);
        const w = [];
        ev.forEach((e) => { if (e.kind === "kill") w.push([e.t, k.wall]); else if (e.kind === "restart" && w.length) w[w.length - 1][1] = e.t; });
        down[pc] = w;
      }
      const plot = g.append("g").attr("clip-path", `url(#${clipId})`);
      pcs.forEach((pc, i) => {
        const yb = i * rowH + rowH - 4, yt = i * rowH + 4;
        const y = d3.scaleLinear([0, peak], [yb, yt]);
        g.append("line").attr("x1", 0).attr("x2", f.w).attr("y1", yb + 0.5).attr("y2", yb + 0.5).attr("stroke", T().grid);
        for (const [a, b] of down[pc]) {
          if (a > tmax) continue;
          plot.append("rect").attr("x", x(a)).attr("width", Math.max(2, x(Math.min(b, tmax)) - x(a))).attr("y", yt - 2).attr("height", yb - yt + 2).attr("fill", `url(#${hatch})`);
          g.append("path").attr("d", d3.symbol(d3.symbolCross, 30)()).attr("transform", `translate(${x(a)},${yt - 1}) rotate(45)`).attr("fill", T().bad);
        }
        const v = k.per_pc[pc];
        if (!v) return;
        const pts = series(v.slice(0, Math.ceil(tmax / k.bin_s) + 1), k.bin_s);
        plot.append("path").attr("d", stepArea(x, y, pts)).attr("fill", C(k.system)).attr("opacity", 0.2);
        plot.append("path").attr("d", d3.line().curve(d3.curveStepAfter).x((d) => x(d[0])).y((d) => y(d[1]))(pts))
          .attr("fill", "none").attr("stroke", C(k.system)).attr("stroke-width", 1.5);
      });
      if (k.wall <= tmax) rule(g, x(k.wall), pcs.length * rowH, k.result_ok ? "✓ done" : "✕ exit", { y0: -12, stroke: k.result_ok ? T().good : T().bad, fill: k.result_ok ? T().good : T().bad, opacity: 1 });
      else text(g, f.w - 4, -6, `→ runs on to ${fmtS(k.wall)}, axis cut at ${tmax} s`, { size: 10.5, anchor: "end", fill: T().muted });
      text(g, f.w + 14, 12, NAME[k.system], { size: 13, weight: 650, fill: T().ink });
      text(g, f.w + 14, 30, k.result_ok ? `✓ correct in ${fmtS(k.wall)}` : `✕ failed at ${fmtS(k.wall)}`, { size: 11.5, fill: k.result_ok ? T().good : T().bad, weight: 600 });
      text(g, f.w + 14, 46, `${k.kills} kills · ${f0(k.items_rerun || 0)} recomputed`, { size: 11, fill: T().ink2 });
      if (k.error) text(g, f.w + 14, 62, k.error.length > 26 ? k.error.slice(0, 25) + "…" : k.error, { size: 10.5, fill: T().muted });
      const strip = tileStrip(g.append("g").attr("transform", `translate(${-f.m.left + 8},0)`), pcs, 0, rowH, {
        color: C(k.system),
        state: (pc, t) => ({
          present: true, down: t < k.wall && down[pc].some(([a, b]) => t >= a && t < b),
          level: !k.per_pc[pc] || t > k.wall ? 0 : atBin(k.per_pc[pc], k.bin_s, t) / peak,
        }),
      });
      const head = g.append("line").attr("y1", 0).attr("y2", pcs.length * rowH).attr("stroke", T().ink).attr("opacity", 0.55);
      updates.push((t) => { strip(t); head.attr("x1", x(t)).attr("x2", x(t)).attr("opacity", t >= tmax ? 0 : 0.55); });
      y0 += heights[idx] + gap;
    });
    const ax = f.g.append("g").attr("transform", `translate(0,${y0 - gap})`).call(d3.axisBottom(x).ticks(8).tickSizeOuter(0).tickFormat((v) => v + " s"));
    styleAxis(ax);
    text(f.g, -f.m.left + 8, y0 - gap + 32, "items finished per second, per PC · hatched: killed until restarted", { size: 11, fill: T().muted });
    const draw = (t) => { clip.attr("width", x(Math.min(t, tmax))); updates.forEach((u) => u(t)); };
    const was = this.rp;
    this.rp = replay(tmax, 8, draw);
    draw(was && was.playing ? 0 : tmax);
    if (was && was.playing) this.rp.play();
  },
};

const EV = [["setup_start", "setup starts", d3.symbolCircle, true], ["labpc", "LAB-PC line", d3.symbolCircle, false],
  ["ssh_ok", "answers SSH", d3.symbolSquare, false], ["install_end", "pipedpeer installed", d3.symbolTriangle, false],
  ["join_end", "joined", d3.symbolDiamond, false], ["first_work", "first work", d3.symbolStar, false]];

const Staircase = {
  mount(root) {
    const S = M.staircase;
    if (!S || !S.rows.length) return placeholder(root, "Curl to cluster: from typing the install line on pc00 to every PC running work.",
      "Needs E14: timeline.jsonl with head_start, labpc, ssh_ok, install_end, join_end and first_work per PC.");
    const s = S.summary, head = S.anchor === "head_start";
    const big = head ? s.head_to_all_pcs_working_s : s.first_job_to_all_pcs_working_s;
    const host = el("div", { class: "chart-host" });
    const miss = S.missing.length ? el("div", { class: "ph-inline" }, el("span", { class: "ph-kicker", text: "Lab day fills this" }),
      " Not in this data: " + S.missing.map((m) => (EV.find((e) => e[0] === m) || [0, m])[1]).join(", ") + ". Only the job phase was rehearsed.") : null;
    root.replaceChildren(el("div", { class: "fig-row" },
      figure(big == null ? "–" : big >= 120 ? f1(big / 60) + " min" : fmtS(big), head ? "from the head line on pc00 until every PC has run work" : "from the first job's start until every PC ran work", { mark: "pipedpeer" }),
      figure(String(s.pcs), "PCs in the cluster", { noStamp: true }),
      s.first_job_end_s != null ? figure(fmtS(s.first_job_end_s), "first distributed job, start to end", { noStamp: true }) : null),
    host, miss, legend(EV.filter((e) => S.rows.some((r) => r[e[0] + "_s"] != null)).map((e) => e[0]), { label: (k) => EV.find((e) => e[0] === k)[1] }),
    tableView([["PC", (r) => r.pc]].concat(EV.map(([k, lab]) => [lab, (r) => (r[k + "_s"] == null ? "–" : fmtS(r[k + "_s"]))])), S.rows));
    // Legend markers for events, not systems.
    root.querySelectorAll(".legend .lg-mark").forEach((m, i) => {
      const e = EV.filter((ev) => S.rows.some((r) => r[ev[0] + "_s"] != null))[i];
      m.replaceChildren(d3.create("svg").attr("width", 12).attr("height", 12).call((sv) => sv.append("path").attr("transform", "translate(6,6)")
        .attr("d", d3.symbol(e[2], e[0] === "first_work" ? 60 : 36)()).attr("fill", e[0] === "first_work" ? "var(--c-pipedpeer)" : "var(--ink2)")).node());
    });
    chart(host, (t) => this.draw(t, S));
    onEnter(host, () => this.rp && this.rp.play());
  },
  draw(target, S) {
    const rows = S.rows.slice().sort((a, b) => (a.labpc_s ?? a.first_work_s ?? 0) - (b.labpc_s ?? b.first_work_s ?? 0) || pcSort(a.pc, b.pc));
    const rowH = rows.length > 10 ? 22 : 34;
    const f = frame(target, { height: rows.length * rowH + 80, margin: { left: 56, right: 30, top: 30, bottom: 40 }, name: "e14-staircase" });
    const all = rows.flatMap((r) => EV.map((e) => r[e[0] + "_s"]).filter((v) => v != null)).concat([0, S.summary.first_job_end_s || 0]);
    const minutes = d3.max(all) > 180;
    const u = minutes ? 60 : 1;
    const x = d3.scaleLinear([Math.min(0, d3.min(all)) / u, d3.max(all) / u * 1.04], [0, f.w]).nice();
    const y = (i) => i * rowH + rowH / 2;
    axisBottom(f, x, { grid: true, ticks: 8, format: (v) => v + (minutes ? " min" : " s"), label: S.anchor === "head_start" ? "since the head line on pc00" : "since the first job started" });
    rule(f.g, x(0), f.h, S.anchor === "head_start" ? "head line" : "first job starts", { y0: -22 });
    const sum = S.summary;
    if (sum.head_to_cluster_ready_s != null) rule(f.g, x(sum.head_to_cluster_ready_s / u), f.h, "cluster ready", { y0: -22 });
    if (sum.first_job_end_s != null) {
      f.g.append("rect").attr("x", x(0)).attr("width", x(sum.first_job_end_s / u) - x(0)).attr("y", 0).attr("height", f.h).attr("fill", C("pipedpeer")).attr("opacity", 0.06);
      rule(f.g, x(sum.first_job_end_s / u), f.h, "first job ends", { y0: -22 });
    }
    const clipId = "stairclip" + Math.random().toString(36).slice(2, 7);
    const clip = f.svg.append("defs").append("clipPath").attr("id", clipId).append("rect").attr("y", -40).attr("height", f.h + 60).attr("x", -20);
    const plot = f.g.append("g").attr("clip-path", `url(#${clipId})`);
    rows.forEach((r, i) => {
      text(f.g, -10, y(i), r.pc, { size: 11, anchor: "end", baseline: "middle", fill: T().ink2, weight: 600 });
      const ts = EV.map((e) => r[e[0] + "_s"]).filter((v) => v != null);
      if (ts.length > 1) plot.append("line").attr("x1", x(d3.min(ts) / u)).attr("x2", x(d3.max(ts) / u)).attr("y1", y(i)).attr("y2", y(i)).attr("stroke", T().ink2).attr("stroke-width", 1.5).attr("opacity", 0.6);
      for (const [k, lab, shape] of EV) {
        const v = r[k + "_s"];
        if (v == null) continue;
        const star = k === "first_work";
        plot.append("path").attr("transform", `translate(${x(v / u)},${y(i)})`).attr("d", d3.symbol(shape, star ? 110 : 46)())
          .attr("fill", star ? C("pipedpeer") : T().ink2).attr("stroke", T().surface).attr("stroke-width", 2).attr("paint-order", "stroke");
        hit(plot, x(v / u), y(i), 12, (ev) => tip.show(ev, `${r.pc} · ${lab}`, [[star ? C("pipedpeer") : T().ink2, fmtS(v), S.anchor === "head_start" ? "after the head line" : "after the first job started"]]));
      }
    });
    const draw = (t) => clip.attr("width", 20 + x(t));
    const dom = x.domain()[1];
    this.rp = replay(dom, 3.5, draw);
    draw(dom);
  },
};

const Network = {
  mount(root) {
    const R = M.network || [];
    if (!R.length) return placeholder(root, "Network delay: added round-trip time against wall time, per system.", "Needs E15 lines with rtt_added_ms.");
    const rtts = [...new Set(R.map((r) => r.rtt_added_ms))].sort((a, b) => a - b);
    const top = rtts[rtts.length - 1];
    const at = (s, v) => R.find((r) => r.system === s && r.rtt_added_ms === v);
    const host = el("div", { class: "chart-host" });
    root.replaceChildren(el("div", { class: "fig-row" }, SYS.filter((s) => at(s, top)).map((s) => figure(fmtX(at(s, top).slowdown_vs_0ms),
      `${NAME[s]}: wall at +${top} ms RTT over its wall at 0 ms`, { mark: s }))), host, legend(SYS.filter((s) => at(s, 0)), { line: true }),
    tableView([["System", (r) => NAME[r.system]], ["Added RTT", (r) => r.rtt_added_ms + " ms"], ["N", (r) => r.n], ["Runs", (r) => r.runs],
      ["Median wall", (r) => fmtS(r.median_wall_s)], ["vs 0 ms", (r) => fmtX(r.slowdown_vs_0ms)]], R));
    chart(host, (t) => this.draw(t, R, rtts));
  },
  draw(target, R, rtts) {
    const f = frame(target, { height: 340, margin: { left: 52, right: 120, top: 24, bottom: 44 }, name: "e15-network" });
    const x = d3.scalePoint(rtts, [0, f.w]).padding(0.1);
    const y = d3.scaleLinear([Math.min(0.8, d3.min(R, (r) => r.slowdown_vs_0ms) * 0.95), Math.max(1.3, d3.max(R, (r) => r.slowdown_vs_0ms) * 1.06)], [f.h, 0]).nice();
    axisLeft(f, y, { ticks: 5, format: (v) => f1(v) + "×", label: "wall over the same system's wall at 0 ms" });
    axisBottom(f, x, { format: (v) => `+${v} ms`, label: "round-trip time added between every pair of PCs (netem)" });
    f.g.append("line").attr("x1", 0).attr("x2", f.w).attr("y1", y(1)).attr("y2", y(1)).attr("stroke", T().ink2).attr("opacity", 0.5);
    const ends = [];
    for (const s of SYS) {
      const pts = rtts.map((v) => R.find((r) => r.system === s && r.rtt_added_ms === v)).filter(Boolean);
      if (!pts.length) continue;
      f.g.append("path").attr("d", d3.line().x((r) => x(r.rtt_added_ms)).y((r) => y(r.slowdown_vs_0ms))(pts)).attr("fill", "none").attr("stroke", C(s)).attr("stroke-width", 2).attr("stroke-linejoin", "round");
      for (const r of pts) {
        marker(f.g, s, x(r.rtt_added_ms), y(r.slowdown_vs_0ms), 64);
        hit(f.g, x(r.rtt_added_ms), y(r.slowdown_vs_0ms), 12, (ev) => tip.show(ev, `${NAME[s]} · +${r.rtt_added_ms} ms`, [[C(s), fmtX(r.slowdown_vs_0ms), "vs 0 ms"], [null, fmtS(r.median_wall_s), `median wall, ${r.runs} run${r.runs > 1 ? "s" : ""}`]]));
      }
      const e = pts[pts.length - 1];
      ends.push({ s, y: y(e.slowdown_vs_0ms), x: x(e.rtt_added_ms), v: e.slowdown_vs_0ms });
    }
    ends.sort((a, b) => a.y - b.y);
    for (let i = 1; i < ends.length; i++) ends[i].ly = Math.max(ends[i].y, (ends[i - 1].ly ?? ends[i - 1].y) + 15);
    for (const e of ends) {
      const ly = e.ly ?? e.y;
      if (Math.abs(ly - e.y) > 2) f.g.append("line").attr("x1", e.x + 8).attr("x2", e.x + 16).attr("y1", e.y).attr("y2", ly).attr("stroke", T().axis);
      text(f.g, e.x + 18, ly, `${NAME[e.s]} ${fmtX(e.v)}`, { size: 12, baseline: "middle", fill: T().ink, weight: e.s === "pipedpeer" ? 650 : 450 });
    }
  },
};
