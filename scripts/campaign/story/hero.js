// The hero: every PC as a tile of cores, for PipedPeer, Ray and Dask side by
// side on the same workload and N, replayed from the run's own records. A
// core cell fills with the PC's busy cores (OTel CPU samples, interpolated
// between 1 s samples); a particle is a chunk of items leaving the submitter
// for a PC and its result coming back (the receipt's compute steps, or
// chunks.json). Where a system recorded no per-task timeline, the flow is
// inferred from the PC's CPU and the panel says so. draw(t) is a pure
// function of t: play, scrub and the video export all call it.
const Hero = (() => {
  const H = M.hero;
  let canvas, ctx, W = 0, Hh = 0, dpr = 1, layout = null, t = -0.6, playing = false, speed = 1, raf = null, last = null;
  const ui = {};
  const systems = H ? SYS.filter((s) => H.systems[s]) : [];
  const pcs = (M.meta.pcs || []).slice().sort(pcSort);
  const sub = M.meta.submitter;
  const T0 = -0.6;
  const T1 = H ? d3.max(systems, (s) => H.systems[s].wall || 0) + 1.2 : 1;

  // Flights: [{pc, ts, dur, out, k}] per system, from chunks or inferred from CPU.
  const flights = {};
  const totals = {};
  for (const s of systems) {
    const d = H.systems[s];
    const f = [];
    if (d.chunks && d.chunks.length) {
      d.chunks.forEach(([pc, t0, t1, items], i) => {
        if (pc === sub) return;
        const k = items ? Math.max(2, Math.min(7, Math.round(Math.log2(items + 1)))) : 4;
        f.push({ pc, ts: t0 - 0.32, dur: 0.32, out: true, k, i });
        f.push({ pc, ts: t1, dur: 0.32, out: false, k: Math.max(1, k >> 1), i });
      });
    } else {
      for (const pc of d.hosts) {
        if (pc === sub || !d.cpu[pc]) continue;
        for (let tt = 0; tt < (d.wall || 0); tt += 0.25) {
          const b = interp(d.cpu[pc], tt);
          if (b > 0.25) f.push({ pc, ts: tt, dur: 0.3, out: (Math.round(tt * 4) & 1) === 0, k: Math.min(6, 1 + Math.round(b)), i: Math.round(tt * 4) });
        }
      }
    }
    flights[s] = f;
    const grid = d3.range(T0, T1, 0.1);
    totals[s] = grid.map((tt) => [tt, d3.sum(Object.values(d.cpu), (ser) => (tt > (d.wall || 0) + 0.6 ? 0 : interp(ser, tt)))]);
  }
  const peak = d3.max(systems, (s) => d3.max(totals[s], (p) => p[1])) || 1;
  const cores = (pc) => M.meta.cores[pc] || 8;

  function mount(root) {
    if (!H) {
      placeholder(root, "The cluster replay: every PC's cores filling and draining through one run, PipedPeer, Ray and Dask side by side.",
        "Needs one E1 run per system with OTel CPU samples (system.cpu.time per lab.pc_id).");
      return;
    }
    canvas = el("canvas", { class: "hero-canvas", "aria-label": "Replay of one run per system: busy cores per PC over time" });
    ui.play = el("button", { class: "ctl play", "aria-label": "Play", onclick: () => (playing ? pause() : play()) });
    ui.scrub = el("input", { type: "range", min: T0, max: T1, step: 0.01, value: t, class: "scrub", "aria-label": "Time",
      oninput: (e) => { pause(); t = +e.target.value; draw(); } });
    ui.time = el("span", { class: "ctl-time" });
    ui.speed = el("div", { class: "seg" }, [0.25, 0.5, 1, 2].map((v) => el("button", {
      class: v === speed ? "on" : "", text: v + "×", onclick: (e) => { speed = v; ui.speed.querySelectorAll("button").forEach((b) => b.classList.toggle("on", b === e.target)); },
    })));
    const w = H.systems;
    const run = el("div", { class: "hero-meta" },
      `${H.workload} at N = ${H.n} · one representative run per system (median wall) · cores from ${[...new Set(systems.flatMap((s) => w[s].cpu_source))].join(", ") || "OTel"} CPU samples · `,
      el("span", { class: "chip" }, "particles: " + systems.map((s) => `${NAME[s]} ${w[s].source === "cpu" ? "inferred from CPU" : w[s].source}`).join(" · ")));
    root.replaceChildren(el("div", { class: "hero-stage" }, canvas),
      el("div", { class: "controls" }, ui.play, ui.scrub, ui.time, ui.speed), run);
    new ResizeObserver(() => size()).observe(canvas);
    size();
    setPlayIcon();
  }

  function size() {
    const r = canvas.getBoundingClientRect();
    dpr = window.devicePixelRatio || 1;
    W = Math.round(r.width);
    layout = computeLayout(W);
    Hh = layout.height;
    canvas.style.height = Hh + "px";
    canvas.width = W * dpr;
    canvas.height = Hh * dpr;
    ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    draw();
  }

  function computeLayout(width) {
    const big = document.body.classList.contains("export-hero");
    const gap = width > 900 ? 28 : 16;
    const stacked = width < 700;
    const colW = stacked ? width : (width - gap * (systems.length - 1)) / systems.length;
    const n = pcs.length;
    // A few PCs at 1080p: one tile per row, cores in a wide grid, so the tiles carry the frame.
    const wide = big && n <= 4;
    const cols = wide ? 1 : n <= 3 ? n : n <= 4 ? 2 : n <= 9 ? 3 : n <= 16 ? 4 : 5;
    const rows = Math.ceil(n / cols);
    const tg = big ? 12 : 8;
    const tileW = (colW - tg * (cols - 1)) / cols;
    const maxCores = d3.max(pcs, cores);
    const cc = wide ? Math.ceil(maxCores / 2) : Math.ceil(Math.sqrt(maxCores * 1.25));
    const cr = Math.ceil(maxCores / cc);
    const pad = big ? 12 : 9, head = big ? 30 : 24;
    const cell = Math.min(wide ? 46 : big ? 34 : 22, (tileW - pad * 2 - (cc - 1) * 3) / cc);
    const tileH = head + pad + cr * cell + (cr - 1) * 3;
    const top = big ? 64 : 56;
    // At 1080p the replay fills the frame: the busy-cores strip takes what the tiles leave.
    const tilesH = rows * tileH + (rows - 1) * tg;
    const spark = big ? Math.max(140, Math.min(230, 800 - top - tilesH - 22)) : 76;
    const panelH = top + rows * tileH + (rows - 1) * tg + 22 + spark;
    const height = stacked ? panelH * systems.length + gap * (systems.length - 1) : panelH;
    const panels = systems.map((s, i) => ({ s, x: stacked ? 0 : i * (colW + gap), y: stacked ? i * (panelH + gap) : 0 }));
    const tiles = {};
    pcs.forEach((pc, i) => { tiles[pc] = { x: (i % cols) * (tileW + tg), y: top + Math.floor(i / cols) * (tileH + tg) }; });
    return { colW, panels, tiles, tileW, tileH, cell, cc, pad, head, top, spark, sparkY: top + rows * tileH + (rows - 1) * tg + 22, height, big };
  }

  function rr(x, y, w, h, r) { ctx.beginPath(); ctx.roundRect(x, y, w, h, r); }

  function draw() {
    if (!ctx) return;
    const th = T(), L = layout;
    ctx.clearRect(0, 0, W, Hh);
    for (const p of L.panels) drawPanel(p, th, L);
    ui.scrub.value = t;
    ui.time.textContent = `t = ${t < 0 ? "0.00" : f2(t)} s`;
  }

  function drawPanel({ s, x, y }, th, L) {
    const d = H.systems[s], col = C(s), wall = d.wall || 0, done = t >= wall;
    ctx.save();
    ctx.translate(x, y);
    // Head: name, then live status.
    ctx.fillStyle = col;
    ctx.beginPath(); ctx.arc(6, 14, 5, 0, 7); ctx.fill();
    ctx.fillStyle = th.ink;
    ctx.font = `650 ${L.big ? 24 : 18}px ${FONT}`;
    ctx.textBaseline = "middle";
    ctx.fillText(NAME[s], 20, 14);
    ctx.font = `500 ${L.big ? 16 : 13}px ${FONT}`;
    ctx.textAlign = "right";
    if (done) {
      ctx.fillStyle = th.ink;
      ctx.fillText(`✓ done in ${fmtS(wall)}`, L.colW, 14);
    } else {
      ctx.fillStyle = th.muted;
      ctx.fillText(t < 0 ? "starting" : `running · ${f1(Math.max(0, t))} s`, L.colW, 14);
    }
    ctx.textAlign = "left";
    const busyNow = done ? 0 : d3.sum(Object.values(d.cpu), (ser) => interp(ser, t));
    ctx.fillStyle = th.muted;
    ctx.font = `400 ${L.big ? 15 : 12}px ${FONT}`;
    ctx.fillText(`${f1(busyNow)} cores busy across ${d.hosts.length} PC${d.hosts.length > 1 ? "s" : ""}`, 20, L.big ? 42 : 36);

    for (const pc of pcs) drawTile(s, d, pc, th, L, col, done);
    drawFlights(s, d, th, L, col);
    drawSpark(s, th, L, col);
    ctx.restore();
  }

  function drawTile(s, d, pc, th, L, col, done) {
    const { x, y } = L.tiles[pc];
    const inRun = d.hosts.includes(pc);
    const busy = done || !inRun ? 0 : Math.max(0, interp(d.cpu[pc], t));
    ctx.globalAlpha = inRun ? 1 : 0.38;
    rr(x, y, L.tileW, L.tileH, 10);
    ctx.fillStyle = th.raised;
    ctx.fill();
    ctx.strokeStyle = pc === sub ? th.ink2 : th.border;
    ctx.lineWidth = pc === sub ? 1.25 : 1;
    ctx.stroke();
    ctx.fillStyle = th.ink2;
    ctx.font = `600 ${L.big ? 14 : 11}px ${FONT}`;
    ctx.textBaseline = "middle";
    ctx.fillText(pc, x + L.pad, y + L.head / 2 + 2);
    ctx.textAlign = "right";
    ctx.fillStyle = th.muted;
    ctx.font = `400 ${L.big ? 13 : 10}px ${FONT}`;
    const roles = pc === sub ? (s === "pipedpeer" ? ["submitter"] : ["head · submitter", "head"]) : inRun ? [`${f1(busy)} busy`] : ["not in run", "idle"];
    const room = L.tileW - L.pad * 2 - ctx.measureText(pc).width - 10;
    const role = roles.find((r) => ctx.measureText(r).width <= room);
    if (role) ctx.fillText(role, x + L.tileW - L.pad, y + L.head / 2 + 2);
    ctx.textAlign = "left";
    const n = cores(pc);
    for (let i = 0; i < n; i++) {
      const cx = x + L.pad + (i % L.cc) * (L.cell + 3), cy = y + L.head + Math.floor(i / L.cc) * (L.cell + 3);
      rr(cx, cy, L.cell, L.cell, 3);
      ctx.fillStyle = th.cell;
      ctx.fill();
      const f = Math.max(0, Math.min(1, busy - i));
      if (f > 0.02) {
        ctx.globalAlpha = (inRun ? 1 : 0.38) * (0.35 + 0.65 * f);
        rr(cx, cy, L.cell, L.cell, 3);
        ctx.fillStyle = col;
        ctx.fill();
        ctx.globalAlpha = inRun ? 1 : 0.38;
      }
    }
    ctx.globalAlpha = 1;
  }

  function centre(L, pc) {
    const p = L.tiles[pc];
    return [p.x + L.tileW / 2, p.y + L.tileH / 2];
  }

  function drawFlights(s, d, th, L, col) {
    const [sx, sy] = centre(L, sub);
    for (const f of flights[s]) {
      if (t < f.ts || t > f.ts + f.dur || !L.tiles[f.pc]) continue;
      const [tx, ty] = centre(L, f.pc);
      const p = (t - f.ts) / f.dur;
      const [ax, ay, bx, by] = f.out ? [sx, sy, tx, ty] : [tx, ty, sx, sy];
      const mx = (ax + bx) / 2, my = (ay + by) / 2 - Math.max(18, Math.hypot(bx - ax, by - ay) * 0.22);
      for (let j = 0; j < f.k; j++) {
        const q = Math.max(0, Math.min(1, p - j * 0.06));
        if (q <= 0 || q >= 1) continue;
        const e = d3.easeCubicInOut(q);
        const px = (1 - e) * (1 - e) * ax + 2 * (1 - e) * e * mx + e * e * bx;
        const py = (1 - e) * (1 - e) * ay + 2 * (1 - e) * e * my + e * e * by;
        ctx.beginPath();
        ctx.arc(px, py, f.out ? (L.big ? 4 : 3) : (L.big ? 3 : 2.2), 0, 7);
        ctx.fillStyle = f.out ? col : th.ink2;
        ctx.globalAlpha = f.out ? 0.95 : 0.7;
        ctx.fill();
        ctx.globalAlpha = 1;
      }
    }
  }

  function drawSpark(s, th, L, col) {
    const y0 = L.sparkY, h = L.spark - 18, w = L.colW;
    const x = d3.scaleLinear([T0, T1], [0, w]), y = d3.scaleLinear([0, peak], [y0 + h, y0]);
    ctx.strokeStyle = th.grid;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, y0 + h + 0.5); ctx.lineTo(w, y0 + h + 0.5); ctx.stroke();
    const pts = totals[s];
    const area = d3.area().x((p) => x(p[0])).y0(y0 + h).y1((p) => y(p[1])).curve(d3.curveMonotoneX).context(ctx);
    ctx.beginPath(); area(pts); ctx.fillStyle = col; ctx.globalAlpha = 0.07; ctx.fill(); ctx.globalAlpha = 1;
    const shown = pts.filter((p) => p[0] <= t);
    ctx.beginPath(); d3.line().x((p) => x(p[0])).y((p) => y(p[1])).curve(d3.curveMonotoneX).context(ctx)(shown);
    ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.lineJoin = "round"; ctx.stroke();
    const wx = x(H.systems[s].wall || 0);
    ctx.fillStyle = th.muted; ctx.font = `400 ${L.big ? 13 : 10.5}px ${FONT}`; ctx.textBaseline = "alphabetic";
    ctx.fillText("busy cores, all PCs", 0, y0 - 6);
    ctx.textAlign = wx > w - 60 ? "right" : "left";
    ctx.fillText(`end ${fmtS(H.systems[s].wall)}`, wx + (wx > w - 60 ? -4 : 4), y0 + h + 13);
    ctx.textAlign = "left";
    ctx.strokeStyle = th.axis; ctx.beginPath(); ctx.moveTo(wx, y0 + 2); ctx.lineTo(wx, y0 + h); ctx.stroke();
    const px = x(Math.max(T0, Math.min(T1, t)));
    ctx.strokeStyle = th.ink; ctx.globalAlpha = 0.5; ctx.beginPath(); ctx.moveTo(px, y0); ctx.lineTo(px, y0 + h); ctx.stroke(); ctx.globalAlpha = 1;
  }

  function frame(now) {
    if (!playing) return;
    if (last != null) t += ((now - last) / 1000) * speed;
    last = now;
    if (t > T1 + 1.2) t = T0;
    draw();
    raf = requestAnimationFrame(frame);
  }
  function play() { if (!H) return; if (t >= T1) t = T0; playing = true; last = null; setPlayIcon(); raf = requestAnimationFrame(frame); }
  function pause() { playing = false; cancelAnimationFrame(raf); setPlayIcon(); }
  function setPlayIcon() { if (ui.play) { ui.play.textContent = playing ? "❚❚" : "▶"; ui.play.setAttribute("aria-label", playing ? "Pause" : "Play"); } }
  function seek(v) { pause(); t = v; draw(); }

  return { mount, play, pause, seek, draw, redraw: () => canvas && size(), range: [T0, T1], ok: !!H };
})();
