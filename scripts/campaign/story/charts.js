// SVG primitives every chart shares: a frame sized to its container (axis
// band included), hairline axes and grid, markers with a surface ring, and
// small annotation helpers. Every colour is written as an attribute from the
// theme, never a CSS class, so an exported SVG is self-contained.
function frame(target, { height = 320, margin = {}, minWidth = 280, name } = {}) {
  const m = Object.assign({ top: 16, right: 24, bottom: 36, left: 48 }, margin);
  const width = Math.max(minWidth, target.clientWidth || 640);
  target.replaceChildren();
  const svg = d3.select(target).append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`).attr("width", width).attr("height", height)
    .attr("font-family", FONT).attr("font-size", 12).attr("fill", T().ink2)
    .attr("data-export", name || null).attr("role", "img");
  svg.append("rect").attr("class", "bg").attr("width", width).attr("height", height).attr("fill", T().surface).attr("opacity", 0);
  const g = svg.append("g").attr("transform", `translate(${m.left},${m.top})`);
  return { svg, g, w: width - m.left - m.right, h: height - m.top - m.bottom, m, width, height };
}

function styleAxis(sel, { grid = 0, domain = true } = {}) {
  sel.attr("font-family", FONT).attr("font-size", 11);
  sel.selectAll(".domain").attr("fill", "none").attr("stroke", T().axis).attr("stroke-width", 1).attr("opacity", domain ? 1 : 0);
  sel.selectAll(".tick line").attr("stroke", grid ? T().grid : T().axis).attr("stroke-width", 1);
  sel.selectAll(".tick text").attr("fill", T().muted).style("font-variant-numeric", "tabular-nums");
  return sel;
}
function axisBottom(f, x, opts = {}) {
  const a = d3.axisBottom(x).tickSizeOuter(0).ticks(opts.ticks || 6);
  if (opts.values) a.tickValues(opts.values);
  if (opts.format) a.tickFormat(opts.format);
  a.tickSizeInner(opts.grid ? -f.h : 4).tickPadding(opts.grid ? 8 : 6);
  const s = f.g.append("g").attr("transform", `translate(0,${f.h})`).call(a);
  styleAxis(s, { grid: opts.grid });
  if (opts.grid) s.select(".domain").attr("stroke", T().axis);
  if (opts.label) f.g.append("text").attr("x", f.w).attr("y", f.h + 32).attr("text-anchor", "end")
    .attr("fill", T().muted).attr("font-size", 11).text(opts.label);
  return s;
}
function axisLeft(f, y, opts = {}) {
  const a = d3.axisLeft(y).ticks(opts.ticks || 5).tickSizeInner(-f.w).tickSizeOuter(0).tickPadding(8);
  if (opts.format) a.tickFormat(opts.format);
  if (opts.values) a.tickValues(opts.values);
  const s = f.g.append("g").call(a);
  styleAxis(s, { grid: 1, domain: false });
  if (opts.label) f.g.append("text").attr("x", -f.m.left + 2).attr("y", -6).attr("fill", T().muted)
    .attr("font-size", 11).text(opts.label);
  return s;
}

// A marker: the system's shape, filled (plain is hollow), 2px surface ring.
function marker(g, s, x, y, size = 64) {
  const hollow = fam(s) === "plain";
  return g.append("path").attr("transform", `translate(${x},${y})`).attr("d", d3.symbol(SHAPE[fam(s)], size)())
    .attr("fill", hollow ? T().surface : C(s)).attr("stroke", hollow ? C(s) : T().surface)
    .attr("stroke-width", hollow ? 2 : 2).attr("paint-order", "stroke");
}

// A transparent hit target around a mark: at least 24px.
function hit(g, x, y, r, onMove) {
  return g.append("circle").attr("cx", x).attr("cy", y).attr("r", Math.max(12, r)).attr("fill", "transparent")
    .style("cursor", "default").on("pointermove", onMove).on("pointerleave", () => tip.hide());
}

function text(g, x, y, s, opts = {}) {
  return g.append("text").attr("x", x).attr("y", y).attr("fill", opts.fill || T().ink2)
    .attr("font-size", opts.size || 12).attr("font-weight", opts.weight || null)
    .attr("text-anchor", opts.anchor || "start").attr("dominant-baseline", opts.baseline || null).text(s);
}

// Vertical event rule with a label on top.
function rule(g, x, h, label, opts = {}) {
  g.append("line").attr("x1", x).attr("x2", x).attr("y1", opts.y0 || 0).attr("y2", h)
    .attr("stroke", opts.stroke || T().ink2).attr("stroke-width", 1).attr("opacity", opts.opacity || 0.7);
  if (label) text(g, x + 4, (opts.y0 || 0) + 11, label, { size: 11, fill: opts.fill || T().ink2, weight: 600 });
}

// A card: kicker title, the chart, a legend, a caption, the table.
function card(parent, { title, sub, cls } = {}) {
  const c = el("figure", { class: "card " + (cls || "") });
  if (title) c.append(el("figcaption", { class: "card-head" }, el("div", { class: "card-title", text: title }),
    sub ? el("div", { class: "card-sub", text: sub }) : null));
  const body = el("div", { class: "card-body" });
  c.append(body);
  parent.append(c);
  return { card: c, body };
}

// A big number with its label and the emulated stamp.
function figure(value, label, opts = {}) {
  return el("div", { class: "fig " + (opts.cls || "") },
    el("div", { class: "fig-value" }, opts.mark ? el("span", { class: "fig-mark", style: `background:var(--c-${fam(opts.mark)})` }) : null, value),
    el("div", { class: "fig-label", text: label }),
    STAMP && !opts.noStamp ? el("div", { class: "stamp", text: STAMP }) : null);
}

// Status badge: icon + label, colour never alone.
function badge(ok, label) {
  return el("span", { class: "badge " + (ok ? "ok" : "bad") }, el("span", { class: "badge-icon", text: ok ? "✓" : "✕" }), label);
}

// Area under a stepped series of bins: [[t, v]] -> path.
function stepArea(x, y, pts, y0 = 0) {
  return d3.area().curve(d3.curveStepAfter).x((d) => x(d[0])).y0(() => y(y0)).y1((d) => y(d[1]))(pts);
}
