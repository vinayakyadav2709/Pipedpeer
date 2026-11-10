import { useEffect, useMemo, useState, type PointerEvent } from "react";
import { time, timeSec } from "@/lib/format";
import { ChartTip, type TipRow } from "./frame";
import { downsample, gapPath, niceTicks, timeTicks, useWidth } from "./scale";

export interface LineSeries {
  id: string;
  label: string;
  color: string;
  values: (number | null)[];
  width?: number;
}

export interface TimeLinesProps {
  times: number[];
  series: LineSeries[];
  height?: number;
  /** Fixed y domain; omitted means 0 to a rounded maximum. */
  domain?: [number, number];
  log?: boolean;
  format: (v: number) => string;
  hoverT: number | null;
  onHover: (t: number | null) => void;
  ariaLabel: string;
  axisFormat?: (v: number) => string;
  /** Extends the x domain past the data (a window that is longer than the history so far). */
  from?: number;
  to?: number;
  /** Candidate tick values for a log axis; the axis spans the candidates that bracket the data. */
  logTicks?: number[];
  /** Draw a dot at every reading (sparse series where a bare line would hide single points). */
  markers?: boolean;
  xFormat?: (t: number) => string;
  tipTitle?: (t: number) => string;
}

const M = { top: 6, right: 8, bottom: 20 };
const MIN_LEFT = 34;

export function TimeLines({ times, series, height = 132, domain, log, format, axisFormat, hoverT, onHover, ariaLabel, from, to, logTicks, markers, xFormat = time, tipTitle = timeSec }: TimeLinesProps) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const fmtAxis = axisFormat ?? format;
  const [left, setLeft] = useState(MIN_LEFT);
  const innerW = Math.max(0, width - left - M.right);
  const innerH = height - M.top - M.bottom;
  const t0 = from ?? times[0] ?? 0;
  const t1 = Math.max(to ?? 0, times[times.length - 1] ?? 1, t0 + 1);

  const model = useMemo(() => {
    const maxPoints = Math.max(40, Math.floor(innerW / 1.5));
    const sampled = series.map((s) => ({ ...s, ...downsample(times, s.values, maxPoints) }));
    let lo = Infinity;
    let hi = -Infinity;
    for (const s of sampled)
      for (const v of s.values)
        if (v !== null) {
          if (v < lo) lo = v;
          if (v > hi) hi = v;
        }
    if (!Number.isFinite(lo)) (lo = 0), (hi = 1);
    let y0: number;
    let y1: number;
    let ticks: number[];
    if (log) {
      const cands = logTicks ?? Array.from({ length: 12 }, (_, i) => 10 ** i);
      const a = Math.max(0, cands.findLastIndex((c) => c <= lo));
      const b = cands.findIndex((c) => c >= hi);
      ticks = cands.slice(a, (b < 0 ? cands.length - 1 : Math.max(b, a + 1)) + 1);
      y0 = Math.log10(ticks[0]!);
      y1 = Math.log10(ticks[ticks.length - 1]!);
    } else if (domain) {
      [y0, y1] = domain;
      ticks = niceTicks(y0, y1, 4);
    } else {
      ticks = niceTicks(0, hi || 1, 4);
      y0 = 0;
      y1 = Math.max(ticks[ticks.length - 1] ?? 1, hi);
    }
    const x = (t: number) => left + ((t - t0) / (t1 - t0)) * innerW;
    const y = (v: number) => {
      const u = log ? (Math.log10(Math.max(v, 10 ** y0)) - y0) / (y1 - y0) : (v - y0) / (y1 - y0);
      return M.top + innerH - Math.min(1, Math.max(0, u)) * innerH;
    };
    const paths = sampled.map((s) => {
      const xs = s.times.map(x);
      const ys = s.values.map((v) => (v === null ? null : y(v)));
      const path = gapPath(xs, ys);
      return { ...s, d: path.d, dots: markers ? xs.flatMap((px, i): [number, number][] => (ys[i] === null || ys[i] === undefined ? [] : [[px, ys[i]!]])) : path.dots };
    });
    return { x, y, ticks, paths };
  }, [times, series, innerW, innerH, domain, log, logTicks, markers, t0, t1, left]);

  const widest = Math.max(...model.ticks.map((t) => fmtAxis(t).length));
  useEffect(() => setLeft(Math.max(MIN_LEFT, Math.ceil(widest * 5.8) + 12)), [widest]);

  const hoverIndex = useMemo(() => {
    if (hoverT === null || !times.length) return -1;
    let lo = 0;
    let hi = times.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (times[mid]! < hoverT) lo = mid + 1;
      else hi = mid;
    }
    if (lo > 0 && Math.abs(times[lo - 1]! - hoverT) < Math.abs(times[lo]! - hoverT)) lo--;
    return lo;
  }, [hoverT, times]);

  const move = (e: PointerEvent<SVGSVGElement>) => {
    if (!innerW) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const u = (e.clientX - rect.left - left) / innerW;
    onHover(t0 + Math.min(1, Math.max(0, u)) * (t1 - t0));
  };

  const hx = hoverIndex >= 0 ? model.x(times[hoverIndex]!) : null;
  const tipRows: TipRow[] =
    hoverIndex >= 0
      ? series
          .map((s) => ({ label: s.label, color: s.color, v: s.values[hoverIndex] ?? null }))
          .sort((a, b) => (b.v ?? -1) - (a.v ?? -1))
          .map((r) => ({ label: r.label, color: r.color, value: r.v === null ? "no reading" : format(r.v) }))
      : [];
  const xTicks = timeTicks(t0, t1, Math.max(2, Math.floor(innerW / 64)));

  return (
    <div ref={ref} className="relative" style={{ height }}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={ariaLabel} className="block touch-none select-none" onPointerMove={move} onPointerDown={move} onPointerLeave={() => onHover(null)}>
          {model.ticks.map((t) => (
            <g key={t}>
              <line x1={left} x2={width - M.right} y1={model.y(t)} y2={model.y(t)} stroke={t === model.ticks[0] ? "var(--axis)" : "var(--grid)"} strokeWidth={1} shapeRendering="crispEdges" />
              <text x={left - 6} y={model.y(t)} dy="0.32em" textAnchor="end" className="num fill-fg-3 text-[10px]">
                {fmtAxis(t)}
              </text>
            </g>
          ))}
          {xTicks.map((t) => (
            <text key={t} x={Math.min(model.x(t), width - 1)} y={height - 5} textAnchor={model.x(t) > width - 20 ? "end" : "middle"} className="num fill-fg-3 text-[10px]">
              {xFormat(t)}
            </text>
          ))}
          {model.paths.map((p) => (
            <g key={p.id}>
              <path d={p.d} fill="none" stroke={p.color} strokeWidth={p.width ?? 1.75} strokeLinejoin="round" strokeLinecap="round" />
              {p.dots.map(([cx, cy], i) => (
                <circle key={i} cx={cx} cy={cy} r={markers ? 3 : 1.75} fill={p.color} stroke={markers ? "var(--surface)" : undefined} strokeWidth={markers ? 1.5 : undefined} />
              ))}
            </g>
          ))}
          {hx !== null && (
            <g>
              <line x1={hx} x2={hx} y1={M.top} y2={M.top + innerH} stroke="var(--fg-3)" strokeWidth={1} shapeRendering="crispEdges" />
              {series.map((s) => {
                const v = s.values[hoverIndex];
                return v === null || v === undefined ? null : <circle key={s.id} cx={hx} cy={model.y(v)} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />;
              })}
            </g>
          )}
        </svg>
      )}
      {hx !== null && tipRows.length > 0 && <ChartTip x={hx} y={M.top} width={width} title={tipTitle(times[hoverIndex]!)} rows={tipRows} />}
    </div>
  );
}
