import { useState } from "react";
import { day, time } from "@/lib/format";
import { ChartTip } from "./frame";
import { niceTicks, useWidth } from "./scale";

export interface ColumnPart {
  label: string;
  color: string;
}

export interface ColumnsProps {
  buckets: { t: number; values: number[] }[];
  parts: ColumnPart[];
  bucketMs: number;
  height?: number;
  ariaLabel: string;
}

const M = { top: 6, right: 8, bottom: 20, left: 34 };
const GAP = 2;

export const bucketLabel = (t: number, bucketMs: number) => (bucketMs >= 86_400_000 ? day(t) : bucketMs >= 3_600_000 * 6 ? `${day(t)} ${time(t)}` : time(t));

function topRounded(x: number, y: number, w: number, h: number, r: number) {
  const rr = Math.min(r, h, w / 2);
  return `M${x},${y + h}V${y + rr}Q${x},${y} ${x + rr},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + h}Z`;
}

export function Columns({ buckets, parts, bucketMs, height = 168, ariaLabel }: ColumnsProps) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const innerW = Math.max(0, width - M.left - M.right);
  const innerH = height - M.top - M.bottom;
  const max = Math.max(1, ...buckets.map((b) => b.values.reduce((a, v) => a + v, 0)));
  const ticks = niceTicks(0, max, 3).filter((t) => Number.isInteger(t));
  const top = Math.max(max, ticks[ticks.length - 1] ?? 1);
  const slot = buckets.length ? innerW / buckets.length : 0;
  const bar = Math.max(1, Math.min(24, slot - GAP));
  const y = (v: number) => M.top + innerH - (v / top) * innerH;
  const labelEvery = Math.max(1, Math.ceil(buckets.length / Math.max(1, Math.floor(innerW / 72))));
  const hb = hover !== null ? buckets[hover] : undefined;

  return (
    <div ref={ref} className="relative" style={{ height }}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={ariaLabel} className="block select-none" onPointerLeave={() => setHover(null)}>
          {ticks.map((t) => (
            <g key={t}>
              <line x1={M.left} x2={width - M.right} y1={y(t)} y2={y(t)} stroke={t === 0 ? "var(--axis)" : "var(--grid)"} strokeWidth={1} shapeRendering="crispEdges" />
              <text x={M.left - 6} y={y(t)} dy="0.32em" textAnchor="end" className="num fill-fg-3 text-[10px]">
                {t}
              </text>
            </g>
          ))}
          {buckets.map((b, i) => {
            const cx = M.left + slot * i + slot / 2;
            const x = cx - bar / 2;
            let acc = 0;
            const lastPart = b.values.reduce((li, v, pi) => (v > 0 ? pi : li), -1);
            return (
              <g key={b.t} opacity={hover === null || hover === i ? 1 : 0.55}>
                {b.values.map((v, pi) => {
                  if (v <= 0) return null;
                  const y1 = y(acc);
                  acc += v;
                  const y0 = y(acc);
                  const h = Math.max(1, y1 - y0 - (pi === lastPart ? 0 : GAP));
                  const yy = y1 - h;
                  return pi === lastPart ? <path key={pi} d={topRounded(x, yy, bar, h, 3)} fill={parts[pi]!.color} /> : <rect key={pi} x={x} y={yy} width={bar} height={h} fill={parts[pi]!.color} />;
                })}
                {i % labelEvery === 0 && (
                  <text x={Math.min(cx, width - 1)} y={height - 5} textAnchor={cx > width - 24 ? "end" : "middle"} className="num fill-fg-3 text-[10px]">
                    {bucketLabel(b.t, bucketMs)}
                  </text>
                )}
                <rect x={M.left + slot * i} y={M.top} width={slot} height={innerH} fill="transparent" onPointerEnter={() => setHover(i)} onPointerDown={() => setHover(i)} />
              </g>
            );
          })}
        </svg>
      )}
      {hb && hover !== null && (
        <ChartTip
          x={M.left + slot * hover + slot / 2}
          y={M.top}
          width={width}
          title={bucketLabel(hb.t, bucketMs)}
          rows={parts.map((p, pi) => ({ label: p.label, color: p.color, value: String(hb.values[pi] ?? 0) }))}
        />
      )}
    </div>
  );
}
