import { useMemo } from "react";
import { downsample, gapPath, useWidth } from "./scale";

export function Sparkline({ times, values, color = "var(--s1)", height = 28, domain = [0, 100], label }: { times: number[]; values: (number | null)[]; color?: string; height?: number; domain?: [number, number]; label: string }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const path = useMemo(() => {
    if (!width || !times.length) return null;
    const s = downsample(times, values, Math.max(20, Math.floor(width / 2)));
    const t0 = s.times[0]!;
    const t1 = Math.max(s.times[s.times.length - 1]!, t0 + 1);
    const [lo, hi] = domain;
    const pad = 2;
    return gapPath(
      s.times.map((t) => ((t - t0) / (t1 - t0)) * width),
      s.values.map((v) => (v === null ? null : pad + (1 - Math.min(1, Math.max(0, (v - lo) / (hi - lo)))) * (height - pad * 2))),
    );
  }, [times, values, width, height, domain]);
  return (
    <div ref={ref} style={{ height }} role="img" aria-label={label}>
      {path && (
        <svg width={width} height={height} className="block">
          <line x1={0} x2={width} y1={height - 0.5} y2={height - 0.5} stroke="var(--grid)" strokeWidth={1} />
          <path d={path.d} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
          {path.dots.map(([cx, cy], i) => (
            <circle key={i} cx={cx} cy={cy} r={1.5} fill={color} />
          ))}
        </svg>
      )}
    </div>
  );
}
