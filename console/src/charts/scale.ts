import { useLayoutEffect, useRef, useState } from "react";

export const SERIES = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)", "var(--s5)", "var(--s6)", "var(--s7)", "var(--s8)"] as const;
export const MUTED_SERIES = "var(--s-mute)";

/** Colour follows the entity: slots are assigned by sorted id so a filter never repaints survivors. */
export function seriesColors(ids: string[]): Map<string, string> {
  const sorted = [...new Set(ids)].sort();
  return new Map(sorted.map((id, i) => [id, SERIES[i] ?? MUTED_SERIES]));
}

export function niceTicks(min: number, max: number, count = 4): number[] {
  if (max <= min) return [min];
  const raw = (max - min) / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const norm = raw / mag;
  const step = (norm < 1.5 ? 1 : norm < 3 ? 2 : norm < 7 ? 5 : 10) * mag;
  const ticks: number[] = [];
  for (let v = Math.ceil(min / step) * step; v <= max + step * 1e-9; v += step) ticks.push(Math.round(v / step) * step);
  return ticks;
}

const TIME_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 180, 360, 720, 1440, 2880, 10080].map((m) => m * 60_000);

export function timeTicks(from: number, to: number, maxCount: number): number[] {
  const span = to - from;
  const step = TIME_STEPS.find((s) => span / s <= maxCount) ?? TIME_STEPS[TIME_STEPS.length - 1]!;
  const offset = new Date(from).getTimezoneOffset() * 60_000;
  const ticks: number[] = [];
  for (let t = Math.ceil((from - offset) / step) * step + offset; t <= to; t += step) ticks.push(t);
  return ticks;
}

export function useWidth<T extends HTMLElement>(): [React.RefObject<T | null>, number] {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    setWidth(el.clientWidth);
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w !== undefined) setWidth(Math.round(w));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width];
}

/** Mean-buckets a series to at most `maxPoints`, keeping a bucket null only when every reading in it is null. */
export function downsample(times: number[], values: (number | null)[], maxPoints: number): { times: number[]; values: (number | null)[] } {
  if (times.length <= maxPoints || maxPoints < 2) return { times, values };
  const size = Math.ceil(times.length / maxPoints);
  const outT: number[] = [];
  const outV: (number | null)[] = [];
  for (let i = 0; i < times.length; i += size) {
    let sum = 0;
    let n = 0;
    const end = Math.min(times.length, i + size);
    for (let j = i; j < end; j++) {
      const v = values[j];
      if (v !== null && v !== undefined) {
        sum += v;
        n++;
      }
    }
    outT.push(times[end - 1]!);
    outV.push(n ? sum / n : null);
  }
  return { times: outT, values: outV };
}

/** SVG path that breaks at nulls, plus the isolated points a line cannot show. */
export function gapPath(xs: number[], ys: (number | null)[]): { d: string; dots: [number, number][] } {
  let d = "";
  const dots: [number, number][] = [];
  let run = 0;
  for (let i = 0; i < xs.length; i++) {
    const y = ys[i];
    if (y === null || y === undefined) {
      if (run === 1) dots.push([xs[i - 1]!, ys[i - 1]!]);
      run = 0;
      continue;
    }
    d += `${run === 0 ? "M" : "L"}${xs[i]!.toFixed(1)},${y.toFixed(1)}`;
    run++;
  }
  if (run === 1) dots.push([xs[xs.length - 1]!, ys[ys.length - 1]!]);
  return { d, dots };
}
