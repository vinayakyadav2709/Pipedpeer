const UNITS = ["B", "KB", "MB", "GB", "TB"] as const;

export function bytes(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined || !Number.isFinite(n) || n <= 0) return "0 B";
  const i = Math.min(UNITS.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  const v = n / 1024 ** i;
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(digits)} ${UNITS[i]}`;
}

export function gb(n: number): string {
  const v = n / 1024 ** 3;
  return v >= 100 ? String(Math.round(v)) : v.toFixed(1);
}

export function duration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) return "–";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 10) return `${s.toFixed(1)} s`;
  if (s < 60) return `${Math.round(s)} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(Math.round(s % 60)).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function clock(ms: number): string {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}:${String(s % 60).padStart(2, "0")}` : `${(ms / 1000).toFixed(1)}s`;
}

export function percent(v: number, digits = 0): string {
  return `${v.toFixed(digits)}%`;
}

export function ago(t: number, now = Date.now()): string {
  if (!t) return "–";
  const d = now - t;
  if (d < 5000) return "just now";
  if (d < 60_000) return `${Math.floor(d / 1000)}s ago`;
  if (d < 3_600_000) return `${Math.floor(d / 60_000)}m ago`;
  if (d < 86_400_000) return `${Math.floor(d / 3_600_000)}h ago`;
  if (d < 30 * 86_400_000) return `${Math.floor(d / 86_400_000)}d ago`;
  return new Date(t).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

const timeFmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
const timeSecFmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
const dayFmt = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });
const fullFmt = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });

export const time = (t: number) => timeFmt.format(t);
export const timeSec = (t: number) => timeSecFmt.format(t);
export const day = (t: number) => dayFmt.format(t);
export const dateTime = (t: number) => (t ? fullFmt.format(t) : "–");

export function compact(n: number): string {
  if (Math.abs(n) < 10_000) return n.toLocaleString();
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(n);
}

export function plural(n: number, one: string, many = one + "s") {
  return `${n.toLocaleString()} ${n === 1 ? one : many}`;
}

/** Axis label for a round duration (log-scale ticks). */
export function durationTick(ms: number): string {
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${ms / 1000} s`;
  if (ms < 3_600_000) return `${ms / 60_000} min`;
  if (ms < 86_400_000) return `${ms / 3_600_000} h`;
  return `${ms / 86_400_000} d`;
}

export const DURATION_TICKS = [100, 1000, 10_000, 60_000, 600_000, 3_600_000, 6 * 3_600_000, 86_400_000];

/** "hour", "6 hours", "5 minutes": the noun for a bucket width. */
export function bucketNoun(ms: number): string {
  if (ms === 86_400_000) return "day";
  if (ms === 3_600_000) return "hour";
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000} hours`;
  return `${Math.round(ms / 60_000)} minutes`;
}

/** Keeps the end of a long path, which is the part that identifies a script. */
export function shortPath(p: string, keep = 2): string {
  const parts = p.split("/").filter(Boolean);
  return parts.length > keep + 1 ? `…/${parts.slice(-keep).join("/")}` : p;
}
