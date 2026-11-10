import { cn } from "@/ui/cn";

export interface BarRow {
  key: string;
  label: string;
  /** Printed at the bar's end; the bar never carries a value the row does not show. */
  display: string;
  segments: { value: number; color: string; label: string }[];
  note?: string;
}

/** Ranked horizontal bars on one baseline. Values are direct-labelled, so no tooltip is needed to read them. */
export function Bars({ rows, labelWidth = "7.5rem", className }: { rows: BarRow[]; labelWidth?: string; className?: string }) {
  const max = Math.max(1e-9, ...rows.map((r) => r.segments.reduce((a, s) => a + s.value, 0)));
  return (
    <ul className={cn("flex flex-col gap-2", className)}>
      {rows.map((r) => {
        const total = r.segments.reduce((a, s) => a + s.value, 0);
        const title = r.segments
          .filter((s) => s.value > 0)
          .map((s) => `${s.label}: ${s.value.toLocaleString()}`)
          .join(", ");
        return (
          <li key={r.key} className="grid items-center gap-x-3 text-xs" style={{ gridTemplateColumns: `${labelWidth} minmax(0,1fr) auto` }} title={r.segments.length > 1 ? title : undefined}>
            <span className="truncate text-fg-2" title={r.label}>
              {r.label}
            </span>
            <span className="flex h-2 items-stretch gap-0.5">
              {r.segments.map((s, i) => {
                const last = r.segments.slice(i + 1).every((n) => n.value <= 0);
                return s.value > 0 ? <span key={s.label} className={cn("block min-w-0.5", last && "rounded-r-[3px]")} style={{ width: `${(s.value / max) * 100}%`, background: s.color }} /> : null;
              })}
              {total <= 0 && <span className="block w-px bg-line-strong" />}
            </span>
            <span className="num text-right text-fg">
              {r.display}
              {r.note && <span className="ml-1.5 text-fg-3">{r.note}</span>}
            </span>
          </li>
        );
      })}
    </ul>
  );
}
