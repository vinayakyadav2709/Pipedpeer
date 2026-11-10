import { ChartLine, Table2 } from "lucide-react";
import { useState, type CSSProperties, type ReactNode } from "react";
import { cn } from "@/ui/cn";
import { IconButton } from "@/ui/controls";

export interface LegendItem {
  label: string;
  color: string;
  shape?: "line" | "rect" | "dot";
  muted?: boolean;
}

export interface TableView {
  head: string[];
  rows: (string | number)[][];
}

export function Legend({ items, className }: { items: LegendItem[]; className?: string }) {
  return (
    <ul className={cn("flex flex-wrap gap-x-3.5 gap-y-1 text-xs text-fg-2", className)}>
      {items.map((it) => (
        <li key={it.label} className={cn("inline-flex items-center gap-1.5", it.muted && "text-fg-3")}>
          <span
            aria-hidden
            className={cn("inline-block shrink-0", it.shape === "line" ? "h-0.5 w-3 rounded-full" : it.shape === "dot" ? "size-2 rounded-full" : "size-2 rounded-[2px]")}
            style={{ background: it.color }}
          />
          {it.label}
        </li>
      ))}
    </ul>
  );
}

export function DataTable({ table, className }: { table: TableView; className?: string }) {
  return (
    <div className={cn("max-h-64 overflow-auto", className)}>
      <table className="num w-full border-collapse text-xs">
        <thead className="sticky top-0 bg-surface">
          <tr>
            {table.head.map((h, i) => (
              <th key={h} scope="col" className={cn("border-b border-line px-2 py-1.5 font-medium text-fg-3", i === 0 ? "text-left" : "text-right")}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((r, i) => (
            <tr key={i} className="border-b border-line last:border-0">
              {r.map((c, j) => (
                <td key={j} className={cn("px-2 py-1", j === 0 ? "text-left text-fg-2" : "text-right")}>
                  {c}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** A chart with its title, legend and table-view twin. */
export function ChartFrame({ title, hint, legend, table, children, className, footer }: { title: string; hint?: ReactNode; legend?: LegendItem[]; table?: TableView; children: ReactNode; className?: string; footer?: ReactNode }) {
  const [asTable, setAsTable] = useState(false);
  return (
    <figure className={cn("flex min-w-0 flex-col", className)}>
      <figcaption className="flex items-start justify-between gap-2 pb-2">
        <div className="min-w-0">
          <h3 className="text-sm font-medium">{title}</h3>
          {hint && <p className="text-xs text-fg-3">{hint}</p>}
        </div>
        {table && (
          <IconButton size="sm" label={asTable ? `Show ${title} as a chart` : `Show ${title} as a table`} onClick={() => setAsTable((v) => !v)} className="-mt-1 -mr-1">
            {asTable ? <ChartLine className="size-3.5" aria-hidden /> : <Table2 className="size-3.5" aria-hidden />}
          </IconButton>
        )}
      </figcaption>
      {legend && legend.length > 1 && !asTable && <Legend items={legend} className="pb-2" />}
      {asTable && table ? <DataTable table={table} /> : children}
      {!asTable && footer}
    </figure>
  );
}

export interface TipRow {
  label: string;
  value: string;
  color?: string;
}

export function ChartTip({ x, y, width, title, rows, note }: { x: number; y: number; width: number; title: string; rows: TipRow[]; note?: string }) {
  const flip = x > width / 2;
  const style: CSSProperties = { top: Math.max(0, y), ...(flip ? { right: width - x + 10 } : { left: x + 10 }) };
  return (
    <div role="status" className="pointer-events-none absolute z-10 max-w-64 min-w-28 rounded-md border border-line-strong bg-raised px-2 py-1.5 text-xs shadow-pop" style={style}>
      <div className="num pb-0.5 text-fg-3">{title}</div>
      {rows.map((r) => (
        <div key={r.label} className="flex items-center gap-1.5">
          {r.color && <span aria-hidden className="inline-block h-0.5 w-2.5 shrink-0 rounded-full" style={{ background: r.color }} />}
          <span className="num font-semibold text-fg">{r.value}</span>
          <span className="truncate text-fg-2">{r.label}</span>
        </div>
      ))}
      {note && <div className="pt-0.5 text-fg-3">{note}</div>}
    </div>
  );
}

export function ChartEmpty({ height, children }: { height: number; children: ReactNode }) {
  return (
    <div className="flex items-center justify-center rounded-md border border-dashed border-line px-4 text-center text-xs text-fg-3" style={{ height }}>
      <span className="max-w-[44ch]">{children}</span>
    </div>
  );
}
