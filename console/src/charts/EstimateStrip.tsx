import { Check, MoveRight, TriangleAlert } from "lucide-react";
import { useState, type MouseEvent } from "react";
import { bytes } from "@/lib/format";
import type { Analytics } from "@/lib/types";
import { ChartTip, type TableView } from "./frame";
import { useWidth } from "./scale";

type Estimate = Analytics["mem_estimates"][number];

const TIER_ORDER = ["user", "historical", "filesize", "import"];
const TIER_HINT: Record<string, string> = {
  user: "declared by the user",
  historical: "learned from earlier runs",
  filesize: "guessed from input size",
  import: "guessed from imports",
};

/** Estimates within this factor of the real peak count as on target. */
const TOLERANCE = 1.25;
const ROW_H = 34;

export interface TierSummary {
  tier: string;
  items: (Estimate & { ratio: number })[];
  median: number;
  under: number;
  over: number;
}

export function summariseEstimates(rows: Estimate[]): TierSummary[] {
  const byTier = new Map<string, (Estimate & { ratio: number })[]>();
  for (const r of rows) {
    if (r.estimated_bytes <= 0 || r.peak_bytes <= 0) continue;
    const tier = r.tier || "unknown";
    byTier.set(tier, [...(byTier.get(tier) ?? []), { ...r, ratio: r.estimated_bytes / r.peak_bytes }]);
  }
  return [...byTier]
    .sort(([a], [b]) => (TIER_ORDER.indexOf(a) + 99) % 99 - (TIER_ORDER.indexOf(b) + 99) % 99 || a.localeCompare(b))
    .map(([tier, items]) => {
      const sorted = items.map((i) => i.ratio).sort((a, b) => a - b);
      const mid = sorted.length >> 1;
      const median = sorted.length % 2 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
      return {
        tier,
        items,
        median,
        under: items.filter((i) => i.ratio < 1 / TOLERANCE).length,
        over: items.filter((i) => i.ratio > TOLERANCE).length,
      };
    });
}

const times = (r: number) => `${r >= 10 ? r.toFixed(0) : r.toFixed(1)}×`;

export function verdict(median: number): { text: string; side: "under" | "over" | "ok" } {
  if (median < 1 / TOLERANCE) return { text: `${times(1 / median)} too low`, side: "under" };
  if (median > TOLERANCE) return { text: `${times(median)} too high`, side: "over" };
  return { text: "on target", side: "ok" };
}

export function estimateTable(tiers: TierSummary[]): TableView {
  return {
    head: ["Tier", "Jobs", "Median estimate ÷ peak", "Too low", "Too high"],
    rows: tiers.map((t) => [t.tier, t.items.length, times(t.median), `${Math.round((t.under / t.items.length) * 100)}%`, `${Math.round((t.over / t.items.length) * 100)}%`]),
  };
}

function jitter(id: string) {
  let h = 2166136261;
  for (let i = 0; i < id.length; i++) h = Math.imul(h ^ id.charCodeAt(i), 16777619);
  return ((h >>> 0) % 1000) / 1000;
}

const sideColor = (ratio: number) => (ratio < 1 / TOLERANCE ? "var(--div-warm)" : ratio > TOLERANCE ? "var(--div-cool)" : "var(--div-mid)");

function Strip({ tier, bound, onOpen }: { tier: TierSummary; bound: number; onOpen?: (id: string) => void }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const x = (ratio: number) => ((Math.min(bound, Math.max(-bound, Math.log2(ratio))) + bound) / (2 * bound)) * width;
  const dots = tier.items.map((it) => ({ it, cx: x(it.ratio), cy: 8 + jitter(it.id) * (ROW_H - 16) }));

  const nearest = (e: MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    let best = -1;
    let bestD = 24 * 24;
    dots.forEach((d, i) => {
      const dist = (d.cx - px) ** 2 + (d.cy - py) ** 2;
      if (dist < bestD) (bestD = dist), (best = i);
    });
    return best < 0 ? null : best;
  };
  const h = hover !== null ? dots[hover] : undefined;

  return (
    <div ref={ref} className="relative" style={{ height: ROW_H }}>
      {width > 0 && (
        <svg
          width={width}
          height={ROW_H}
          className="block touch-none"
          role="img"
          aria-label={`${tier.tier}: ${tier.items.length} jobs, median estimate ${times(tier.median)} of the real peak`}
          onPointerMove={(e) => setHover(nearest(e))}
          onPointerLeave={() => setHover(null)}
          onClick={(e) => {
            const i = nearest(e);
            if (i !== null && onOpen) onOpen(dots[i]!.it.id);
          }}
          style={{ cursor: h && onOpen ? "pointer" : undefined }}
        >
          <rect x={x(1 / TOLERANCE)} width={x(TOLERANCE) - x(1 / TOLERANCE)} y={0} height={ROW_H} fill="var(--sunken)" />
          <line x1={x(1)} x2={x(1)} y1={0} y2={ROW_H} stroke="var(--axis)" strokeWidth={1} shapeRendering="crispEdges" />
          {dots.map((d, i) => (
            <circle key={d.it.id} cx={d.cx} cy={d.cy} r={hover === i ? 5 : 4} fill={sideColor(d.it.ratio)} fillOpacity={hover === null || hover === i ? 0.9 : 0.4} stroke="var(--surface)" strokeWidth={1} />
          ))}
          <line x1={x(tier.median)} x2={x(tier.median)} y1={2} y2={ROW_H - 2} stroke="var(--fg)" strokeWidth={2} strokeLinecap="round" />
        </svg>
      )}
      {h && (
        <ChartTip
          x={h.cx}
          y={-4}
          width={width}
          title={h.it.job_name || h.it.id}
          rows={[
            { label: "estimated", value: bytes(h.it.estimated_bytes) },
            { label: "real peak", value: bytes(h.it.peak_bytes) },
          ]}
          note={h.it.ratio >= 1 ? `estimate ${times(h.it.ratio)} the peak` : `peak ${times(1 / h.it.ratio)} the estimate`}
        />
      )}
    </div>
  );
}

export function EstimateStrip({ tiers, onOpen }: { tiers: TierSummary[]; onOpen?: (id: string) => void }) {
  const maxLog = Math.max(...tiers.flatMap((t) => t.items.map((i) => Math.abs(Math.log2(i.ratio)))));
  const bound = Math.min(4, Math.max(2, Math.ceil(maxLog)));
  const ticks = Array.from({ length: bound * 2 + 1 }, (_, i) => i - bound).filter((t) => bound <= 3 || t % 2 === 0);

  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-x-4 sm:grid-cols-[8.5rem_minmax(0,1fr)_9.5rem]">
      {tiers.map((t) => {
        const v = verdict(t.median);
        const share = v.side === "over" ? t.over : t.under;
        return (
          <div key={t.tier} className="col-span-full grid grid-cols-subgrid items-center gap-y-1 border-b border-line py-2 first:pt-0">
            <div className="flex items-baseline justify-between gap-2 sm:block">
              <div className="text-sm font-medium">{t.tier}</div>
              <div className="text-xs text-fg-3">{TIER_HINT[t.tier] ?? `${t.items.length} jobs`}</div>
            </div>
            <Strip tier={t} bound={bound} onOpen={onOpen} />
            <div className="flex items-baseline justify-between gap-2 sm:block">
              <div className="flex items-center gap-1.5 text-sm font-medium">
                {v.side === "under" ? (
                  <TriangleAlert className="size-3.5 shrink-0 text-critical-fg" aria-hidden />
                ) : v.side === "over" ? (
                  <MoveRight className="size-3.5 shrink-0 text-accent-fg" aria-hidden />
                ) : (
                  <Check className="size-3.5 shrink-0 text-good-fg" aria-hidden />
                )}
                {v.text}
              </div>
              <div className="num text-xs text-fg-3">
                {t.items.length} jobs{v.side !== "ok" || t.under > 0 ? `, ${Math.round((share / t.items.length) * 100)}% too ${v.side === "over" ? "high" : "low"}` : ""}
              </div>
            </div>
          </div>
        );
      })}
      <div className="col-span-full grid grid-cols-subgrid pt-1">
        <div className="hidden sm:block" />
        <div>
          <div className="relative h-4">
            {ticks.map((t) => (
              <span key={t} className="num absolute -translate-x-1/2 text-2xs text-fg-3" style={{ left: `${((t + bound) / (2 * bound)) * 100}%` }}>
                {t === 0 ? "exact" : t < 0 ? `÷${2 ** -t}` : `×${2 ** t}`}
              </span>
            ))}
          </div>
          <div className="flex justify-between gap-3 pt-1 text-xs text-fg-3">
            <span className="inline-flex items-center gap-1.5">
              <span aria-hidden className="size-2 rounded-full" style={{ background: "var(--div-warm)" }} />
              estimate too low: job can run out of memory
            </span>
            <span className="inline-flex items-center gap-1.5 text-right">
              estimate too high: capacity held back
              <span aria-hidden className="size-2 rounded-full" style={{ background: "var(--div-cool)" }} />
            </span>
          </div>
        </div>
        <div className="hidden sm:block" />
      </div>
    </div>
  );
}
