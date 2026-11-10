import { Ban, Check, CircleDashed, Clock, Hourglass, LoaderCircle, TriangleAlert, X, type LucideIcon } from "lucide-react";
import { cn } from "./cn";

type Tone = "good" | "critical" | "warn" | "accent" | "neutral";

const TONE: Record<Tone, string> = {
  good: "bg-good/12 text-good-fg",
  critical: "bg-critical/12 text-critical-fg",
  warn: "bg-warn/18 text-warn-fg",
  accent: "bg-accent-soft text-accent-fg",
  neutral: "bg-sunken text-fg-2",
};

interface Spec {
  tone: Tone;
  icon: LucideIcon;
  spin?: boolean;
  label?: string;
}

const JOB: Record<string, Spec> = {
  queued: { tone: "neutral", icon: Clock },
  reserved: { tone: "neutral", icon: CircleDashed },
  running: { tone: "accent", icon: LoaderCircle, spin: true },
  succeeded: { tone: "good", icon: Check },
  failed: { tone: "critical", icon: X },
  cancelled: { tone: "neutral", icon: Ban },
  expired: { tone: "warn", icon: Hourglass },
};

const NODE: Record<string, Spec> = {
  healthy: { tone: "good", icon: Check },
  stale: { tone: "warn", icon: TriangleAlert },
  unavailable: { tone: "critical", icon: X, label: "unreachable" },
};

function Chip({ spec, text, className, still }: { spec: Spec; text: string; className?: string; still?: boolean }) {
  const Icon = spec.icon;
  return (
    <span className={cn("inline-flex h-5 items-center gap-1 rounded-sm px-1.5 text-xs font-medium whitespace-nowrap", TONE[spec.tone], className)}>
      <Icon className={cn("size-3", spec.spin && !still && "animate-[spin_1.1s_linear_infinite]")} strokeWidth={2.5} aria-hidden />
      {spec.label ?? text}
    </span>
  );
}

const UNKNOWN: Spec = { tone: "neutral", icon: CircleDashed };

/** `past` marks a state the job has already left (a timeline entry), so nothing animates. */
export function JobStatus({ status, className, past }: { status: string; className?: string; past?: boolean }) {
  return <Chip spec={JOB[status] ?? UNKNOWN} text={status || "unknown"} className={className} still={past} />;
}

export function NodeState({ state, className }: { state: string; className?: string }) {
  return <Chip spec={NODE[state] ?? UNKNOWN} text={state || "unknown"} className={className} />;
}

export const JOB_STATUSES = Object.keys(JOB);
export const nodeTone = (state: string): Tone => NODE[state]?.tone ?? "neutral";
