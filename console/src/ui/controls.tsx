import { Check, Copy } from "lucide-react";
import { useEffect, useRef, useState, type ButtonHTMLAttributes, type ReactNode } from "react";
import { cn } from "./cn";

type Variant = "solid" | "outline" | "ghost" | "danger";

const VARIANT: Record<Variant, string> = {
  solid: "bg-solid text-solid-fg hover:opacity-90 active:opacity-80",
  outline: "border border-line-strong bg-surface text-fg hover:bg-hover active:bg-sunken",
  ghost: "text-fg-2 hover:bg-hover hover:text-fg active:bg-sunken",
  danger: "border border-critical/50 text-critical-fg hover:bg-critical/10",
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: "sm" | "md";
  icon?: ReactNode;
}

export function Button({ variant = "outline", size = "md", icon, className, children, type = "button", ...rest }: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(
        "inline-flex shrink-0 items-center justify-center gap-1.5 rounded-md font-medium whitespace-nowrap transition-colors duration-150 select-none disabled:opacity-45 disabled:pointer-events-none",
        size === "sm" ? "h-7 px-2 text-xs" : "h-8 px-3 text-sm",
        VARIANT[variant],
        className,
      )}
      {...rest}
    >
      {icon}
      {children}
    </button>
  );
}

export interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  label: string;
  size?: "sm" | "md";
  active?: boolean;
}

export function IconButton({ label, size = "md", active, className, children, type = "button", ...rest }: IconButtonProps) {
  return (
    <button
      type={type}
      aria-label={label}
      title={label}
      aria-pressed={active}
      className={cn(
        "inline-flex shrink-0 items-center justify-center rounded-md text-fg-2 transition-colors duration-150 hover:bg-hover hover:text-fg active:bg-sunken disabled:opacity-40 disabled:pointer-events-none",
        size === "sm" ? "size-7" : "size-8",
        active && "bg-hover text-fg",
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
}

export interface SegmentedProps<T extends string> {
  label: string;
  value: T;
  options: readonly (T | { value: T; label: string })[];
  onChange: (v: T) => void;
  className?: string;
}

export function Segmented<T extends string>({ label, value, options, onChange, className }: SegmentedProps<T>) {
  return (
    <div role="radiogroup" aria-label={label} className={cn("inline-flex h-7 items-stretch rounded-md border border-line bg-sunken p-0.5", className)}>
      {options.map((o) => {
        const v = typeof o === "string" ? o : o.value;
        const text = typeof o === "string" ? o : o.label;
        const on = v === value;
        return (
          <button
            key={v}
            type="button"
            role="radio"
            aria-checked={on}
            onClick={() => onChange(v)}
            className={cn(
              "num rounded-[4px] px-2 text-xs font-medium transition-colors duration-150",
              on ? "bg-surface text-fg shadow-[0_0_0_1px_var(--line),0_1px_1px_oklch(0_0_0/0.06)]" : "text-fg-3 hover:text-fg",
            )}
          >
            {text}
          </button>
        );
      })}
    </div>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="inline-flex h-5 min-w-5 items-center justify-center rounded-sm border border-line-strong bg-sunken px-1 font-mono text-2xs text-fg-2">
      {children}
    </kbd>
  );
}

export function CopyButton({ text, label = "Copy", className }: { text: string | (() => string); label?: string; className?: string }) {
  const [done, setDone] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(typeof text === "function" ? text() : text);
      setDone(true);
      clearTimeout(timer.current);
      timer.current = setTimeout(() => setDone(false), 1600);
    } catch {
      setDone(false);
    }
  };
  return (
    <IconButton label={done ? "Copied" : label} size="sm" onClick={copy} className={className}>
      {done ? <Check className="size-3.5 text-good-fg" aria-hidden /> : <Copy className="size-3.5" aria-hidden />}
    </IconButton>
  );
}

export function Meter({ value, max = 100, tone = "accent", reserved, label, className }: { value: number; max?: number; tone?: "accent" | "auto" | "muted"; reserved?: number; label: string; className?: string }) {
  const pct = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;
  const resPct = reserved && max > 0 ? Math.min(100 - pct, (reserved / max) * 100) : 0;
  const color = tone === "muted" ? "var(--s-mute)" : tone === "auto" && pct >= 90 ? "var(--critical)" : tone === "auto" && pct >= 75 ? "var(--serious)" : "var(--accent)";
  return (
    <div
      role="meter"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(pct)}
      className={cn("flex h-1.5 w-full overflow-hidden rounded-full", className)}
      style={{ background: `color-mix(in oklab, ${color} 16%, var(--sunken))` }}
    >
      <div className="h-full rounded-full transition-[width] duration-500 ease-out" style={{ width: `${pct}%`, background: color }} />
      {resPct > 0 && <div className="ml-0.5 h-full rounded-full transition-[width] duration-500" style={{ width: `${resPct}%`, background: `color-mix(in oklab, ${color} 45%, transparent)` }} />}
    </div>
  );
}
