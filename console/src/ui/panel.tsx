import { CircleAlert, RefreshCw } from "lucide-react";
import type { ReactNode } from "react";
import { ago } from "@/lib/format";
import type { Polled } from "@/lib/poll";
import { cn } from "./cn";
import { Button } from "./controls";

export function PageHeader({ title, meta, actions }: { title: string; meta?: ReactNode; actions?: ReactNode }) {
  return (
    <header className="flex flex-wrap items-end justify-between gap-x-4 gap-y-2 pb-4">
      <div className="min-w-0">
        <h1 className="text-xl font-semibold tracking-[-0.015em]">{title}</h1>
        {meta && <div className="mt-0.5 text-sm text-fg-3">{meta}</div>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </header>
  );
}

export function Panel({ title, hint, actions, children, className, bodyClassName, stale }: { title?: ReactNode; hint?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string; bodyClassName?: string; stale?: ReactNode }) {
  return (
    <section className={cn("min-w-0 rounded-lg border border-line bg-surface", className)}>
      {(title || actions) && (
        <div className="flex min-h-11 flex-wrap items-center justify-between gap-x-3 gap-y-1 border-b border-line px-4 py-2">
          <div className="flex min-w-0 flex-wrap items-baseline gap-x-2.5">
            <h2 className="text-sm font-semibold">{title}</h2>
            {hint && <span className="text-xs text-fg-3">{hint}</span>}
            {stale}
          </div>
          {actions && <div className="flex items-center gap-2">{actions}</div>}
        </div>
      )}
      <div className={bodyClassName}>{children}</div>
    </section>
  );
}

/** Shown beside a title when the last poll failed and the panel is displaying older data. */
export function StaleMark({ polled, now }: { polled: Pick<Polled<unknown>, "error" | "updatedAt" | "data">; now?: number }) {
  if (!polled.error || polled.data === undefined) return null;
  return (
    <span className="inline-flex items-center gap-1 text-xs text-warn-fg" title={polled.error}>
      <CircleAlert className="size-3" aria-hidden />
      stale{polled.updatedAt ? `, last updated ${ago(polled.updatedAt, now)}` : ""}
    </span>
  );
}

export function EmptyState({ title, children, className }: { title: string; children?: ReactNode; className?: string }) {
  return (
    <div className={cn("flex flex-col items-start gap-1 px-4 py-6", className)}>
      <p className="text-sm font-medium text-fg-2">{title}</p>
      {children && <div className="max-w-[60ch] text-sm text-fg-3">{children}</div>}
    </div>
  );
}

export function ErrorState({ message, onRetry, className }: { message: string; onRetry?: () => void; className?: string }) {
  return (
    <div role="alert" className={cn("flex flex-col items-start gap-2 px-4 py-6", className)}>
      <p className="flex items-center gap-1.5 text-sm font-medium text-critical-fg">
        <CircleAlert className="size-4" aria-hidden />
        Could not load this
      </p>
      <p className="max-w-[70ch] font-mono text-xs break-words text-fg-2">{message}</p>
      {onRetry && (
        <Button size="sm" onClick={onRetry} icon={<RefreshCw className="size-3" aria-hidden />}>
          Retry
        </Button>
      )}
    </div>
  );
}

export function SkeletonRows({ rows = 4, className }: { rows?: number; className?: string }) {
  return (
    <div className={cn("flex flex-col gap-2.5 px-4 py-4", className)} aria-busy="true" aria-label="Loading">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="skeleton h-4" style={{ width: `${88 - ((i * 17) % 40)}%` }} />
      ))}
    </div>
  );
}

export interface AsyncProps<T> {
  polled: Polled<T>;
  children: (data: T) => ReactNode;
  skeleton?: ReactNode;
  isEmpty?: (data: T) => boolean;
  empty?: ReactNode;
}

/** Loading, error, empty and ready states for one polled resource. Never unmounts good data on a failed poll. */
export function Async<T>({ polled, children, skeleton, isEmpty, empty }: AsyncProps<T>) {
  if (polled.data === undefined) {
    if (polled.error) return <ErrorState message={polled.error} onRetry={polled.refresh} />;
    return <>{skeleton ?? <SkeletonRows />}</>;
  }
  if (isEmpty?.(polled.data)) return <>{empty}</>;
  return <div className={cn("transition-opacity duration-200", polled.transitioning && "opacity-50")}>{children(polled.data)}</div>;
}
