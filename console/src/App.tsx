import { LayoutDashboard, ListChecks, Monitor, Moon, NotebookText, Server, Sun, type LucideIcon } from "lucide-react";
import { lazy, Suspense, useEffect, useState, type ReactNode } from "react";
import { onUnauthorized } from "@/lib/api";
import { ClusterProvider, useOverview } from "@/lib/cluster";
import { Link, Router, useRouter, type Route } from "@/lib/router";
import { useTheme, type ThemeChoice } from "@/lib/theme";
import { JobDetailPage } from "@/pages/JobDetail";
import { JobsPage } from "@/pages/Jobs";
import { NodesPage } from "@/pages/Nodes";
import { NotebooksPage } from "@/pages/Notebooks";
import { OverviewPage } from "@/pages/Overview";
import { cn } from "@/ui/cn";
import { SkeletonRows } from "@/ui/panel";

const NotebookPage = lazy(() => import("@/notebook/NotebookPage"));

const NAV: { href: string; label: string; icon: LucideIcon; match: Route["name"][] }[] = [
  { href: "/", label: "Overview", icon: LayoutDashboard, match: ["overview"] },
  { href: "/nodes", label: "Nodes", icon: Server, match: ["nodes"] },
  { href: "/jobs", label: "Jobs", icon: ListChecks, match: ["jobs", "job"] },
  { href: "/notebooks", label: "Notebooks", icon: NotebookText, match: ["notebooks", "notebook"] },
];

function Mark() {
  return (
    <svg viewBox="0 0 32 32" className="size-6 shrink-0" aria-hidden>
      <rect width="32" height="32" rx="7" className="fill-solid" />
      <g fill="none" className="stroke-solid-fg" strokeWidth="2.4" strokeLinecap="round">
        <path d="M9 11h7.5a4.5 4.5 0 0 1 0 9H9" />
        <path d="M9 8v16" />
      </g>
      <circle cx="23.5" cy="21.5" r="2.4" className="fill-accent" />
    </svg>
  );
}

const THEMES: { value: ThemeChoice; label: string; icon: LucideIcon }[] = [
  { value: "system", label: "Follow system theme", icon: Monitor },
  { value: "light", label: "Light theme", icon: Sun },
  { value: "dark", label: "Dark theme", icon: Moon },
];

function ThemeSwitch() {
  const { choice, setChoice } = useTheme();
  return (
    <div role="radiogroup" aria-label="Theme" className="inline-flex rounded-md border border-line p-0.5">
      {THEMES.map((t) => (
        <button
          key={t.value}
          type="button"
          role="radio"
          aria-checked={choice === t.value}
          aria-label={t.label}
          title={t.label}
          onClick={() => setChoice(t.value)}
          className={cn("inline-flex size-6 items-center justify-center rounded-[4px] transition-colors duration-150", choice === t.value ? "bg-hover text-fg" : "text-fg-3 hover:text-fg")}
        >
          <t.icon className="size-3.5" aria-hidden />
        </button>
      ))}
    </div>
  );
}

function DaemonLine() {
  const { data, error } = useOverview();
  const up = data?.daemon.reachable;
  const tone = data === undefined ? "bg-fg-3" : error ? "bg-warn" : up ? "bg-good" : "bg-critical";
  const text = data === undefined ? (error ? "console server not responding" : "connecting") : up ? `daemon on :${data.daemon.port}` : "daemon not running";
  return (
    <div className="flex min-w-0 items-center gap-2 text-xs text-fg-3" title={data?.daemon.error || error || undefined}>
      <span aria-hidden className={cn("size-1.5 shrink-0 rounded-full", tone)} />
      <span className="truncate">{text}</span>
    </div>
  );
}

function Shell({ children }: { children: ReactNode }) {
  const { route } = useRouter();
  const { data } = useOverview();
  return (
    <div className="min-h-dvh lg:grid lg:grid-cols-[13.5rem_minmax(0,1fr)]">
      <a href="#main" className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-50 focus:rounded-md focus:bg-solid focus:px-3 focus:py-1.5 focus:text-sm focus:text-solid-fg">
        Skip to content
      </a>
      <aside className="sticky top-0 z-30 flex border-b border-line bg-sunken max-lg:flex-col lg:h-dvh lg:flex-col lg:border-r lg:border-b-0">
        <div className="flex h-12 items-center justify-between gap-3 px-4 lg:h-14">
          <Link href="/" className="flex items-center gap-2.5 rounded-md">
            <Mark />
            <span className="text-[0.9375rem] font-semibold tracking-[-0.01em]">Pipedpeer</span>
            <span className="text-xs text-fg-3">console</span>
          </Link>
          <div className="lg:hidden">
            <ThemeSwitch />
          </div>
        </div>
        <nav aria-label="Main" className="flex gap-0.5 overflow-x-auto px-2 max-lg:pb-1.5 max-sm:justify-between lg:flex-col lg:py-1">
          {NAV.map((n) => {
            const on = n.match.includes(route.name);
            return (
              <Link
                key={n.href}
                href={n.href}
                aria-current={on ? "page" : undefined}
                className={cn(
                  "flex h-8 shrink-0 items-center gap-2.5 rounded-md px-2.5 text-sm transition-colors duration-150 max-sm:gap-1.5 max-sm:px-2",
                  on ? "bg-surface font-medium text-fg shadow-[0_0_0_1px_var(--line)]" : "text-fg-2 hover:bg-hover hover:text-fg",
                )}
              >
                <n.icon className={cn("size-4", on ? "text-fg" : "text-fg-3")} aria-hidden />
                {n.label}
              </Link>
            );
          })}
        </nav>
        <div className="mt-auto hidden flex-col gap-3 border-t border-line p-4 lg:flex">
          <div className="min-w-0">
            <div className="truncate text-sm font-medium" title={data?.self.node_id}>
              {data?.self.hostname ?? " "}
            </div>
            <DaemonLine />
          </div>
          <ThemeSwitch />
        </div>
      </aside>
      <main id="main" className="min-w-0">
        {children}
      </main>
    </div>
  );
}

function Unauthorized() {
  return (
    <div className="mx-auto flex min-h-dvh max-w-lg flex-col justify-center gap-3 px-6">
      <Mark />
      <h1 className="text-xl font-semibold">This session is not signed in</h1>
      <p className="text-fg-2">
        The console only accepts the link it printed when it started. Find the line in the terminal where you ran <code className="rounded-sm border border-line bg-sunken px-1 text-xs">pipedpeer console</code> and open that URL, including its <code className="rounded-sm border border-line bg-sunken px-1 text-xs">?token=</code> part.
      </p>
    </div>
  );
}

function NotFound() {
  return (
    <div className="px-6 py-10">
      <h1 className="text-xl font-semibold">Nothing at this address</h1>
      <p className="mt-1 text-fg-2">
        Go back to the <Link href="/" className="text-accent-fg underline">overview</Link>.
      </p>
    </div>
  );
}

export function Page({ children, wide }: { children: ReactNode; wide?: boolean }) {
  return <div className={cn("mx-auto px-4 pt-5 pb-16 sm:px-6", wide ? "max-w-[92rem]" : "max-w-[82rem]")}>{children}</div>;
}

function Routes() {
  const { route } = useRouter();
  switch (route.name) {
    case "overview":
      return <OverviewPage />;
    case "nodes":
      return <NodesPage />;
    case "jobs":
      return <JobsPage />;
    case "job":
      return <JobDetailPage key={route.id} id={route.id} />;
    case "notebooks":
      return <NotebooksPage />;
    case "notebook":
      return (
        <Suspense fallback={<SkeletonRows rows={6} className="mx-auto max-w-4xl pt-10" />}>
          <NotebookPage key={route.path} path={route.path} />
        </Suspense>
      );
    case "notfound":
      return <NotFound />;
  }
}

export function App() {
  const [unauthorized, setUnauthorized] = useState(false);
  useEffect(() => onUnauthorized(() => setUnauthorized(true)), []);
  if (unauthorized) return <Unauthorized />;
  return (
    <Router>
      <ClusterProvider>
        <Shell>
          <Routes />
        </Shell>
      </ClusterProvider>
    </Router>
  );
}
