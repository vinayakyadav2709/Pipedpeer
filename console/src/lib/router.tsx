import { createContext, useCallback, useContext, useEffect, useMemo, useState, type AnchorHTMLAttributes, type MouseEvent, type ReactNode } from "react";

export type Route =
  | { name: "overview" }
  | { name: "nodes" }
  | { name: "jobs" }
  | { name: "job"; id: string }
  | { name: "notebooks" }
  | { name: "notebook"; path: string }
  | { name: "notfound" };

export function parseRoute(pathname: string): Route {
  const p = pathname.replace(/\/+$/, "") || "/";
  if (p === "/") return { name: "overview" };
  if (p === "/nodes") return { name: "nodes" };
  if (p === "/jobs") return { name: "jobs" };
  if (p.startsWith("/jobs/")) return { name: "job", id: decodeURIComponent(p.slice(6)) };
  if (p === "/notebooks") return { name: "notebooks" };
  if (p.startsWith("/notebooks/")) return { name: "notebook", path: p.slice(11).split("/").map(decodeURIComponent).join("/") };
  return { name: "notfound" };
}

export const notebookHref = (path: string) => `/notebooks/${path.split("/").map(encodeURIComponent).join("/")}`;
export const jobHref = (id: string) => `/jobs/${encodeURIComponent(id)}`;

interface RouterValue {
  pathname: string;
  search: string;
  route: Route;
  navigate: (to: string, opts?: { replace?: boolean }) => void;
}

const Ctx = createContext<RouterValue | null>(null);

export function Router({ children }: { children: ReactNode }) {
  const [loc, setLoc] = useState(() => ({ pathname: location.pathname, search: location.search }));

  useEffect(() => {
    const sync = () => setLoc({ pathname: location.pathname, search: location.search });
    addEventListener("popstate", sync);
    return () => removeEventListener("popstate", sync);
  }, []);

  const navigate = useCallback((to: string, opts?: { replace?: boolean }) => {
    if (opts?.replace) history.replaceState(null, "", to);
    else history.pushState(null, "", to);
    setLoc({ pathname: location.pathname, search: location.search });
    if (!opts?.replace) scrollTo(0, 0);
  }, []);

  const value = useMemo(() => ({ ...loc, route: parseRoute(loc.pathname), navigate }), [loc, navigate]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useRouter(): RouterValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("useRouter outside <Router>");
  return v;
}

export function Link({ href, onClick, ...rest }: AnchorHTMLAttributes<HTMLAnchorElement> & { href: string }) {
  const { navigate } = useRouter();
  const handle = (e: MouseEvent<HTMLAnchorElement>) => {
    onClick?.(e);
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    e.preventDefault();
    navigate(href);
  };
  return <a href={href} onClick={handle} {...rest} />;
}
