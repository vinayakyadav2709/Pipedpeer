import { createContext, useContext, type ReactNode } from "react";
import { api } from "./api";
import { usePoll, type Polled } from "./poll";
import type { Overview } from "./types";

const Ctx = createContext<Polled<Overview> | null>(null);

export const OVERVIEW_POLL_MS = 4000;

export function ClusterProvider({ children }: { children: ReactNode }) {
  const overview = usePoll("overview", api.overview, OVERVIEW_POLL_MS);
  return <Ctx.Provider value={overview}>{children}</Ctx.Provider>;
}

export function useOverview(): Polled<Overview> {
  const v = useContext(Ctx);
  if (!v) throw new Error("useOverview outside <ClusterProvider>");
  return v;
}
