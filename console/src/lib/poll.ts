import { useCallback, useEffect, useRef, useState } from "react";

export interface Polled<T> {
  data: T | undefined;
  error: string | null;
  /** True while the first load of the current key has not produced data. */
  loading: boolean;
  /** True when the data on screen belongs to a previous key (window or range switch in flight). */
  transitioning: boolean;
  updatedAt: number | null;
  refresh: () => void;
}

interface Slot<T> {
  key: string;
  data: T | undefined;
  dataKey: string | null;
  error: string | null;
  updatedAt: number | null;
}

/**
 * Polls `fetcher` every `intervalMs` while the tab is visible. A failed poll keeps the last
 * good data and sets `error`, so panels show a stale marker instead of flashing to empty.
 */
export function usePoll<T>(key: string | null, fetcher: (signal: AbortSignal) => Promise<T>, intervalMs: number): Polled<T> {
  const [slot, setSlot] = useState<Slot<T>>({ key: key ?? "", data: undefined, dataKey: null, error: null, updatedAt: null });
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (key === null) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let ctl: AbortController | undefined;
    let alive = true;

    const run = async () => {
      clearTimeout(timer);
      if (document.hidden) return;
      ctl?.abort();
      ctl = new AbortController();
      const mine = ctl;
      try {
        const data = await fetcherRef.current(mine.signal);
        if (!alive || mine.signal.aborted) return;
        setSlot({ key, data, dataKey: key, error: null, updatedAt: Date.now() });
      } catch (err) {
        if (!alive || mine.signal.aborted) return;
        const message = err instanceof Error ? err.message : String(err);
        setSlot((s) => ({ ...s, key, error: message }));
      }
      if (alive) timer = setTimeout(run, intervalMs);
    };

    const onVisibility = () => {
      if (document.hidden) {
        clearTimeout(timer);
        ctl?.abort();
      } else void run();
    };

    void run();
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      alive = false;
      clearTimeout(timer);
      ctl?.abort();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [key, intervalMs, nonce]);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);
  const current = slot.dataKey === key;
  return {
    data: slot.data,
    error: slot.key === key ? slot.error : null,
    loading: slot.data === undefined && !(slot.key === key && slot.error),
    transitioning: slot.data !== undefined && !current,
    updatedAt: slot.updatedAt,
    refresh,
  };
}

export function useNow(intervalMs: number, active = true): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs, active]);
  return now;
}

/**
 * The live endpoints answer with an error while the daemon is stopped. That state already has its own
 * banner, so those panels show their "daemon stopped" empty state instead of repeating the error.
 */
export function whenDaemonDown<T>(polled: Polled<T>, down: boolean, empty: T): Polled<T> {
  return down && polled.error ? { ...polled, data: empty, error: null, loading: false, transitioning: false } : polled;
}
