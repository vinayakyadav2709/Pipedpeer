import { kernelSocketUrl } from "@/lib/api";
import type { ClientFrame, KernelVar, ServerFrame } from "@/lib/types";
import type { Link } from "./state";

export interface Completion {
  matches: string[];
  from: number;
  to: number;
}

const REPLY_TIMEOUT_MS = 2500;
const BACKOFF_MS = [400, 800, 1600, 3200, 5000];

/** One WebSocket to a notebook's kernel, reconnecting until closed. The kernel itself outlives the socket. */
export class KernelClient {
  private ws: WebSocket | null = null;
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;
  private seq = 0;
  private waiting = new Map<string, (frame: ServerFrame | null) => void>();

  private readonly notebook: string;
  private readonly onFrame: (frame: ServerFrame) => void;
  private readonly onLink: (link: Link) => void;

  constructor(notebook: string, onFrame: (frame: ServerFrame) => void, onLink: (link: Link) => void) {
    this.notebook = notebook;
    this.onFrame = onFrame;
    this.onLink = onLink;
  }

  connect() {
    if (this.closed) return;
    this.onLink("connecting");
    const ws = new WebSocket(kernelSocketUrl(this.notebook));
    this.ws = ws;
    ws.onopen = () => {
      this.attempt = 0;
      this.onLink("open");
    };
    ws.onmessage = (e) => {
      let frame: ServerFrame;
      try {
        frame = JSON.parse(String(e.data)) as ServerFrame;
      } catch {
        return;
      }
      if (frame.type === "complete_reply" || frame.type === "vars_reply") {
        const resolve = this.waiting.get(frame.req_id);
        this.waiting.delete(frame.req_id);
        resolve?.(frame);
        return;
      }
      this.onFrame(frame);
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      for (const resolve of this.waiting.values()) resolve(null);
      this.waiting.clear();
      if (this.closed) return;
      this.onLink("closed");
      const delay = BACKOFF_MS[Math.min(this.attempt++, BACKOFF_MS.length - 1)]!;
      this.timer = setTimeout(() => this.connect(), delay);
    };
  }

  close() {
    this.closed = true;
    clearTimeout(this.timer);
    this.ws?.close();
    this.ws = null;
  }

  get open() {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  send(frame: ClientFrame): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return false;
    this.ws.send(JSON.stringify(frame));
    return true;
  }

  private request(build: (reqId: string) => ClientFrame): Promise<ServerFrame | null> {
    const reqId = `r${++this.seq}`;
    return new Promise((resolve) => {
      if (!this.send(build(reqId))) return resolve(null);
      const timer = setTimeout(() => {
        this.waiting.delete(reqId);
        resolve(null);
      }, REPLY_TIMEOUT_MS);
      this.waiting.set(reqId, (frame) => {
        clearTimeout(timer);
        resolve(frame);
      });
    });
  }

  async complete(code: string, cursor: number): Promise<Completion | null> {
    const f = await this.request((req_id) => ({ type: "complete", req_id, code, cursor }));
    return f?.type === "complete_reply" ? { matches: f.matches ?? [], from: f.cursor_start, to: f.cursor_end } : null;
  }

  async inspectVars(): Promise<KernelVar[] | null> {
    const f = await this.request((req_id) => ({ type: "inspect_vars", req_id }));
    return f?.type === "vars_reply" ? (f.vars ?? []) : null;
  }
}
