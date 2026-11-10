import { ChevronDown, ChevronRight, ChevronsDownUp, ChevronsUpDown } from "lucide-react";
import { memo, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Ansi } from "@/lib/ansi";
import { useResolvedTheme, type Theme } from "@/lib/theme";
import type { MimeBundle } from "@/lib/types";
import { cn } from "@/ui/cn";
import type { Output } from "./nbformat";

const COLLAPSED_PX = 384;
const FRAME_MAX_PX = 640;

const FRAME_COLORS: Record<Theme, { fg: string; fg2: string; line: string; zebra: string; hover: string; link: string }> = {
  light: { fg: "#171717", fg2: "#5c5c5c", line: "#e4e4e4", zebra: "#f7f7f7", hover: "#eef4fd", link: "#1c5cab" },
  dark: { fg: "#f5f5f5", fg2: "#b5b5b5", line: "#333333", zebra: "#1f1f1f", hover: "#15273f", link: "#86b6ef" },
};

/**
 * The output's HTML runs in a frame with an opaque origin: it cannot read the console's cookie or DOM, and the
 * policy blocks plain-http requests, so a script in a cell output cannot call the console or daemon on localhost.
 */
function frameDocument(html: string, theme: Theme, token: string): string {
  const c = FRAME_COLORS[theme];
  return `<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline' 'unsafe-eval' https:; style-src 'unsafe-inline' https:; img-src data: blob: https:; font-src data: https:; media-src data: blob: https:; connect-src https:">
<style>
:root{color-scheme:${theme}}
html,body{margin:0;background:transparent;color:${c.fg};font:13px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
body{padding:2px 0;overflow-x:auto}
a{color:${c.link}}
table{border-collapse:collapse;border:0;font-size:12.5px;font-variant-numeric:tabular-nums}
table.dataframe{margin:0}
th,td{border:0;border-bottom:1px solid ${c.line};padding:4px 12px 4px 8px;text-align:right;white-space:nowrap}
thead th{color:${c.fg2};font-weight:600;border-bottom-color:${c.fg2}}
tbody th{color:${c.fg2};font-weight:500}
tbody tr:nth-child(even){background:${c.zebra}}
tbody tr:hover{background:${c.hover}}
p{margin:6px 0;color:${c.fg2}}
pre,code{font:12px/1.5 ui-monospace,Menlo,monospace}
img,svg,canvas,video{max-width:100%}
</style></head><body>${html}
<script>(function(){var last=0;function post(){var h=Math.ceil(document.documentElement.getBoundingClientRect().height);if(h!==last){last=h;parent.postMessage({ppFrame:${JSON.stringify(token)},height:h},"*")}}
new ResizeObserver(post).observe(document.documentElement);addEventListener("load",post);post()})()</script>
</body></html>`;
}

function HtmlFrame({ html }: { html: string }) {
  const theme = useResolvedTheme();
  const token = useId();
  const ref = useRef<HTMLIFrameElement>(null);
  const [height, setHeight] = useState(48);
  const doc = useMemo(() => frameDocument(html, theme, token), [html, theme, token]);

  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      const d = e.data as { ppFrame?: unknown; height?: unknown } | null;
      if (e.source !== ref.current?.contentWindow || !d || d.ppFrame !== token || typeof d.height !== "number") return;
      setHeight(Math.min(FRAME_MAX_PX, Math.max(16, d.height)));
    };
    addEventListener("message", onMessage);
    return () => removeEventListener("message", onMessage);
  }, [token]);

  return <iframe ref={ref} title="HTML output" sandbox="allow-scripts" referrerPolicy="no-referrer" srcDoc={doc} className="block w-full border-0" style={{ height }} />;
}

function JsonNode({ name, value, depth }: { name?: string; value: unknown; depth: number }) {
  const [open, setOpen] = useState(depth < 2);
  const key = name !== undefined && <span className="text-[var(--syn-def)]">{name}: </span>;
  if (value === null || typeof value !== "object") {
    const cls = value === null ? "text-fg-3" : typeof value === "string" ? "text-[var(--syn-string)]" : typeof value === "number" ? "text-[var(--syn-number)]" : "text-[var(--syn-keyword)]";
    return (
      <div className="pl-4">
        {key}
        <span className={cls}>{typeof value === "string" ? JSON.stringify(value) : String(value)}</span>
      </div>
    );
  }
  const entries: [string, unknown][] = Array.isArray(value) ? value.map((v, i) => [String(i), v]) : Object.entries(value);
  const [l, r] = Array.isArray(value) ? ["[", "]"] : ["{", "}"];
  if (!entries.length)
    return (
      <div className="pl-4">
        {key}
        {l}
        {r}
      </div>
    );
  return (
    <div>
      <button type="button" aria-expanded={open} onClick={() => setOpen((o) => !o)} className="flex items-center rounded-sm text-left hover:bg-hover">
        {open ? <ChevronDown className="size-4 shrink-0 text-fg-3" aria-hidden /> : <ChevronRight className="size-4 shrink-0 text-fg-3" aria-hidden />}
        <span>
          {key}
          {l}
          {!open && (
            <span className="text-fg-3">
              {" "}
              {entries.length} {Array.isArray(value) ? (entries.length === 1 ? "item" : "items") : entries.length === 1 ? "key" : "keys"} {r}
            </span>
          )}
        </span>
      </button>
      {open && (
        <>
          <div className="ml-2 border-l border-line pl-2">
            {entries.map(([k, v]) => (
              <JsonNode key={k} name={Array.isArray(value) ? undefined : k} value={v} depth={depth + 1} />
            ))}
          </div>
          <div className="pl-4">{r}</div>
        </>
      )}
    </div>
  );
}

const pre = "font-mono text-[0.8125rem] leading-[1.55] whitespace-pre-wrap break-words";

function Rich({ data }: { data: MimeBundle }) {
  const str = (k: string) => (typeof data[k] === "string" ? (data[k] as string) : null);
  const html = str("text/html");
  if (html !== null) return <HtmlFrame html={html} />;
  const svg = str("image/svg+xml");
  if (svg !== null) return <img alt={str("text/plain") ?? "SVG output"} className="max-w-full rounded-sm bg-white" src={`data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`} />;
  for (const mime of ["image/png", "image/jpeg", "image/gif"]) {
    const b64 = str(mime);
    if (b64 !== null) return <img alt={str("text/plain") ?? "Image output"} className="max-w-full rounded-sm" src={`data:${mime};base64,${b64.replace(/\s+/g, "")}`} />;
  }
  if ("application/json" in data)
    return (
      <div className="font-mono text-[0.8125rem] leading-[1.55]">
        <JsonNode value={data["application/json"]} depth={0} />
      </div>
    );
  const plain = str("text/plain");
  return plain === null ? null : <pre className={pre}>{plain}</pre>;
}

const OutputView = memo(function OutputView({ output }: { output: Output }) {
  switch (output.output_type) {
    case "stream":
      return output.name === "stderr" ? (
        <div className="rounded-md bg-critical/8 px-2.5 py-1.5">
          <div className="pb-0.5 font-sans text-2xs font-medium text-critical-fg">stderr</div>
          <pre className={pre}>
            <Ansi text={output.text} />
          </pre>
        </div>
      ) : (
        <pre className={pre}>
          <Ansi text={output.text} />
        </pre>
      );
    case "error":
      return (
        <div role="alert" className="rounded-md border border-critical/35 bg-critical/8 px-2.5 py-2">
          <div className="pb-1 font-sans text-sm">
            <span className="font-semibold text-critical-fg">{output.ename || "Error"}</span>
            {output.evalue && <span className="text-fg">: {output.evalue}</span>}
          </div>
          {output.traceback.length > 0 && (
            <pre className="overflow-x-auto font-mono text-xs leading-[1.55] whitespace-pre">
              <Ansi text={output.traceback.join("\n")} />
            </pre>
          )}
        </div>
      );
    case "display_data":
    case "execute_result":
      return <Rich data={output.data} />;
  }
});

/** A cell's outputs. Anything taller than the collapsed height scrolls inside it until expanded. */
export function OutputArea({ outputs, live, actions }: { outputs: Output[]; live: boolean; actions?: ReactNode }) {
  const scroller = useRef<HTMLDivElement>(null);
  const inner = useRef<HTMLDivElement>(null);
  const [tall, setTall] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const pinned = useRef(true);

  useLayoutEffect(() => {
    const el = inner.current;
    if (!el) return;
    const measure = () => setTall(el.scrollHeight > COLLAPSED_PX + 24);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useLayoutEffect(() => {
    const el = scroller.current;
    if (live && el && !expanded && pinned.current) el.scrollTop = el.scrollHeight;
  }, [outputs, live, expanded]);

  if (!outputs.length) return null;
  return (
    <div className="group/out relative">
      <div
        ref={scroller}
        tabIndex={tall && !expanded ? 0 : undefined}
        aria-label={tall && !expanded ? "Cell output, scrollable" : undefined}
        onScroll={(e) => {
          const el = e.currentTarget;
          pinned.current = el.scrollHeight - el.scrollTop - el.clientHeight < 32;
        }}
        className={cn(tall && !expanded && "overflow-y-auto rounded-md border border-line")}
        style={tall && !expanded ? { maxHeight: COLLAPSED_PX } : undefined}
      >
        <div ref={inner} className={cn("flex flex-col gap-2", tall && !expanded && "p-2")}>
          {outputs.map((o, i) => (
            <OutputView key={i} output={o} />
          ))}
        </div>
      </div>
      {(tall || actions) && (
        <div className="flex items-center gap-1 pt-1">
          {tall && (
            <button type="button" onClick={() => setExpanded((e) => !e)} className="inline-flex h-6 items-center gap-1 rounded-sm px-1.5 text-xs text-fg-3 hover:bg-hover hover:text-fg">
              {expanded ? <ChevronsDownUp className="size-3.5" aria-hidden /> : <ChevronsUpDown className="size-3.5" aria-hidden />}
              {expanded ? "Collapse output" : "Show all output"}
            </button>
          )}
          {actions}
        </div>
      )}
    </div>
  );
}
