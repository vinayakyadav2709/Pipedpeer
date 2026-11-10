import { ArrowDownToLine, WrapText } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { Ansi, applyCarriageReturns, stripAnsi } from "@/lib/ansi";
import { plural } from "@/lib/format";
import { cn } from "./cn";
import { CopyButton, IconButton } from "./controls";

export function LogViewer({ text, title = "Output", empty }: { text: string; title?: string; empty: string }) {
  const [wrap, setWrap] = useState(false);
  const scroller = useRef<HTMLDivElement>(null);
  const pinned = useRef(true);
  const lines = useMemo(() => {
    const ls = applyCarriageReturns(text).split("\n");
    if (ls[ls.length - 1] === "") ls.pop();
    return ls;
  }, [text]);

  useEffect(() => {
    const el = scroller.current;
    if (el && pinned.current) el.scrollTop = el.scrollHeight;
  }, [lines]);

  const toEnd = () => {
    const el = scroller.current;
    if (el) el.scrollTop = el.scrollHeight;
  };

  return (
    <section className="flex min-w-0 flex-col rounded-lg border border-line bg-surface" aria-label={title}>
      <div className="flex min-h-10 items-center justify-between gap-2 border-b border-line py-1 pr-1.5 pl-4">
        <h2 className="text-sm font-semibold">
          {title} {lines.length > 0 && <span className="num ml-1.5 text-xs font-normal text-fg-3">last {plural(lines.length, "line")}</span>}
        </h2>
        {lines.length > 0 && (
          <div className="flex items-center gap-0.5">
            <IconButton size="sm" label={wrap ? "Do not wrap long lines" : "Wrap long lines"} active={wrap} onClick={() => setWrap((w) => !w)}>
              <WrapText className="size-3.5" aria-hidden />
            </IconButton>
            <IconButton size="sm" label="Jump to the end" onClick={toEnd}>
              <ArrowDownToLine className="size-3.5" aria-hidden />
            </IconButton>
            <CopyButton text={() => stripAnsi(lines.join("\n"))} label="Copy log" />
          </div>
        )}
      </div>
      {lines.length === 0 ? (
        <p className="px-4 py-6 text-sm text-fg-3">{empty}</p>
      ) : (
        <div
          ref={scroller}
          tabIndex={0}
          onScroll={(e) => {
            const el = e.currentTarget;
            pinned.current = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
          }}
          className="max-h-[70vh] min-h-32 overflow-auto rounded-b-lg bg-sunken py-2 font-mono text-xs leading-[1.6] focus-visible:outline-offset-[-2px]"
        >
          <div className={cn("grid grid-cols-[auto_minmax(0,1fr)]", !wrap && "w-max min-w-full")}>
            {lines.map((line, i) => (
              <div key={i} className="col-span-2 grid grid-cols-subgrid hover:bg-hover">
                <span className="num sticky left-0 bg-sunken pr-3 pl-4 text-right text-fg-3 select-none" aria-hidden>
                  {i + 1}
                </span>
                <span className={cn("pr-4", wrap ? "break-words whitespace-pre-wrap" : "whitespace-pre")}>
                  <Ansi text={line} />
                  {"\n"}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
