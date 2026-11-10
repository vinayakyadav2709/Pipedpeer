import { ChevronDown } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import { cn } from "./cn";

export interface MenuItem {
  label: string;
  hint?: string;
  icon?: ReactNode;
  disabled?: boolean;
  onSelect: () => void;
}

/** A button that opens a small action menu. The panel is fixed-positioned so scrolling toolbars never clip it. */
export function Menu({ label, icon, items, showLabel = true, className }: { label: string; icon?: ReactNode; items: MenuItem[]; showLabel?: boolean; className?: string }) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const button = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    if (!open || !button.current || !panel.current) return;
    const b = button.current.getBoundingClientRect();
    const w = panel.current.offsetWidth;
    setPos({ top: b.bottom + 4, left: Math.max(8, Math.min(b.left, innerWidth - w - 8)) });
    panel.current.querySelector<HTMLButtonElement>("button:not(:disabled)")?.focus();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const close = (e: Event) => {
      if (e.target instanceof Node && (panel.current?.contains(e.target) || button.current?.contains(e.target))) return;
      setOpen(false);
    };
    const dismiss = () => setOpen(false);
    addEventListener("pointerdown", close);
    addEventListener("resize", dismiss);
    addEventListener("scroll", dismiss, true);
    return () => {
      removeEventListener("pointerdown", close);
      removeEventListener("resize", dismiss);
      removeEventListener("scroll", dismiss, true);
    };
  }, [open]);

  const onKey = (e: KeyboardEvent) => {
    const buttons = [...(panel.current?.querySelectorAll<HTMLButtonElement>("button:not(:disabled)") ?? [])];
    const i = buttons.indexOf(document.activeElement as HTMLButtonElement);
    if (e.key === "Escape") {
      e.stopPropagation();
      setOpen(false);
      button.current?.focus();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      buttons[(i + 1) % buttons.length]?.focus();
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      buttons[(i - 1 + buttons.length) % buttons.length]?.focus();
    } else if (e.key === "Tab") setOpen(false);
  };

  return (
    <>
      <button
        ref={button}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={showLabel ? undefined : label}
        title={showLabel ? undefined : label}
        onClick={() => setOpen((o) => !o)}
        className={cn(
          "inline-flex h-7 shrink-0 items-center gap-1 rounded-md px-1.5 text-xs font-medium text-fg-2 transition-colors duration-150 hover:bg-hover hover:text-fg",
          open && "bg-hover text-fg",
          className,
        )}
      >
        {icon}
        {showLabel && label}
        <ChevronDown className="size-3 text-fg-3" aria-hidden />
      </button>
      {open && (
        <div
          ref={panel}
          role="menu"
          aria-label={label}
          onKeyDown={onKey}
          className="fixed z-50 min-w-52 rounded-md border border-line-strong bg-raised p-1 shadow-pop"
          style={pos ? { top: pos.top, left: pos.left } : { top: 0, left: 0, visibility: "hidden" }}
        >
          {items.map((it) => (
            <button
              key={it.label}
              type="button"
              role="menuitem"
              disabled={it.disabled}
              onClick={() => {
                setOpen(false);
                it.onSelect();
              }}
              className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-left text-sm hover:bg-hover focus-visible:bg-hover focus-visible:outline-none disabled:pointer-events-none disabled:opacity-40"
            >
              {it.icon && <span className="text-fg-3">{it.icon}</span>}
              <span className="flex-1">
                {it.label}
                {it.hint && <span className="block text-xs text-fg-3">{it.hint}</span>}
              </span>
            </button>
          ))}
        </div>
      )}
    </>
  );
}
