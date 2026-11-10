import { memo, type CSSProperties, type ReactNode } from "react";

interface Style {
  fg: number | string | null;
  bg: string | null;
  bold: boolean;
  dim: boolean;
  italic: boolean;
  underline: boolean;
}

const RESET: Style = { fg: null, bg: null, bold: false, dim: false, italic: false, underline: false };

function xterm256(n: number): string {
  if (n < 16) return "";
  if (n >= 232) {
    const v = 8 + (n - 232) * 10;
    return `rgb(${v},${v},${v})`;
  }
  const i = n - 16;
  const c = (x: number) => (x === 0 ? 0 : 55 + x * 40);
  return `rgb(${c(Math.floor(i / 36))},${c(Math.floor(i / 6) % 6)},${c(i % 6)})`;
}

function applySgr(style: Style, params: number[]): Style {
  const s = { ...style };
  for (let i = 0; i < params.length; i++) {
    const p = params[i]!;
    if (p === 0) Object.assign(s, RESET);
    else if (p === 1) s.bold = true;
    else if (p === 2) s.dim = true;
    else if (p === 3) s.italic = true;
    else if (p === 4) s.underline = true;
    else if (p === 22) (s.bold = false), (s.dim = false);
    else if (p === 23) s.italic = false;
    else if (p === 24) s.underline = false;
    else if (p >= 30 && p <= 37) s.fg = p - 30;
    else if (p >= 90 && p <= 97) s.fg = p - 90 + 8;
    else if (p === 39) s.fg = null;
    else if (p === 49) s.bg = null;
    else if (p === 38 || p === 48) {
      let color: number | string | null = null;
      if (params[i + 1] === 5) {
        const n = params[i + 2] ?? 0;
        color = n < 16 ? n : xterm256(n);
        i += 2;
      } else if (params[i + 1] === 2) {
        color = `rgb(${params[i + 2] ?? 0},${params[i + 3] ?? 0},${params[i + 4] ?? 0})`;
        i += 4;
      }
      if (p === 38) s.fg = color;
      else s.bg = typeof color === "string" ? color : null;
    }
  }
  return s;
}

const CSI = /\u001b\[([0-9;]*)([A-Za-z])/g;

/** Collapses carriage-return overwrites (progress bars) to what a terminal would show. */
export function applyCarriageReturns(text: string): string {
  if (!text.includes("\r")) return text;
  return text
    .split("\n")
    .map((line) => {
      const parts = line.split("\r");
      for (let i = parts.length - 1; i >= 0; i--) if (parts[i]) return parts[i]!;
      return "";
    })
    .join("\n");
}

export const stripAnsi = (text: string) => text.replace(/\u001b\[[0-9;]*[A-Za-z]/g, "");

function span(text: string, s: Style, key: number): ReactNode {
  if (s === RESET || (s.fg === null && !s.bg && !s.bold && !s.dim && !s.italic && !s.underline)) return text;
  const cls: string[] = [];
  const css: CSSProperties = {};
  if (typeof s.fg === "number") cls.push(`ansi-fg-${s.fg}`);
  else if (s.fg) {
    cls.push("ansi-rgb");
    (css as Record<string, string>)["--ansi-c"] = s.fg;
  }
  if (s.bg) css.backgroundColor = s.bg;
  if (s.bold) cls.push("ansi-bold");
  if (s.dim) cls.push("ansi-dim");
  if (s.italic) cls.push("ansi-italic");
  if (s.underline) cls.push("ansi-underline");
  return (
    <span key={key} className={cls.join(" ") || undefined} style={Object.keys(css).length ? css : undefined}>
      {text}
    </span>
  );
}

export function ansiToNodes(input: string): ReactNode[] {
  const text = applyCarriageReturns(input);
  const out: ReactNode[] = [];
  let style = RESET;
  let last = 0;
  let key = 0;
  CSI.lastIndex = 0;
  for (let m = CSI.exec(text); m; m = CSI.exec(text)) {
    if (m.index > last) out.push(span(text.slice(last, m.index), style, key++));
    if (m[2] === "m") style = applySgr(style, m[1] ? m[1].split(";").map(Number) : [0]);
    last = CSI.lastIndex;
  }
  if (last < text.length) out.push(span(text.slice(last), style, key++));
  return out;
}

export const Ansi = memo(function Ansi({ text }: { text: string }) {
  return <>{ansiToNodes(text)}</>;
});
