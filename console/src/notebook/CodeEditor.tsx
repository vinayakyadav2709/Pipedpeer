import { autocompletion, closeBrackets, closeBracketsKeymap, closeCompletion, completionKeymap, completionStatus, startCompletion, type CompletionContext, type CompletionResult } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap, indentLess, indentMore, insertTab } from "@codemirror/commands";
import { python } from "@codemirror/lang-python";
import { bracketMatching, HighlightStyle, indentOnInput, indentUnit, syntaxHighlighting } from "@codemirror/language";
import { highlightSelectionMatches, search, searchKeymap } from "@codemirror/search";
import { EditorState, Prec } from "@codemirror/state";
import { drawSelection, EditorView, keymap, placeholder as placeholderExt } from "@codemirror/view";
import { tags as t } from "@lezer/highlight";
import { useEffect, useRef } from "react";
import type { Completion } from "./kernel";

export type RunMode = "advance" | "stay" | "insert";

export interface CodeEditorProps {
  value: string;
  /** Changes when `value` was replaced from outside, so the editor re-reads it. */
  epoch: number;
  language: "python" | "text";
  focused: boolean;
  placeholder?: string;
  ariaLabel: string;
  onChange: (value: string) => void;
  onFocus: () => void;
  onRun: (mode: RunMode) => void;
  onEscape: () => void;
  /** Arrow key pressed at the first or last line: move to the neighbouring cell. */
  onLeave: (direction: -1 | 1) => void;
  complete?: (code: string, cursor: number) => Promise<Completion | null>;
}

const highlight = HighlightStyle.define([
  { tag: [t.keyword, t.controlKeyword, t.operatorKeyword, t.definitionKeyword, t.moduleKeyword], color: "var(--syn-keyword)" },
  { tag: [t.string, t.special(t.string), t.docString], color: "var(--syn-string)" },
  { tag: [t.number, t.bool, t.null], color: "var(--syn-number)" },
  { tag: [t.comment, t.lineComment], color: "var(--syn-comment)", fontStyle: "italic" },
  { tag: [t.function(t.definition(t.variableName)), t.definition(t.className), t.className], color: "var(--syn-def)" },
  { tag: [t.function(t.variableName), t.standard(t.variableName), t.self], color: "var(--syn-builtin)" },
  { tag: [t.operator, t.punctuation, t.derefOperator], color: "var(--syn-op)" },
  { tag: t.meta, color: "var(--syn-def)" },
]);

export function CodeEditor(props: CodeEditorProps) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  const live = useRef(props);
  live.current = props;

  useEffect(() => {
    if (!host.current) return;

    const kernelCompletions = async (ctx: CompletionContext): Promise<CompletionResult | null> => {
      const complete = live.current.complete;
      if (!complete) return null;
      const word = ctx.matchBefore(/[\w.]+/);
      if (!ctx.explicit && (!word || (word.text.length < 2 && !word.text.endsWith(".")))) return null;
      const reply = await complete(ctx.state.doc.toString(), ctx.pos);
      if (ctx.aborted || !reply || !reply.matches.length) return null;
      return { from: reply.from, to: reply.to, options: reply.matches.map((label) => ({ label })), validFor: /^\w*$/ };
    };

    // CodeMirror announces focus on a short timer. Leaving the editor blurs it first, so an announcement
    // that is still pending cannot land after the cell has gone back to command mode and undo that.
    const exit = (v: EditorView, then: () => void) => {
      v.contentDOM.blur();
      then();
      return true;
    };
    const run = (mode: RunMode) => (v: EditorView) => exit(v, () => live.current.onRun(mode));
    const leave = (direction: -1 | 1) => (v: EditorView) => {
      const sel = v.state.selection.main;
      if (!sel.empty) return false;
      const line = v.state.doc.lineAt(sel.head);
      if (direction < 0 ? line.number !== 1 : line.number !== v.state.doc.lines) return false;
      live.current.onLeave(direction);
      return true;
    };

    const state = EditorState.create({
      doc: live.current.value,
      extensions: [
        Prec.highest(
          keymap.of([
            { key: "Shift-Enter", run: run("advance") },
            { key: "Mod-Enter", run: run("stay") },
            { key: "Alt-Enter", run: run("insert") },
          ]),
        ),
        history(),
        drawSelection(),
        indentUnit.of("    "),
        EditorState.tabSize.of(4),
        indentOnInput(),
        bracketMatching(),
        closeBrackets(),
        highlightSelectionMatches(),
        search({ top: false }),
        syntaxHighlighting(highlight),
        EditorView.lineWrapping,
        EditorView.contentAttributes.of({ "aria-label": live.current.ariaLabel, spellcheck: "false", autocapitalize: "off" }),
        ...(live.current.language === "python" ? [python(), autocompletion({ override: [kernelCompletions], icons: false, defaultKeymap: false })] : []),
        ...(live.current.placeholder ? [placeholderExt(live.current.placeholder)] : []),
        keymap.of([
          {
            // Ahead of the completion keymap: that one also swallows Escape for a query that is still
            // pending with no popup on screen, which would make the first Escape after typing do nothing.
            key: "Escape",
            run: (v) => {
              if (completionStatus(v.state) === "active") return closeCompletion(v);
              closeCompletion(v);
              return exit(v, () => live.current.onEscape());
            },
          },
          ...closeBracketsKeymap,
          ...completionKeymap,
          ...searchKeymap,
          {
            key: "Tab",
            run: (v) => {
              const sel = v.state.selection.main;
              if (!sel.empty) return indentMore(v);
              const before = v.state.doc.sliceString(v.state.doc.lineAt(sel.head).from, sel.head);
              if (live.current.language === "python" && /[\w.]$/.test(before)) return startCompletion(v);
              return insertTab(v);
            },
            shift: indentLess,
          },
          { key: "ArrowUp", run: leave(-1) },
          { key: "ArrowDown", run: leave(1) },
          ...historyKeymap,
          ...defaultKeymap,
        ]),
        EditorView.updateListener.of((u) => {
          if (u.docChanged) live.current.onChange(u.state.doc.toString());
          if (u.focusChanged && u.view.hasFocus) live.current.onFocus();
        }),
      ],
    });
    const v = new EditorView({ state, parent: host.current });
    view.current = v;
    return () => {
      v.destroy();
      view.current = null;
    };
  }, [props.language]);

  useEffect(() => {
    const v = view.current;
    if (!v || v.state.doc.toString() === live.current.value) return;
    v.dispatch({ changes: { from: 0, to: v.state.doc.length, insert: live.current.value } });
  }, [props.epoch]);

  useEffect(() => {
    const v = view.current;
    if (!v) return;
    if (props.focused && !v.hasFocus) v.focus();
    else if (!props.focused && v.hasFocus) v.contentDOM.blur();
  }, [props.focused]);

  return <div ref={host} className="min-w-0" />;
}
