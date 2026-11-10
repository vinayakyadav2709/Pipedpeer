import { useEffect, useState } from "react";

export type ThemeChoice = "system" | "light" | "dark";
export type Theme = "light" | "dark";

const KEY = "pp-theme";
const media = matchMedia("(prefers-color-scheme: dark)");

function stored(): ThemeChoice {
  try {
    const v = localStorage.getItem(KEY);
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
}

const resolve = (c: ThemeChoice): Theme => (c === "system" ? (media.matches ? "dark" : "light") : c);

export function useTheme() {
  const [choice, setChoice] = useState<ThemeChoice>(stored);
  const [theme, setTheme] = useState<Theme>(() => resolve(stored()));

  useEffect(() => {
    const apply = () => {
      const t = resolve(choice);
      document.documentElement.dataset.theme = t;
      setTheme(t);
    };
    apply();
    try {
      if (choice === "system") localStorage.removeItem(KEY);
      else localStorage.setItem(KEY, choice);
    } catch {
      /* storage unavailable: the choice lasts for this page only */
    }
    if (choice !== "system") return;
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, [choice]);

  return { choice, theme, setChoice };
}

/** Current resolved theme for components that must restyle non-CSS surfaces (sandboxed frames). */
export function useResolvedTheme(): Theme {
  const [theme, setTheme] = useState<Theme>(() => (document.documentElement.dataset.theme === "dark" ? "dark" : "light"));
  useEffect(() => {
    const obs = new MutationObserver(() => setTheme(document.documentElement.dataset.theme === "dark" ? "dark" : "light"));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);
  return theme;
}
