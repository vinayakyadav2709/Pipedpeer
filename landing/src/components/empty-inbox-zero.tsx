import { useEffect, useRef, useState } from "react";
import { CheckCheckIcon, CopyIcon, TerminalIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ParticleField, pulseParticleTypingImpulse } from "@/components/particle-field";
import dustyFieldSrc from "@/assets/figures/dusty-field.png";

const INSTALL_CMD =
  "curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly";

export function EmptyInboxZeroShowcasePage() {
  const [copied, setCopied] = useState(false);
  const typingImpulse = useRef(0);

  useEffect(() => {
    const id = setInterval(
      () => pulseParticleTypingImpulse(typingImpulse, 0.06),
      2500,
    );
    return () => clearInterval(id);
  }, []);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(INSTALL_CMD);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative h-dvh w-dvw overflow-hidden bg-background">
      <ParticleField
        src={dustyFieldSrc}
        sampleStep={2}
        threshold={48}
        dotSize={0.9}
        renderScale={1}
        align="center"
        typingImpulseRef={typingImpulse}
      />

      <div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{
          background:
            "radial-gradient(1000px 700px at 50% 45%, transparent 30%, color-mix(in srgb, var(--background) 85%, transparent) 100%)",
        }}
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-x-0 bottom-0 h-[60%]"
        style={{
          background:
            "linear-gradient(to bottom, transparent 0%, color-mix(in srgb, var(--background) 50%, transparent) 35%, color-mix(in srgb, var(--background) 90%, transparent) 70%, var(--background) 100%)",
        }}
      />

      <div className="absolute top-6 left-6 z-10 inline-flex items-center gap-2 rounded-full border border-border/70 bg-background/60 px-3 py-1 font-mono text-[10px] uppercase tracking-[0.3em] text-muted-foreground backdrop-blur">
        <TerminalIcon className="size-3 text-primary" aria-hidden />
        Pipedpeer
      </div>

      <div className="absolute inset-0 z-10 flex flex-col items-center justify-center gap-6 px-6 pb-20 text-center">
        <div className="font-mono text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
          peer-to-peer pipeline
        </div>

        <h1 className="font-sans text-5xl font-bold tracking-tight md:text-6xl">
          Pipedpeer
        </h1>

        <p className="max-w-md text-balance text-muted-foreground text-sm leading-relaxed">
          Pipe your data anywhere. Install in seconds.
        </p>

        <div className="mt-2 flex w-full max-w-2xl items-center gap-0 rounded-lg border border-border bg-card/80 backdrop-blur-sm">
          <code className="flex-1 whitespace-pre-wrap break-all px-4 py-3 text-left font-mono text-xs sm:text-sm text-foreground/80 select-all">
            {INSTALL_CMD}
          </code>
          <Button
            variant="ghost"
            size="icon"
            className="mr-1 shrink-0 h-9 w-9 text-muted-foreground hover:text-foreground"
            onClick={handleCopy}
            aria-label="Copy install command"
          >
            {copied ? (
              <CheckCheckIcon className="size-4 text-primary" />
            ) : (
              <CopyIcon className="size-4" />
            )}
          </Button>
        </div>
      </div>
    </div>
  );
}
