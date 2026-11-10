import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const backend = process.env.PIPEDPEER_CONSOLE_BACKEND ?? "http://127.0.0.1:7878";

export default defineConfig({
  base: "/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@": path.resolve(import.meta.dirname, "./src") },
  },
  build: {
    outDir: path.resolve(import.meta.dirname, "../src/internal/console/web/dist"),
    emptyOutDir: true,
    target: "es2023",
    // The lazy notebook chunk carries CodeMirror; it is expected to be the big one.
    chunkSizeWarningLimit: 700,
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": { target: backend, ws: true, changeOrigin: false },
    },
  },
});
