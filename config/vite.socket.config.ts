import { resolve } from "node:path";

import { defineConfig } from "vite";

export default defineConfig({
  root: resolve(__dirname, "../src"),
  base: "./",
  build: {
    outDir: "../dist",
    emptyOutDir: true,
    copyPublicDir: false,
    target: "esnext",
    minify: "terser",
    terserOptions: {
      toplevel: true,
      compress: {
        dead_code: true,
      },
      format: {
        comments: false,
      },
    },
    lib: {
      name: "etpSocket",
      entry: "index.ts",
      formats: ["es", "cjs"],
      fileName: "index",
    },
  },
});
