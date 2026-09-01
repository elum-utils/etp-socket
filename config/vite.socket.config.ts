import { resolve } from "node:path";

import copy from "rollup-plugin-copy";
import { defineConfig } from "vite";

export default defineConfig({
  root: resolve(__dirname, "../src"),
  base: "./",
  build: {
    outDir: "../dist",
    emptyOutDir: false,
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
  plugins: [
    copy({
      targets: [
        { src: "LICENSE", dest: "dist" },
        { src: "README.md", dest: "dist" },
        { src: "src/package.json", dest: "dist", rename: "package.json" },
      ],
    }),
  ],
});
