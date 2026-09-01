import { resolve } from "node:path";

import { defineConfig } from "vite";

export default defineConfig({
  root: resolve(__dirname),
  resolve: {
    alias: {
      "@elum/etp-socket": resolve(__dirname, "../../src/index.ts"),
    },
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
  },
});
