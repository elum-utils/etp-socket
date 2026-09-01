import dts from "rollup-plugin-dts";
import type { RollupOptions } from "rollup";

const config: RollupOptions = {
  input: "src/index.ts",
  output: {
    file: "dist/index.d.ts",
    format: "es",
  },
  plugins: [
    dts(),
  ],
};

export default config;
