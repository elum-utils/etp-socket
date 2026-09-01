import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const env = { ...process.env, GOCACHE: process.env.GOCACHE ?? "/tmp/etp-socket-go-cache" };
const commands = [
  spawn("go", ["run", "."], { cwd: resolve(root, "examples/browser/server"), env, stdio: "inherit" }),
  spawn(resolve(root, "node_modules/.bin/vite"), ["--config", "examples/browser/vite.config.ts"], {
    cwd: root,
    stdio: "inherit",
  }),
];

let stopping = false;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of commands) child.kill("SIGTERM");
  setTimeout(() => process.exit(code), 100).unref();
}

for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => stop());
for (const child of commands) child.on("exit", (code, signal) => {
  if (!stopping && (code || signal)) stop(code ?? 1);
});
