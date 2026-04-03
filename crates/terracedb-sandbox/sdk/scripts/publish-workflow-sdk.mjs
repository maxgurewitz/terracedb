import { readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const sdkDir = dirname(scriptDir);
const generatedDir = join(sdkDir, "generated");

const jsSource = await readFile(join(generatedDir, "workflow.js"), "utf8");
await writeFile(join(sdkDir, "workflow.js"), jsSource);

const dtsSource = await readFile(join(generatedDir, "workflow.d.ts"), "utf8");
const ambient = [
  'declare module "@terrace/workflow" {',
  dtsSource
    .trim()
    .split("\n")
    .map((line) => (line.length === 0 ? "" : `  ${line}`))
    .join("\n"),
  "}",
  "",
].join("\n");
await writeFile(join(sdkDir, "workflow.d.ts"), ambient);
