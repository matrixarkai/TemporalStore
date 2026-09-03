/* Run the Setup page's byteHint against real settings shapes and report what it produced.
 *
 * Grepping the built page proves the function is present, not that it fires: a hint appended to a
 * variable nothing renders, or a pattern that never matches the variable names actually shipped,
 * both leave the same source text in the file. byteHint is pure, so it is extracted and called --
 * the thing under test is what it returns for a given field.
 *
 * Cases are the settings the portal really offers, including the two quota knobs that predate the
 * engine group, so a pattern matching only TS_* would be caught here.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join("\n");

const start = scripts.indexOf("function byteHint(");
if (start < 0) {
  console.log("FAIL byteHint is not in the built page");
  process.exit(1);
}
let depth = 0;
let end = -1;
for (let i = scripts.indexOf("{", start); i < scripts.length; i += 1) {
  if (scripts[i] === "{") {
    depth += 1;
  } else if (scripts[i] === "}") {
    depth -= 1;
    if (depth === 0) { end = i + 1; break; }
  }
}
if (end < 0) {
  console.log("FAIL could not brace-match byteHint");
  process.exit(1);
}
const byteHint = new Function(scripts.slice(start, end) + "; return byteHint;")();

let failures = 0;

const cases = [
  { env: "TS_BLOCK_SEGMENT_TARGET_BYTES", value: "1073741824", expect: "1 GiB" },
  { env: "TS_COMPACTION_WATERMARK_BYTES", value: "268435456", expect: "256 MiB" },
  { env: "TS_CONTEXT_PAGE_TARGET_BYTES", value: "65536", expect: "64 KiB" },
  { env: "TS_INDEX_DUMP_WAL_GAP_BYTES", value: "1048576", expect: "1 MiB" },
  { env: "MATRIXARK_QUOTA_MAX_BLOB_BYTES", value: "5368709120", expect: "5 GiB" },
  { env: "MATRIXARK_QUOTA_MAX_BODY_BYTES", value: "16777216", expect: "16 MiB" },
  { env: "TS_PAGE_INDEX_CACHE_BYTES", value: "", default: "67108864", expect: "64 MiB" },
  { env: "TS_STREAM_MAX_BLOB_SIZE_BYTES", value: "1500000", expect: "1.4 MiB" }
];

for (const c of cases) {
  const out = byteHint({ env: c.env, value: c.value, default: c.default });
  if (!out.includes(c.expect)) {
    console.log("FAIL " + c.env + ": expected " + c.expect + ", got " + JSON.stringify(out));
    failures += 1;
  } else {
    console.log("ok   " + c.env + " -> " + c.expect);
  }
}

for (const env of ["TS_VECTOR_SCALED", "TS_MAX_RETAINED_FINISHED_JOBS", "MATRIXARK_HTTP_PORT"]) {
  const out = byteHint({ env: env, value: "1024" });
  if (out !== "") {
    console.log("FAIL " + env + " is not a byte count and gained " + JSON.stringify(out));
    failures += 1;
  } else {
    console.log("ok   " + env + " -> no hint");
  }
}

for (const bad of ["", "auto", "0", "-1"]) {
  const out = byteHint({ env: "TS_PAGE_INDEX_CACHE_BYTES", value: bad, default: "" });
  if (out !== "") {
    console.log("FAIL unreadable value " + JSON.stringify(bad) + " produced " + JSON.stringify(out));
    failures += 1;
  }
}

/* Wiring. Everything above would still pass if nothing called byteHint. This one is a SOURCE check
 * rather than a behavioural one, and worth naming as such: fieldHtml builds its help text from
 * several pieces and closes over page state, so it cannot be lifted out and called the way byteHint
 * can. What is verified here is that the help construction references byteHint at all. */
const NEEDLE = "var help = esc(f.help)";
const at = scripts.indexOf(NEEDLE);
if (at < 0) {
  console.log("FAIL the help construction is gone; the hint cannot reach a field");
  failures += 1;
} else if (!scripts.slice(at, at + 300).includes("byteHint(f)")) {
  console.log("FAIL byteHint is defined but the help text never calls it");
  failures += 1;
} else {
  console.log("ok   help text calls byteHint (source check)");
}

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
