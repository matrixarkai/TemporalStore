/* Run the shipped probeHtml and read what it says about the model that answered.
 *
 * A call that succeeded with the WRONG model is the failure this panel is least able to show: it
 * answered, it was fast, and the vectors are unusable. So the function is run rather than read.
 *
 * Usage: node probe_model_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("function probeHtml(r)");
if (start < 0) { console.log("probeHtml is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const probeHtml = new Function("esc", page.slice(start, end) + "; return probeHtml;")(esc);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const base = { target: "embedding", ok: true, latency_ms: 12, url: "http://127.0.0.1:8400/v1" };

const agreed = probeHtml(Object.assign({}, base, {
  response: { model: "all-MiniLM-L6-v2", dimensions: 384,
              requested_model: "all-MiniLM-L6-v2", model_matches_request: true },
}));
ok("a model that matches reads as ok", />ok</.test(agreed), agreed);
ok("and is not styled as a problem", !/class="probe bad"/.test(agreed), agreed);

const differs = probeHtml(Object.assign({}, base, {
  response: { model: "all-MiniLM-L6-v2", dimensions: 384,
              requested_model: "text-embedding-3-small", model_matches_request: false },
}));
ok("a different model is not reported as ok", !/>ok</.test(differs), differs);
ok("it is named as a different model", />different model</.test(differs), differs);
ok("and styled as something to look at", /class="probe bad"/.test(differs), differs);
ok("both names are shown, so it can be acted on",
   differs.indexOf("text-embedding-3-small") >= 0 && differs.indexOf("all-MiniLM-L6-v2") >= 0,
   differs);
ok("and it says why a same-width answer is still wrong",
   /same width/.test(differs), differs);

/* The false-alarm floor. A provider that returns no model name has not disagreed with anything,
   and a warning on every terse endpoint is one nobody reads. */
const terse = probeHtml(Object.assign({}, base, { target: "extraction",
  response: { model: "gpt-4o-mini", sample: "hi" } }));
ok("FLOOR: no comparison available is not a mismatch",
   />ok</.test(terse) && !/different model/.test(terse), terse);

const noModel = probeHtml(Object.assign({}, base, { response: { dimensions: 384 } }));
ok("FLOOR: a response naming no model is not a mismatch either",
   />ok</.test(noModel) && !/different model/.test(noModel), noModel);

/* A failure is still a failure, and must not be relabelled by any of the above. */
const broken = probeHtml({ target: "embedding", ok: false, error: "ConnectionRefused",
                           detail: "no listener", url: "http://127.0.0.1:8400/v1" });
ok("a real failure still reads as failed", />failed</.test(broken), broken);
ok("and a not-configured target still says so",
   />not configured</.test(probeHtml({ target: "embedding", skipped: true, detail: "no key" })),
   "skipped");

console.log(failures ? "\n" + failures + " failure(s)" : "\nall ok");
process.exit(failures ? 1 : 0);
