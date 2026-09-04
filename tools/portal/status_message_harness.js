/* One explanation per gateway status, and the page-specific words that survive.
 *
 * Three pages carried a status map of their own and each carried a different subset. Ingestion --
 * the page that uploads files -- was the one with no sentence for 413, so an upload refused for
 * size read as "The gateway answered 413." on the panel where the cause is obvious to everyone
 * except the reader. Four pages had no map at all.
 *
 * A map is a data structure and looks fine in source whatever it omits, so the page's own helper
 * is executed here and asked about each status.
 *
 * Usage: node status_message_harness.js <page.html> [...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(page, header) {
  const at = page.indexOf(header);
  if (at === -1) { return null; }
  let depth = 0;
  for (let i = page.indexOf("{", at); i < page.length; i += 1) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") {
      depth -= 1;
      if (depth === 0) { return page.slice(at, i + 1); }
    }
  }
  return null;
}

/* Statuses the edge actually answers with, and what a reader must not be left holding. */
const EXPLAINED = [401, 403, 413, 429, 502, 503, 504];

const files = process.argv.slice(2);
ok("pages were given", files.length > 0, "none");

let withOwn = 0;
for (const file of files) {
  const name = path.basename(file).replace("_portal.html", "");
  const page = fs.readFileSync(file, "utf8");

  const shared = slice(page, "window.__matrixarkFailure = function (status, overrides) {");
  ok(name + ": carries the shared status map", !!shared);
  if (!shared) { continue; }

  const win = {};
  new Function("window", shared + ";")(win);
  const say = win.__matrixarkFailure;

  const bare = EXPLAINED.filter((s) => /^The gateway answered/.test(say(s)));
  ok(name + ": every status the edge answers with has a sentence", bare.length === 0,
     "left as a bare number: " + bare.join(", "));

  ok(name + ": an unfamiliar status still names the number", /599/.test(say(599)), say(599));
  ok(name + ": an override wins where a page knows better",
     say(413, { 413: "a file thing" }) === "a file thing");
  ok(name + ": an override for another status does not leak",
     say(429, { 413: "a file thing" }) !== "a file thing");

  /* ---------- what the page itself still decides ---------- */
  const own = slice(page, "function failure(status) {");
  if (!own) { continue; }
  withOwn += 1;

  ok(name + ": its own failure() no longer holds a status map",
     !/if \(status === \d/.test(own), own.slice(0, 160));

  const local = new Function("window", own + "; return failure;")({ __matrixarkFailure: say });

  if (name === "ingestion" || name === "explore") {
    ok(name + ": an oversized upload is described as a file, not a request",
       /file/i.test(local(413)), local(413));
  }
  if (name === "ingestion") {
    ok(name + ": a running import is still its own answer", /import/i.test(local(409)), local(409));
  }
  if (name === "setup") {
    ok(name + ": a gateway that cannot reach storage is explained here too",
       /storage/i.test(local(502)), local(502));
  }
  const stillBare = EXPLAINED.filter((s) => /^The gateway answered/.test(local(s)));
  ok(name + ": nothing it answers is left as a bare number", stillBare.length === 0,
     stillBare.join(", "));
}

ok("at least three pages keep a failure() of their own", withOwn >= 3, String(withOwn));

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
