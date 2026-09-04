/* Run the shared "why was this refused" helper against the shapes the edge actually answers with.
 *
 * The interesting cases are not readable from the source: which half wins when both `detail` and
 * `error` are present, and whether the scope list is appended to that or quietly replaces it. The
 * edge sends `detail` for a rejected value ("extraction.provider must be one of ...") and
 * `required` for a refused scope, and a message that shows one and drops the other is how an
 * operator ends up guessing at something the response already answered.
 *
 * Usage: node refusal_message_harness.js <page.html> [more pages...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* Read out of a built page rather than the generator: this is the copy a browser runs. */
function helperFrom(file) {
  const page = fs.readFileSync(file, "utf8");
  const marker = "window.__matrixarkWhy = function (body, fallback) {";
  const start = page.indexOf(marker);
  if (start < 0) { return null; }
  const end = page.indexOf("\n  };", start);
  const win = { window: null };
  win.window = win;
  new Function("window", page.slice(start, end + 5))(win);
  return win.__matrixarkWhy;
}

const pages = process.argv.slice(2);
ok("pages were given", pages.length > 0, "none");

let carried = 0;
for (const file of pages) {
  const name = path.basename(file);
  const why = helperFrom(file);
  if (why === null) {
    console.log("FAIL " + name + " does not carry the shared refusal helper");
    failures += 1;
    continue;
  }
  carried += 1;

  ok(name + ": a scope refusal names the scope",
     /admin:api_key/.test(why({ error: "insufficient_scope", required: ["admin:api_key"] }, "no")),
     why({ error: "insufficient_scope", required: ["admin:api_key"] }, "no"));

  ok(name + ": and still says what was refused",
     /insufficient_scope/.test(why({ error: "insufficient_scope", required: ["admin:api_key"] },
                                   "no")));

  /* A rejected VALUE carries detail and no required: the detail is the whole answer. */
  const value = why({ error: "invalid_value", detail: "extraction.provider must be one of x, y" },
                    "Save failed.");
  ok(name + ": a rejected value shows the reason, not the code",
     /must be one of/.test(value) && !/^invalid_value/.test(value), value);

  /* Both halves present: neither may swallow the other. */
  const both = why({ error: "insufficient_scope", detail: "writing settings needs a write scope",
                     required: ["admin:api_key"] }, "no");
  ok(name + ": detail and scope both survive",
     /write scope/.test(both) && /admin:api_key/.test(both), both);

  ok(name + ": more than one acceptable scope is listed",
     /admin:api_key or admin:audit/.test(
       why({ error: "insufficient_scope", required: ["admin:api_key", "admin:audit"] }, "no")));

  ok(name + ": an empty body falls back", why({}, "Save failed.") === "Save failed.");
  ok(name + ": a missing body falls back", why(null, "Save failed.") === "Save failed.");
  ok(name + ": nothing is appended when nothing was required",
     why({ error: "boom" }, "no") === "boom");
}

ok("every page given carries the helper", carried === pages.length,
   carried + " of " + pages.length);

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
