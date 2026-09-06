/* Run the setup page's fieldHtml and read the badges it produces.
 *
 * Whether the two badges are mutually exclusive cannot be read off the source with any confidence:
 * "needs restart" describes the KIND of setting and is true from page load, and "changed — restart
 * to apply" describes THIS deployment right now. Showing both says the same thing twice and buries
 * the half that is about today, so the second has to replace the first rather than join it.
 *
 * Usage: node setting_badge_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* Read out of the built page rather than the generator: this is the copy a browser runs. */
const start = page.indexOf("function fieldHtml(f) {");
if (start < 0) { console.log("FAIL fieldHtml is not on the page"); process.exit(1); }
const end = page.indexOf("\n  }", start);
const body = page.slice(start, end + 4);

const esc = (s) => String(s === undefined || s === null ? "" : s);

/* The renderer reaches for several small helpers from the page around it, and naming them one by
   one turns each into a round of "run it, read the ReferenceError, add a stub". Anything it asks
   for that is not a real global answers with an inert function instead: this is about which badges
   come out, and helper text would only be noise to match against. */
const scope = new Proxy({}, {
  has: () => true,
  get: (_target, key) => {
    if (key === Symbol.unscopables) { return undefined; }
    if (key === "esc") { return esc; }
    if (key in globalThis) { return globalThis[key]; }
    return () => "";
  }
});
const fieldHtml = new Function("scope", "with (scope) { " + body + "\nreturn fieldHtml; }")(scope);

/* A field carrying every property the renderer reads. A thinner one would throw somewhere in the
   middle and the throw would look like a missing badge. */
function field(extra) {
  return Object.assign({
    key: "extraction.base_url", env: "MATRIXARK_EXTRACTION_BASE_URL",
    label: "Extraction base URL", help: "Where extraction calls go.",
    value: "https://api.deepseek.com/v1", default: "", source: "config",
    applies: "restart", essential: false, secret: false, configured: true,
    overridable_by: [], boot_pinned: false, pending_restart: false, options: null,
    unit: null, minimum: null, maximum: null, group: "Extraction"
  }, extra);
}

const advice = /needs restart/;
const status = /changed — restart to apply/;

let html = fieldHtml(field({}));
ok("an untouched restart setting carries the advice", advice.test(html));
ok("and does not claim to be waiting", !status.test(html), html.slice(0, 200));

html = fieldHtml(field({ pending_restart: true }));
ok("a written one says it is waiting", status.test(html), html.slice(0, 240));
ok("and drops the advice, rather than showing both", !advice.test(html), html.slice(0, 240));

html = fieldHtml(field({ applies: "live", pending_restart: false }));
ok("a live setting carries neither", !advice.test(html) && !status.test(html),
   html.slice(0, 200));

/* The one that would make this a decoration: a live setting can never be pending, so if the
   renderer ever showed the status badge for one, the flag would be meaningless. */
html = fieldHtml(field({ applies: "live", pending_restart: true }));
ok("the renderer shows what it is given, so the flag is the thing to get right",
   status.test(html), html.slice(0, 200));

/* Offered, stored, resolved -- and read by nothing in this build. Without the badge the field is
   indistinguishable from one that works: it takes a value, confirms it, and the deployment
   behaves the same either way. */
const unread = /not read by this build/;

html = fieldHtml(field({ key: "behaviour.summary_levels", env: "MATRIXARK_SUMMARY_LEVELS",
                         applies: "live", read_by_nothing: true }));
ok("a control nothing reads says so", unread.test(html), html.slice(0, 300));
ok("and it is styled as something to look at", /badge failed/.test(html), html.slice(0, 300));
ok("and the field is still drawn, so a stored value stays visible",
   /MATRIXARK_SUMMARY_LEVELS/.test(html), html.slice(0, 300));

/* The floor. Every other field must NOT carry it, or the badge says nothing. */
ok("FLOOR: an ordinary setting does not carry it", !unread.test(fieldHtml(field({}))));
ok("FLOOR: nor does a live one", !unread.test(fieldHtml(field({ applies: "live" }))));
ok("FLOOR: nor one merely pending a restart",
   !unread.test(fieldHtml(field({ pending_restart: true }))));

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
