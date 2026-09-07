/* Run the page's OWN failure classifier against the three things that reach a catch.
 *
 * The shared helper block is executed, not stubbed: the whole point of the change is what the
 * shipped classifier answers, and a copy of it here could drift from the page without a word.
 *
 * Usage: node why_failed_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

const sharedAt = page.indexOf("Helpers every page may call");
if (sharedAt < 0) { console.log(JSON.stringify({ error: "no shared helper block" })); process.exit(2); }
const from = page.lastIndexOf("<script>", sharedAt) + "<script>".length;
const to = page.indexOf("</script>", from);

const win = {};
new Function("window", page.slice(from, to))(win);

/* A page's own failure() may add its own wording for some statuses; the browse path passes one.
   Answered here by the shared one, which is what a page without overrides uses. */
const cases = {
  a_status_the_deployment_answered: win.__matrixarkWhyFailed(503),
  a_status_with_an_override: win.__matrixarkWhyFailed(413, function () {
    return "That file is larger than this deployment accepts.";
  }),
  the_request_never_left: win.__matrixarkWhyFailed(new TypeError("Failed to fetch")),
  the_request_never_left_firefox: win.__matrixarkWhyFailed(
    new TypeError("NetworkError when attempting to fetch resource.")),
  the_request_never_left_safari: win.__matrixarkWhyFailed(new TypeError("Load failed")),
  nothing_at_all: win.__matrixarkWhyFailed(undefined),
  /* The one that used to be indistinguishable, and the exact throw that caused it. */
  the_page_threw_while_showing: win.__matrixarkWhyFailed(
    new TypeError("window.__matrixarkWhen is not a function")),
  a_bad_body: win.__matrixarkWhyFailed(new SyntaxError("Unexpected token < in JSON at position 0"))
};

const arrived = {
  a_status: win.__matrixarkNeverArrived(503),
  fetch_failure: win.__matrixarkNeverArrived(new TypeError("Failed to fetch")),
  nothing_at_all: win.__matrixarkNeverArrived(undefined),
  a_render_throw: win.__matrixarkNeverArrived(
    new TypeError("window.__matrixarkWhen is not a function"))
};

console.log(JSON.stringify({ said: cases, neverArrived: arrived }, null, 1));
