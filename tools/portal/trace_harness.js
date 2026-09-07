/* Run the page's OWN call recorder against a fetch that answers, one that refuses, and one that
 * never answers at all.
 *
 * The shared block is executed rather than copied: the recorder replaces window.fetch, and the one
 * property that matters -- that it hands the response back untouched -- cannot be checked against
 * a copy of the code.
 *
 * Usage: node trace_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const at = page.indexOf("Helpers every page may call");
if (at < 0) { console.log(JSON.stringify({ error: "no shared helper block" })); process.exit(2); }
const from = page.lastIndexOf("<script>", at) + "<script>".length;
const to = page.indexOf("</script>", from);

/* A window with the fetch the page would have had. Bodies are real objects so "was it consumed"
   is a question this harness can actually ask. */
function response(status, body) {
  let used = false;
  return {
    status: status,
    ok: status >= 200 && status < 300,
    text: function () { if (used) { return Promise.reject(new Error("body already used")); }
                        used = true; return Promise.resolve(body); },
    wasUsed: function () { return used; }
  };
}

const answers = {
  "/ok": () => Promise.resolve(response(200, "{\"fine\":true}")),
  "/refused": () => Promise.resolve(response(403, "{\"error\":\"forbidden\"}")),
  "/gone": () => Promise.reject(new TypeError("Failed to fetch"))
};

const win = {
  fetch: function (input, init) {
    const url = String((input && input.url) || input || "");
    const make = answers[url];
    return make ? make() : Promise.resolve(response(404, "{}"));
  }
};
const original = win.fetch;

new Function("window", page.slice(from, to))(win);

const changes = [];
win.__matrixarkTrace.onChange(function (n) { changes.push(n); });

(async () => {
  const okResponse = await win.fetch("/ok");
  /* The whole point: the caller still gets a body. A recorder that read it to pull out an
     incident token would hand back an empty one and break every panel on the portal. */
  const bodyAfterRecording = await okResponse.text();

  await win.fetch("/refused");
  let refusedError = "";
  try { await win.fetch("/gone"); } catch (e) { refusedError = e.message; }

  /* Read the three before flooding the ring, or the assertions below describe whatever survived
     the flood rather than the three calls they name. */
  const three = win.__matrixarkTrace.calls();

  /* The ring is bounded: a portal tab is left open for days. */
  for (let i = 0; i < 80; i++) { await win.fetch("/ok"); }

  const calls = win.__matrixarkTrace.calls();
  console.log(JSON.stringify({
    wrapped: win.fetch !== original,
    bodyAfterRecording: bodyAfterRecording,
    bodyWasNotConsumedByTheRecorder: bodyAfterRecording === "{\"fine\":true}",
    refusedError: refusedError,
    statuses: three.map((c) => c.status),
    methods: three.map((c) => c.method),
    urls: three.map((c) => c.url),
    detailOnTheOneThatNeverAnswered: (three[2] || {}).detail || "",
    bounded: calls.length,
    listenerFired: changes.length,
    everyEntryHasATime: calls.every((c) => typeof c.at === "number" && c.at > 0),
    everyEntryHasADuration: calls.every((c) => typeof c.ms === "number" && c.ms >= 0)
  }, null, 1));
})();
