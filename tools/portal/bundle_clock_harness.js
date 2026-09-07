// Drives the SHIPPED stream, then asks the SHIPPED bundle() what it wrote.
//
// Separate from bundle_harness.js, which drives the WHOLE overview page and reports its
// own ok/FAIL lines. This one returns the bundle as JSON so a caller can assert on
// individual fields, and it puts a live stream in front of the call because the fields it
// exists to check are the ones that come from the stream's clock. The bundle is the file
// that leaves the building, so what matters is the JSON it produces, not the expression that
// produces it.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

function sliceFn(marker) {
  const start = page.indexOf(marker);
  if (start < 0) throw new Error("not found: " + marker);
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + marker);
}

const ORIGIN = 1_760_000_000_000;
const clockStart = page.indexOf("var liveServerMs = 0;");
const source = page.slice(clockStart, page.indexOf("function liveStream(options)"))
  + "\n" + sliceFn("function liveStream(options)")
  + "\nvar lastFrame = null;\n" + sliceFn("  function bundle()");

let clock = ORIGIN;
let ticker = null;

const opening = ["retry: 3000\n\n"];
if (input.serverTs) {
  // The cadence rides on the frame, the way the gateway sends it.
  opening.push("event: status\ndata: " + JSON.stringify({
    ts: input.serverTs,
    tick_s: 2,
    traffic: { recent_failures: [{ at: input.serverTs - 90, status: 500 }] },
    datanode: "ok",
  }) + "\n\n");
}

const scope = {
  Date: function () {},
  fetch: (url) => {
    if (String(url).indexOf("/v1/admin/events") === 0) {
      return Promise.resolve({
        ok: true,
        body: {
          getReader: () => {
            let sent = false;
            return {
              read: () => sent ? new Promise(() => {})
                : ((sent = true), Promise.resolve({ done: false, value: opening.join("") })),
            };
          },
        },
      });
    }
    if (String(url).indexOf("/v1/metrics") === 0) {
      return Promise.resolve({ ok: true, text: () => Promise.resolve("# metrics\n") });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({ ok: true }) });
  },
  TextDecoder: function () { this.decode = (v) => (v === undefined ? "" : String(v)); },
  AbortController: function () { this.abort = () => {}; this.signal = null; },
  document: { hidden: false, addEventListener: () => {} },
  window: { addEventListener: () => {} },
  setTimeout: () => 0,
  setInterval: (fn) => { ticker = fn; return 1; },
  clearInterval: () => { ticker = null; },
  location: { origin: "https://gw.example" },
  auth: () => ({}),
  lastReport: { checks: [] },
  compactConfig: (c) => c,
  JSON: JSON,
  Promise: Promise,
};
// `Date` must be a constructor (the bundle calls `new Date(ms).toISOString()`) AND carry `now`.
scope.Date = function (ms) { return new globalThis.Date(ms === undefined ? clock : ms); };
scope.Date.now = () => clock;

const names = Object.keys(scope);
const api = new Function(...names, source + `
  return {
    start: function () {
      return liveStream({ headers: function () { return {}; }, onState: function () {},
                          onFrame: function (f) { lastFrame = f; } });
    },
    bundle: bundle
  };
`)(...names.map((k) => scope[k]));

const wait = () => new Promise((r) => globalThis.setTimeout(r, 5));

(async () => {
  if (input.withStream !== false) {
    api.start();
    await wait();
  }
  clock += (input.advanceMs || 0);
  const text = await api.bundle();
  process.stdout.write(JSON.stringify({ bundle: JSON.parse(text) }));
})();
