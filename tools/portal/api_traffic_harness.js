/* Run the API page and check each route row reports what that route has served.
 *
 * The page builds its rows as an HTML string, so a stub whose querySelectorAll returns nothing
 * would let every assertion pass without a single cell being painted. The routes container parses
 * what the page assigns to it and hands back real cells, which is the only way the painting is
 * actually exercised.
 *
 * Usage: node api_traffic_harness.js <api_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* Two documented routes sharing one counter, and one with a counter of its own. */
const ROUTES = [
  { group: "Memory", method: "GET", path: "/v1/memories", summary: "List memories.",
    scope: "read", metric: "/v1/memories" },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}", summary: "One memory.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}/history"] },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}/history", summary: "Its history.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}"] },
  { group: "Admin", method: "GET", path: "/v1/admin/scopes", summary: "Scopes.",
    scope: "admin", metric: "/v1/admin/scopes" },
];

const FRAME = {
  ts: 1, imports: {}, warnings: 0, embedding: { total: 1 },
  traffic: {
    total_requests: 9, total_errors: 2, in_flight: 0,
    routes: {
      "/v1/memories": { requests: 7, errors: 0, avg_ms: 4.5 },
      "/v1/memory/{id}": { requests: 2, errors: 2, avg_ms: 11 }
    }
  }
};

let cells = [];
function parseCells(html) {
  cells = [...String(html || "").matchAll(/<span class="route-live" data-metric="([^"]*)"([^>]*)>/g)]
    .map((m) => ({
      metric: m[1], attrs: m[2], textContent: "", className: "route-live",
      getAttribute(k) { return k === "data-metric" ? this.metric : null; }
    }));
}

function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = v; if (this.id === "routes") { parseCells(v); } },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}

const els = new Map();
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(),
    querySelectorAll: (sel) => (sel === ".route-live" ? cells : []),
    createElement: () => makeEl(), addEventListener() {}, body: makeEl(), hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", reload() {} },
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: { clipboard: { writeText() {} } },
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    if (String(url).indexOf("/v1/admin/routes") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve({ status: "ok", routes: ROUTES }) });
    }
    return new Promise(() => {});
  }
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw: " + e.message); failures += 1; }
});

setTimeout(function () {
  ok("the page rendered its routes", cells.length === ROUTES.length,
     "parsed " + cells.length + " live cells from " + ROUTES.length + " routes");

  const before = cells.map((c) => c.textContent).join("");
  ok("nothing is claimed before a frame arrives", before === "",
     "a cell said " + JSON.stringify(before) + " before anything reported traffic");

  /* Deliver a frame the way the strip does. */
  (win.__matrixarkFrameQueue || []).push;
  if (typeof win.__matrixarkLiveFrame === "function") { win.__matrixarkLiveFrame(FRAME); }

  const byMetric = {};
  cells.forEach((c) => { (byMetric[c.metric] = byMetric[c.metric] || []).push(c); });

  const busy = (byMetric["/v1/memories"] || [])[0];
  ok("a route that has served requests says so", busy && /7 requests/.test(busy.textContent),
     busy ? busy.textContent : "no cell for /v1/memories");
  ok("its average latency is shown", busy && /4\.5 ms avg/.test(busy.textContent),
     busy ? busy.textContent : "");
  ok("a route with no errors is not marked bad", busy && busy.className.indexOf("bad") < 0,
     busy ? busy.className : "");

  const failing = (byMetric["/v1/memory/{id}"] || [])[0];
  ok("errors are reported", failing && /2 errors/.test(failing.textContent),
     failing ? failing.textContent : "");
  ok("a route with errors is marked", failing && failing.className.indexOf("bad") >= 0,
     failing ? failing.className : "");

  const quiet = (byMetric["/v1/admin/scopes"] || [])[0];
  ok("a route absent from the frame reports no requests rather than staying blank",
     quiet && /no requests yet/.test(quiet.textContent), quiet ? quiet.textContent : "");

  const shared = cells.filter((c) => /title="Counted together with/.test(c.attrs));
  ok("rows that share a counter say whose number they are showing", shared.length === 2,
     shared.length + " of the two shared rows carry the note");

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}, 30);
