/* Run the Setup page, hand it a frame carrying failures, and read what it drew.
 *
 * The panel is fed by the page's own stream, so nothing here is provable from source: the renderer
 * exists whether or not anything ever calls it, and a status breakdown rendered into the wrong cell
 * looks the same in a diff as one rendered into the right one.
 *
 * Usage: node failures_panel_harness.js <setup_portal.html>
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

const NOW = Math.floor(Date.now() / 1000);
const FRAME = {
  ts: NOW, imports: {}, warnings: 0, embedding: { total: 4, encoded: 4, pending: 0, encoder: {} },
  traffic: {
    total_requests: 12, total_errors: 3, in_flight: 0,
    routes: {
      "/v1/memories": { requests: 9, errors: 1, avg_ms: 4.2, p95_ms: 10,
                        statuses: { "200": 8, "404": 1 } },
      "/v1/retrieve": { requests: 3, errors: 2, avg_ms: 30, p95_ms: null,
                        statuses: { "200": 1, "401": 1, "503": 1 } }
    },
    recent_failures: [
      /* A backend failure carries the token its exception was logged under; a refusal does not,
         because nothing went wrong inside to log. Both shapes are here on purpose. */
      { at: NOW - 5, route: "/v1/retrieve", method: "POST", status: 503,
        incident: "9f2c41ab77de" },
      { at: NOW - 400, route: "/v1/retrieve", method: "POST", status: 401 },
      { at: NOW - 7200, route: "/v1/memories", method: "GET", status: 404 }
    ]
  }
};

const els = new Map();
function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [], _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

const SSE = "event: status\ndata: " + JSON.stringify(FRAME) + "\n\n";

function streamingResponse() {
  let sent = false;
  return Promise.resolve({
    ok: true, status: 200,
    body: { getReader: () => ({ read: () => {
      if (sent) { return new Promise(() => {}); }
      sent = true;
      return new Promise((res) => setTimeout(
        () => res({ done: false, value: Buffer.from(SSE, "utf8") }), 5));
    } }) },
    json: () => Promise.resolve({}), text: () => Promise.resolve("")
  });
}

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(), querySelectorAll: () => [],
    createElement: () => makeEl(), addEventListener() {}, body: makeEl(), hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", reload() {} },
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: {},
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    const u = String(url);
    if (u.indexOf("/v1/admin/events") >= 0) { return streamingResponse(); }
    return Promise.resolve({ ok: true, status: 200,
                             json: () => Promise.resolve({ settings: {} }),
                             text: () => Promise.resolve("{}") });
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
  const traffic = el("traffic").innerHTML;
  const fails = el("failures").innerHTML;

  ok("the traffic table was drawn from a frame", /\/v1\/memories/.test(traffic),
     traffic.slice(0, 160));
  ok("the answers column shows the status breakdown",
     /200.{0,12}8/.test(traffic) && /404/.test(traffic), traffic.slice(0, 300));
  ok("an error status is marked as one", /status-chip bad['"][^>]*>\s*(401|404|503)/.test(traffic)
     || /status-chip bad/.test(traffic), traffic.slice(0, 300));
  ok("a 200 is not marked as an error",
     !/status-chip bad'>200/.test(traffic) && !/status-chip bad">200/.test(traffic),
     traffic.slice(0, 300));

  /* A mean hides the tail; the column exists to show it. And a route with no tail figure must
     say so rather than reading as instant. */
  ok("the tail is shown, not just the mean", /~10 ms/.test(traffic), traffic.slice(0, 460));
  ok("a route with no tail figure does not read as zero", !/~0 ms/.test(traffic),
     traffic.slice(0, 460));

  ok("the failures panel drew the failures", /\/v1\/retrieve/.test(fails), fails.slice(0, 200));
  ok("it shows the status", /503/.test(fails), fails.slice(0, 200));
  ok("it shows the method", />POST</.test(fails), fails.slice(0, 200));
  /* A range, not a number. Time passes between building the frame and the page rendering it,
     so pinning the exact second tests the harness's startup cost, not the formatter. */
  ok("recent seconds read as seconds", /[4-9]s ago/.test(fails), fails.slice(0, 200));
  ok("minutes read as minutes", /\b7m ago\b/.test(fails), fails.slice(0, 300));
  ok("hours read as hours", /\b2h ago\b/.test(fails), fails.slice(0, 400));
  ok("newest is first", fails.indexOf("503") < fails.indexOf("401"), fails.slice(0, 300));
  /* Every backend failure now mints an incident token: the caller is told it and the exception is
     logged under it. Without it here, an operator reading "503 on /v1/retrieve, 5 seconds ago" has
     to guess at timestamps in a log that may hold many. */
  ok("a failure that carries a token shows it", /9f2c41ab77de/.test(fails), fails.slice(0, 500));
  ok("the column is there even for the rows without one", /<th>Incident<\/th>/.test(fails),
     "most rows here are refusals, which mint none; a column that came and went with the data "
     + "would shift the table under whoever is reading it");
  ok("a row without one shows a dash rather than an empty cell", /—/.test(fails),
     fails.slice(0, 700));

  ok("no identity is rendered",
     !/tenant|api[_-]?key|user_id|acme/i.test(fails), fails.slice(0, 300));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}, 40);
