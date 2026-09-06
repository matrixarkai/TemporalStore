/* Run the Setup page, hand it a traffic series, and read the chart it drew.
 *
 * A chart is the easiest panel to make lie. An empty one and a quiet one look the same; a missing
 * sample drawn as zero is a dip that never happened; a series too short to plot rendered as a flat
 * line reads as "no traffic" rather than "not watching long enough". None of that is visible in a
 * diff, so the page is run and the SVG is read.
 *
 * Usage: node trend_panel_harness.js <setup_portal.html> [full|short|absent|gaps]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "full";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function points(n) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    out.push({
      at: 1788600000 + i * 15,
      requests_per_s: 1 + (i % 4),
      errors_per_s: i === 3 ? 2 : 0,
      /* An interval in which nothing completed has no mean. Drawing it as 0 would show a latency
         dip that never happened. */
      mean_ms: (mode === "gaps" && i % 2 === 1) ? null : 12 + i,
    });
  }
  return out;
}

const SERIES = mode === "absent" ? undefined : {
  interval_s: 15,
  points: mode === "short" ? points(1) : points(8),
  covers_s: mode === "short" ? 0 : 105,
  worker_scoped: true,
};

const OVERVIEW = {
  status: "ok",
  footprint: { worker: { resident_bytes: 1048576, peak_bytes: 1048576,
                         source: "/proc/self/status", workers: 1 },
               engine: { available: false } },
  latency: { available: true, deadline_ms: 0, deadline_is_cooperative: true,
             transport_request_timeout_ms: 60000, transport_io_timeout_ms: 60000,
             bounds_a_slow_call: "transport_request_timeout_ms",
             worst_case_single_call_ms: 60000, deadline_can_be_overrun_by_ms: 0 },
  metrics_series: SERIES,
  config: { skills: { budgets: { available: false } } },
};

const els = new Map();
function makeEl(id) {
  return {
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
}
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
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
    if (u.indexOf("/v1/admin/overview") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve(OVERVIEW),
                               text: () => Promise.resolve(JSON.stringify(OVERVIEW)) });
    }
    if (u.indexOf("/v1/admin/events") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               body: { getReader: () => ({ read: () => new Promise(() => {}) }) },
                               json: () => Promise.resolve({}), text: () => Promise.resolve("") });
    }
    return Promise.resolve({ ok: true, status: 200,
                             json: () => Promise.resolve({ settings: {} }),
                             text: () => Promise.resolve("") });
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
  const html = el("trend").innerHTML;
  ok("the panel was drawn at all", !/Loading/.test(html), html.slice(0, 160));

  if (mode === "absent") {
    /* An older worker that does not publish a series must not render an empty chart, which would
       read as a deployment serving nothing. */
    ok("a missing series says so rather than drawing zero",
       /Not available/.test(html) && !/<svg/.test(html), html.slice(0, 300));
  } else if (mode === "short") {
    ok("one interval is not plotted as a line", !/<svg/.test(html), html.slice(0, 300));
    ok("and the page says what it is waiting for",
       /needs two intervals/.test(html), html.slice(0, 300));
    ok("it does not read as an idle deployment", !/no traffic/i.test(html), html.slice(0, 300));
  } else {
    ok("three charts were drawn", (html.match(/<svg/g) || []).length === 3, html.slice(0, 400));
    ok("each is a polyline with points",
       (html.match(/<polyline[^>]*points="[^"]+"/g) || []).length === 3, html.slice(0, 400));
    ok("requests, errors and latency are all labelled",
       /Requests/.test(html) && /Errors/.test(html) && /Mean latency/.test(html),
       html.slice(0, 400));
    ok("the peak of each series is stated", /peak /.test(html), html.slice(0, 400));
    ok("the window is described", /intervals of 15s/.test(html), html.slice(0, 900));
    ok("and it says whose traffic this is", /This worker only/.test(html), html.slice(0, 900));

    if (mode === "gaps") {
      /* Four of the eight latency samples are null. The line must have four vertices, not eight
         with four of them at the floor. */
      const latency = html.slice(html.indexOf("Mean latency"));
      const pts = (latency.match(/points="([^"]+)"/) || ["", ""])[1].trim().split(/\s+/);
      ok("an interval with no calls is skipped, not drawn as zero",
         pts.length === 4, "vertices: " + pts.length + " -> " + pts.join(" "));
    }
  }

  console.log(failures ? "\n" + failures + " failed" : "\nall ok");
  process.exit(failures ? 1 : 0);
}, 60);
