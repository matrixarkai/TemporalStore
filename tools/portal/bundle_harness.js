/* Run the overview page, give it a frame, press copy, and read the bundle it assembled.
 *
 * The bundle is built in a closure from a live frame plus three fetches. Whether a field reaches
 * it cannot be read off the source: the frame half only arrives if the page's own stream delivers,
 * and a field referencing something the page never stored would come out null while the source
 * looks correct.
 *
 * Usage: node bundle_harness.js <overview_portal.html>
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

const FRAME = {
  ts: 1, warnings: 0, imports: {}, embedding: { total: 3 },
  datanode: "unreachable",
  traffic: {
    total_requests: 5, total_errors: 2, in_flight: 0,
    routes: { "/v1/retrieve": { requests: 5, errors: 2, avg_ms: 9, p95_ms: 25,
                                statuses: { "200": 3, "503": 2 } } },
    recent_failures: [
      { at: 1730000000, route: "/v1/retrieve", method: "POST", status: 503 },
      { at: 1729999990, route: "/v1/retrieve", method: "POST", status: 503 }
    ]
  }
};
const SSE = "event: status\ndata: " + JSON.stringify(FRAME) + "\n\n";

const clicks = {};
let copied = null;

const els = new Map();
function makeEl(id) {
  return {
    id: id || "", hidden: false, innerHTML: "", textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { if (type === "click" && id) { clicks[id] = fn; } },
    removeEventListener() {}, appendChild() {}, removeChild() {},
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

function json(body, status) {
  return Promise.resolve({
    ok: (status || 200) < 400, status: status || 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body))
  });
}

function streaming() {
  let sent = false;
  return Promise.resolve({
    ok: true, status: 200,
    body: { getReader: () => ({ read: () => {
      if (sent) { return new Promise(() => {}); }
      sent = true;
      return new Promise((r) => setTimeout(
        () => r({ done: false, value: Buffer.from(SSE, "utf8") }), 5));
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
  navigator: { clipboard: { writeText(text) { copied = text; } } },
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    const u = String(url);
    if (u.indexOf("/v1/admin/events") >= 0) { return streaming(); }
    if (u.indexOf("/v1/readyz") >= 0) {
      return json({ ready: false, datanode: "unreachable" }, 503);
    }
    if (u.indexOf("/v1/metrics") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               text: () => Promise.resolve("# HELP x\nx 1\n"),
                               json: () => Promise.resolve({}) });
    }
    if (u.indexOf("/v1/admin/overview") >= 0) { return json({ checks: [], counts: {} }); }
    if (u.indexOf("/v1/admin/config") >= 0) { return json({ settings: {} }); }
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
  ok("the copy button is wired", typeof clicks.copyDiag === "function");
  if (typeof clicks.copyDiag !== "function") {
    console.log("FAILED " + failures); process.exit(1);
  }
  clicks.copyDiag();

  setTimeout(function () {
    ok("a bundle was produced", typeof copied === "string" && copied.length > 0,
       "nothing reached the clipboard; the bundle refused to assemble");
    let parsed = null;
    try { parsed = JSON.parse(copied); } catch (e) { /* reported below */ }
    ok("it is valid JSON", parsed !== null, String(copied).slice(0, 120));
    if (!parsed) { console.log("FAILED " + failures); process.exit(1); }

    ok("it still carries what it always did",
       "overview" in parsed && "config" in parsed && "metrics" in parsed,
       Object.keys(parsed).join(", "));
    ok("it carries the readiness verdict", parsed.readiness && parsed.readiness.status === 503,
       JSON.stringify(parsed.readiness));
    ok("a 503 from readiness is recorded, not dropped as a failed collection",
       parsed.readiness && parsed.readiness.body && parsed.readiness.body.ready === false,
       JSON.stringify(parsed.readiness));
    ok("it carries the failure timeline",
       Array.isArray(parsed.recent_failures) && parsed.recent_failures.length === 2,
       JSON.stringify(parsed.recent_failures));
    ok("the timeline says when, not just how many",
       Array.isArray(parsed.recent_failures) && parsed.recent_failures[0].at > 0,
       JSON.stringify(parsed.recent_failures && parsed.recent_failures[0]));
    ok("it carries the backend state", parsed.datanode === "unreachable",
       JSON.stringify(parsed.datanode));

    console.log(failures === 0 ? "PASS" : "FAILED " + failures);
    process.exit(failures === 0 ? 0 : 1);
  }, 40);
}, 60);
