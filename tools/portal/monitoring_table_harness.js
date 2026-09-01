/* Run the Setup page's scripts against a real /v1/admin/config payload and report what the
 * monitoring section actually rendered.
 *
 * Grepping the built page cannot tell these apart: a table that renders five rows, and one whose
 * builder is never reached because the payload it reads is shaped differently than the server
 * sends. Both contain the same source text. The specific thing worth proving here is that the
 * engine row says /metrics -- the whole point of the column is that importing that dashboard
 * against the gateway yields blank panels, and a blank panel reads as a quiet cluster.
 *
 * The stub keeps element identity per id, unlike the watcher harness next to it: this test reads
 * back what the page wrote into #monitoring and #scrape, so a fresh element per lookup would
 * discard exactly the thing under test.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const payload = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

const fetched = [];
const byId = new Map();

function makeEl(id) {
  const listeners = {};
  const attrs = {};
  const el = {
    id: id || "",
    hidden: true, innerHTML: "", textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    removeEventListener() {},
    dispatch(type, ev) { (listeners[type] || []).forEach((fn) => fn(ev)); },
    appendChild() {}, removeChild() {},
    setAttribute(k, v) { attrs[k] = v; }, getAttribute(k) { return k in attrs ? attrs[k] : null; },
    closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}

function get(id) {
  if (!byId.has(id)) { byId.set(id, makeEl(id)); }
  return byId.get(id);
}

function respond(body) {
  return Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(typeof body === "string" ? body : JSON.stringify(body))
  });
}

const win = {
  document: {
    getElementById: get,
    querySelector: () => makeEl(),
    querySelectorAll: () => [],
    createElement: () => makeEl(),
    addEventListener() {},
    body: makeEl("body"),
    hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", host: "gw.example:8080", reload() {} },
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: {},
  setTimeout: (fn) => { if (typeof fn === "function") { /* not run: nothing here is time-driven */ } return 0; },
  setInterval: () => 0,
  clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {},
  Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    fetched.push(String(url));
    if (String(url).indexOf("/v1/admin/config") === 0) { return respond(payload); }
    return respond("{}");
  },
  __matrixarkOnFrame() {}
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
const errors = [];
scripts.forEach((body) => {
  try { new Function(...names, body)(...values); } catch (e) { errors.push(String(e)); }
});

/* The renderers run off a resolved promise, so let the microtask queue drain before reading. */
setTimeout(() => {
  const table = get("monitoring").innerHTML;
  const scrape = get("scrape").textContent;

  /* Click the engine row the way a customer would, through the delegated listener, and record
     which asset the page went and asked for. */
  const before = fetched.length;
  get("monitoring").dispatch("click", {
    target: { getAttribute: (k) => ({ "data-asset": "engine", "data-filename": "d.json" }[k] || null) }
  });
  const clicked = fetched.slice(before);

  process.stdout.write(JSON.stringify({
    errors: errors,
    rows: (table.match(/<tr>/g) || []).length,
    table: table,
    scrape: scrape,
    clicked: clicked
  }));
}, 0);
