/* Run the Overview page against a real /v1/admin/overview body and report the rendered checklist.
 *
 * The presence check this replaces was satisfied by any surviving mention of `source_label` in the
 * file -- a mutation that broke the actual render still passed, because the variable was still
 * assigned somewhere above. Reading what the row HTML ends up containing is the only way to tell a
 * label that renders from one that is merely referenced.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const overview = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

const byId = new Map();

function makeEl(id) {
  const listeners = {};
  const attrs = {};
  return {
    id: id || "",
    hidden: false, innerHTML: "", textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(t, fn) { (listeners[t] = listeners[t] || []).push(fn); },
    removeEventListener() {},
    dispatch(t, ev) { (listeners[t] || []).forEach((fn) => fn(ev || {})); },
    appendChild() {}, removeChild() {},
    setAttribute(k, v) { attrs[k] = v; }, getAttribute(k) { return k in attrs ? attrs[k] : null; },
    closest() { return null; }, querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
}

function get(id) {
  if (!byId.has(id)) { byId.set(id, makeEl(id)); }
  return byId.get(id);
}

function respond(body) {
  return Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body))
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
  setTimeout: () => 0,
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
    const key = String(url);
    if (key.indexOf("/v1/admin/overview") === 0) { return respond(overview); }
    return respond({});
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

setTimeout(() => {
  process.stdout.write(JSON.stringify({
    errors,
    checks: get("checks").innerHTML,
    rows: (get("checks").innerHTML.match(/class="checkrow"/g) || []).length
  }));
}, 0);
