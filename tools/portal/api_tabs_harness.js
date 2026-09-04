/* Run the API page's group tabs and read what they did.
 *
 * Two things here cannot be seen in source. The list is rebuilt on every keystroke, so whether the
 * open tab survives a re-render is behaviour. And the text filter searches every group while only
 * one pane is visible -- if it quietly searched the open tab alone, a reference page would answer
 * "nothing matches that" while holding the match one tab away.
 *
 * Usage: node api_tabs_harness.js <api_portal.html>
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

const ROUTES = [
  { group: "Memory", method: "POST", path: "/v1/ingest", summary: "Store a memory.",
    scope: "write", metric: "/v1/ingest" },
  { group: "Memory", method: "POST", path: "/v1/retrieve", summary: "Recall memories.",
    scope: "read", metric: "/v1/retrieve" },
  { group: "Administration", method: "GET", path: "/v1/admin/config",
    summary: "Read the configuration blueprint.", scope: "admin",
    metric: "/v1/admin/config" },
  { group: "Blobs", method: "PUT", path: "/v1/blob/{key}",
    summary: "Upload a zeppelin blueprint.", scope: "write", metric: "/v1/blob/{key}" },
];

let tabCells = [];
let panes = [];
const listeners = {};
const els = new Map();

function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}

function parseTabs(html) {
  tabCells = [...String(html || "").matchAll(/<button[^>]*role="tab"[^>]*>[\s\S]*?<\/button>/g)]
    .map((m) => {
      const tag = m[0];
      const el = {
        id: attr(tag, "id"), text: tag.replace(/<[^>]*>/g, ""),
        dataset: { pane: attr(tag, "data-pane") },
        tabIndex: 0, focused: false,
        _attrs: { "aria-selected": attr(tag, "aria-selected") || "false" },
        _listeners: {},
        addEventListener(t, fn) { (this._listeners[t] = this._listeners[t] || []).push(fn); },
        dispatch(t, ev) { (this._listeners[t] || []).forEach((fn) => fn(ev || {})); },
        setAttribute(k, v) { this._attrs[k] = String(v); },
        getAttribute(k) { return k in this._attrs ? this._attrs[k] : null; },
        focus() { this.focused = true; }, click() { this.dispatch("click"); }
      };
      return el;
    });
}

function parsePanes(html) {
  panes = [...String(html || "").matchAll(/<section[^>]*class="pane"[^>]*>/g)].map((m) => ({
    id: attr(m[0], "id"), hidden: /\shidden[\s>]/.test(m[0])
  }));
}

function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "", value: "", className: "",
    style: {}, dataset: {}, children: [], _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) {
      this._html = String(v);
      if (this.id === "routeTabs") { parseTabs(v); }
      if (this.id === "routes") { parsePanes(v); }
    },
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
    removeEventListener() {}, appendChild() {}, removeChild() {},
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

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(),
    querySelectorAll: (sel) => (sel === ".tabs button" ? tabCells
                                : (sel === ".pane" ? panes : [])),
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
  TextDecoder: function () { this.decode = () => ""; },
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

function visible() { return panes.filter((p) => !p.hidden).map((p) => p.id); }
function selected() {
  return tabCells.filter((t) => t.getAttribute("aria-selected") === "true").map((t) => t.id);
}
function type(text) {
  el("filter").value = text;
  listeners["filter:input"]({});
}

setTimeout(function () {
  ok("a tab per group", tabCells.length === 3,
     tabCells.map((t) => t.text).join(" | "));
  ok("each tab carries its count",
     tabCells.some((t) => /Memory\s*2/.test(t.text)),
     tabCells.map((t) => t.text).join(" | "));
  ok("exactly one pane is visible", visible().length === 1, visible().join(", "));
  ok("exactly one tab is selected", selected().length === 1, selected().join(", "));

  /* Switch to Blobs. */
  const blobs = tabCells.filter((t) => /Blob/.test(t.text))[0];
  ok("there is a Blobs tab", !!blobs, tabCells.map((t) => t.text).join(" | "));
  blobs.dispatch("click");
  ok("clicking a tab shows that group", visible().join() === "pane-blobs", visible().join(", "));

  /* A term that appears only in Memory, while Blobs is open. */
  type("recall");
  ok("the open tab survives a keystroke",
     selected().join().indexOf("blobs") >= 0, selected().join(", "));
  ok("the filter searched every group, not just the open one",
     tabCells.some((t) => /Memory\s*1/.test(t.text)),
     "tab counts: " + tabCells.map((t) => t.text).join(" | "));
  const notice = el("crossGroup").innerHTML;
  ok("it says the matches are in another group", /match/.test(notice) && /Memory/.test(notice),
     JSON.stringify(notice).slice(0, 140));

  /* A term that matches nothing anywhere. */
  type("zeppelin-that-does-not-exist");
  ok("a term matching nothing says so without pointing elsewhere",
     el("crossGroup").innerHTML === "",
     JSON.stringify(el("crossGroup").innerHTML).slice(0, 140));

  /* A term that matches the OPEN group and another one. The notice is for the case where the open
     tab has nothing -- announcing "matches elsewhere" while the visible list is full of matches is
     noise, and only a term hitting both tells those two behaviours apart. */
  type("blueprint");
  ok("the shared term really does match two groups",
     tabCells.filter(function (t) { return /\s1$/.test(t.text.trim()); }).length >= 2,
     "tab counts: " + tabCells.map(function (t) { return t.text; }).join(" | "));
  ok("a match in the open group is not announced as elsewhere",
     el("crossGroup").innerHTML === "",
     JSON.stringify(el("crossGroup").innerHTML).slice(0, 140));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}, 40);
