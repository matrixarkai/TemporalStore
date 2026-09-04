/* Run the catalogue page's Skills/Resources tabs.
 *
 * One filter covers both lists while only one is visible, so the interesting behaviour is what
 * happens when the query matches the tab you are NOT looking at. Reading the source cannot tell a
 * page that says so from one that shows an empty list.
 *
 * Usage: node catalog_tabs_harness.js <catalog_portal.html>
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

const SKILLS = { skills: [
  { name: "deploy-runbook", description: "How to deploy the airship", updated_at: 1 },
  { name: "oncall", description: "Who to wake", updated_at: 2 },
] };
const RESOURCES = { resources: [
  { name: "hull-spec.md", description: "Airship hull specification", updated_at: 3 },
] };

const listeners = {};
const els = new Map();

function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}

/* Tabs and panes come from the STATIC markup, so they are parsed from the page once. */
const tabCells = [...page.matchAll(/<button[^>]*role="tab"[\s\S]*?<\/button>/g)].map((m) => {
  const tag = m[0];
  const el = {
    id: attr(tag, "id"), dataset: { pane: attr(tag, "data-pane") },
    tabIndex: 0, focused: false, _listeners: {},
    _attrs: { "aria-selected": attr(tag, "aria-selected") || "false" },
    addEventListener(t, fn) { (this._listeners[t] = this._listeners[t] || []).push(fn); },
    dispatch(t, ev) { (this._listeners[t] || []).forEach((fn) => fn(ev || {})); },
    setAttribute(k, v) { this._attrs[k] = String(v); },
    getAttribute(k) { return k in this._attrs ? this._attrs[k] : null; },
    focus() { this.focused = true; }, click() { this.dispatch("click"); }
  };
  return el;
});
const panes = [...page.matchAll(/<section[^>]*class="pane"[^>]*>/g)].map((m) => ({
  id: attr(m[0], "id"), hidden: /\shidden[\s>]/.test(m[0])
}));

function makeEl(id) {
  return {
    id: id || "", hidden: false, innerHTML: "", textContent: "", value: "", className: "",
    style: {}, dataset: {}, children: [],
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
    removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
}
function el(id) {
  const fromMarkup = tabCells.filter((t) => t.id === id)[0];
  if (fromMarkup) { return fromMarkup; }
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

function json(body) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body),
                           text: () => Promise.resolve("{}") });
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
  navigator: {},
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    const u = String(url);
    if (u.indexOf("/v1/skills") >= 0) { return json(SKILLS); }
    if (u.indexOf("/v1/resources") >= 0) { return json(RESOURCES); }
    if (u.indexOf("/v1/admin/events") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               body: { getReader: () => ({ read: () => new Promise(() => {}) }) },
                               json: () => Promise.resolve({}), text: () => Promise.resolve("") });
    }
    return json({});
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
function counts() {
  return [el("skillsCount").textContent.trim(), el("resourcesCount").textContent.trim()];
}
function search(text) {
  el("filter").value = text;
  const fn = listeners["filter:input"];
  if (fn) { fn({}); }
}

function settle() { return new Promise((r) => setImmediate(() => setImmediate(r))); }

(async function () {
  for (let i = 0; i < 40; i += 1) { await settle(); }

  ok("both tabs exist", tabCells.length === 2, tabCells.map((t) => t.id).join(", "));
  ok("exactly one pane is visible", visible().length === 1, visible().join(", "));
  ok("the tabs carry counts", counts()[0] === "2" && counts()[1] === "1", counts().join(" / "));

  /* Clicking has to work, which means the page called the switcher. Without this check, removing
     that call leaves every other assertion passing -- the counts and the notice are computed in
     the render path and do not need the tabs to be wired at all. */
  const resourcesTab = tabCells.filter((t) => t.dataset.pane === "resources")[0];
  ok("there is a Resources tab", !!resourcesTab, tabCells.map((t) => t.id).join(", "));
  resourcesTab.dispatch("click");
  ok("clicking a tab switches the visible list", visible().join() === "pane-resources",
     "visible: " + visible().join(", ") + " -- was the switcher wired?");
  tabCells.filter((t) => t.dataset.pane === "skills")[0].dispatch("click");
  ok("and switches back", visible().join() === "pane-skills", visible().join(", "));

  /* A term only in Resources, while Skills is open. */
  search("hull");
  for (let i = 0; i < 40; i += 1) { await settle(); }
  ok("the filter searched both lists", counts()[0] === "0" && counts()[1] === "1",
     "counts: " + counts().join(" / "));
  const notice = el("crossList").innerHTML;
  ok("it says the match is in the other list",
     /match/.test(notice) && /Resources/.test(notice), JSON.stringify(notice).slice(0, 140));

  /* A term in BOTH lists: the open tab has matches, so nothing should be announced. */
  search("airship");
  for (let i = 0; i < 40; i += 1) { await settle(); }
  ok("the shared term matches both lists", counts()[0] === "1" && counts()[1] === "1",
     "counts: " + counts().join(" / "));
  ok("a match in the open list is not announced as elsewhere",
     el("crossList").innerHTML === "",
     JSON.stringify(el("crossList").innerHTML).slice(0, 140));

  /* Nothing anywhere. */
  search("nothing-matches-this");
  for (let i = 0; i < 40; i += 1) { await settle(); }
  ok("a term matching nothing points nowhere", el("crossList").innerHTML === "",
     JSON.stringify(el("crossList").innerHTML).slice(0, 140));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}());
