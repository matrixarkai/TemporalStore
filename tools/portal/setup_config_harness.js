/* When someone else changes the configuration, what does the Setup page do?
 *
 * Two answers, and picking the wrong one loses work. With nothing unsaved, taking their change is
 * right -- the form is showing stale values. With unsaved edits, reloading would discard what the
 * person here has typed, so it must say something instead and leave the edits alone.
 *
 * Both paths run through the same handler and differ only by a condition, which is exactly the
 * kind of thing that reads correct and behaves wrong. Frames are released on demand rather than on
 * a timer, because what is being waited on is a frame having been PROCESSED.
 *
 * Usage: node setup_config_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");
process.on("unhandledRejection", (e) => {
  console.log("  unhandled rejection: " + String(e && e.message ? e.message : e));
});

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const FIELD = { key: "extraction.provider", value: "deterministic", kind: "text",
                label: "Extraction provider",
                /* The page does `f.help.length` unguarded, so a field without help throws and the
                   error surfaces as "Could not reach the gateway". The server always sends it. */
                help: "Which provider extracts memories.", env: "MATRIXARK_EXTRACTION_PROVIDER",
                applies: "live", group: "models" };
const CONFIG = { settings: { groups: { models: [FIELD] },
                             group_meta: { models: { title: "Models", order: 1 } },
                             updated_at: 1000, config_file: "/tmp/runtime.json" } };

function frame(changedAt) {
  const f = { ts: Date.now() / 1000, traffic: { routes: {} }, imports: {}, warnings: 0,
              embedding: { total: 1, encoded: 1, pending: 0, encoder: {} } };
  if (changedAt !== undefined) { f.config_changed_at = changedAt; }
  return "event: status\ndata: " + JSON.stringify(f) + "\n\n";
}

const configReads = [];
const seenUrls = [];
const listeners = {};
const els = new Map();

function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "", _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { (listeners[(id || "") + ":" + type] = fn); },
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

let release = null;
function settle() { return new Promise((r) => setImmediate(() => setImmediate(r))); }
async function deliver(text) {
  /* Bounded. An unbounded wait for a stream that never opened is a harness that hangs instead of
     failing, which is worse than a wrong answer: it tells you nothing and costs the whole timeout. */
  let spins = 0;
  while (!release) {
    if (spins++ > 2000) {
      console.log("FAIL the page never opened its live stream, so no frame could be delivered");
      console.log("FAILED 1");
      process.exit(1);
    }
    await settle();
  }
  const r = release;
  release = null;
  r({ done: false, value: Buffer.from(text, "utf8") });
  await settle();
  await settle();
}

function json(body) {
  return Promise.resolve({ ok: true, status: 200,
                           json: () => Promise.resolve(body),
                           text: () => Promise.resolve(JSON.stringify(body)) });
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
    seenUrls.push(u.slice(0, 32));
    if (u.indexOf("/v1/admin/events") >= 0) {
      return Promise.resolve({
        ok: true, status: 200,
        body: { getReader: () => ({ read: () => new Promise((r) => { release = r; }) }) },
        json: () => Promise.resolve({}), text: () => Promise.resolve("")
      });
    }
    if (u.indexOf("/v1/admin/config") >= 0) { configReads.push(u); return json(CONFIG); }
    if (u.indexOf("/v1/metrics") >= 0) {
      return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve("") });
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

(async function () {
  await settle(); await settle();
  ok("the page loaded its configuration", configReads.length >= 1,
     "config reads: " + configReads.length);
  const afterLoad = configReads.length;

  // ---- nothing unsaved: taking their change is the right answer ----
  await deliver(frame(1000));
  await deliver(frame(2000));
  ok("with nothing unsaved, a change elsewhere is taken",
     configReads.length === afterLoad + 1,
     "re-read " + (configReads.length - afterLoad) + " time(s)");
  const afterReload = configReads.length;

  // ---- now type something and leave it unsaved ----
  const onInput = listeners["groups:input"];
  ok("the settings form records edits", typeof onInput === "function",
     "no input handler was registered, so the next check would prove nothing");
  if (typeof onInput === "function") {
    onInput({ target: { dataset: { key: FIELD.key }, value: "openai" } });
  }
  el("saveMsg").innerHTML = "";

  await deliver(frame(3000));
  ok("with unsaved edits, the page does NOT reload over them",
     configReads.length === afterReload,
     "it re-read " + (configReads.length - afterReload) + " time(s) and discarded the edits");
  const said = el("saveMsg").innerHTML;
  ok("it says someone else changed it", /changed this configuration elsewhere/.test(said),
     JSON.stringify(said).slice(0, 160));
  ok("it warns that saving would overwrite theirs", /overwrite/.test(said),
     JSON.stringify(said).slice(0, 160));
  ok("the notice is a warning, not an aside", /class="msg warn"/.test(said),
     JSON.stringify(said).slice(0, 160));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}());
