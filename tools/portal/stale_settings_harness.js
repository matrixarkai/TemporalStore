/* Load the setup page against a deployment whose config file holds settings this build dropped.
 *
 * Whether the note appears, and whether it appears when there is nothing to say, is behaviour: the
 * warnings area is shared with the model-configuration warnings, and an addition that swallowed
 * those or that drew an empty note every time would read the same in the source.
 *
 * Usage: node stale_settings_harness.js <setup_portal.html> [--none]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const none = process.argv.indexOf("--none") >= 0;
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = new Map();
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false, disabled: false,
    style: {}, dataset: {}, files: [], options: [], children: [],
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

const STALE = none ? [] : ["embedding.old_name", "extraction.retired_knob"];

function snapshot() {
  return {
    status: "ok",
    /* A real warning alongside, so an addition that swallowed the existing ones would show. */
    warnings: ["extraction is falling back to rules"],
    extraction: { provider: "deterministic" }, embedding: { provider: "deterministic" },
    settings: {
      status: "ok",
      groups: { Extraction: [
        { key: "extraction.model", kind: "str", label: "Model", help: "", value: "m",
          env: "MATRIXARK_EXTRACTION_MODEL", applies: "live", source: "config", essential: false,
          default: "", overridable_by: [], boot_pinned: false, pending_restart: false }
      ] },
      group_meta: {}, presets: [], history: [], inventory: {}, essential_keys: [],
      config_file: "/etc/matrixark/runtime_config.json",
      pending_restart: [], unknown_stored: STALE
    }
  };
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
  setTimeout: (fn) => { if (typeof fn === "function") { fn(); } return 0; },
  setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    if (String(url).indexOf("/v1/admin/config") >= 0) {
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(snapshot()),
                               text: () => Promise.resolve("{}") });
    }
    return new Promise(() => {});
  }
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw at load: " + e.message); failures += 1; }
});

function settle() { return new Promise((r) => setImmediate(() => setImmediate(r))); }

(async function () {
  for (let i = 0; i < 60; i += 1) { await settle(); }
  const shown = el("warnings").innerHTML;

  ok("the model warning still renders", /falling back to rules/.test(shown), shown.slice(0, 200));

  if (none) {
    ok("nothing is said when nothing is stale", !/does nothing|do nothing/.test(shown),
       "a note appeared with nothing to report: " + shown.slice(0, 240));
  } else {
    ok("the stale values are named",
       /extraction\.retired_knob/.test(shown) && /embedding\.old_name/.test(shown),
       shown.slice(0, 300));
    ok("it says they do nothing", /do nothing/.test(shown), shown.slice(0, 300));
    ok("it says where they are", /runtime_config\.json/.test(shown), shown.slice(0, 300));
    ok("it counts them", /2 stored values are/.test(shown), shown.slice(0, 300));
  }

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}().catch(function (err) {
  console.log("FAILED harness threw: " + err.message);
  process.exit(1);
}));
