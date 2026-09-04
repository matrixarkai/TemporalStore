/* Apply a pasted configuration and read what the page said afterwards.
 *
 * An export omits secret values on purpose -- blanking them at the target would clear a working
 * key -- and the export button says so to the person exporting. The file does not carry that note,
 * so the person applying it somewhere else is told "Applied 47 settings." and nothing more. Whether
 * the missing credentials are mentioned depends on a reload finishing and a second message
 * replacing the first, which is behaviour a reader of the source cannot confirm.
 *
 * Usage: node import_secrets_harness.js <setup_portal.html>
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

const els = new Map();
const listeners = {};
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false, disabled: false,
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
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

/* One secret configured, one not: the message must name the second and only the second. */
function snapshot() {
  return {
    status: "ok",
    settings: {
      status: "ok", groups: { Extraction: [
        { key: "extraction.model", kind: "str", label: "Model", help: "", value: "m",
          env: "MATRIXARK_EXTRACTION_MODEL", applies: "live", source: "config",
          essential: false, default: "", overridable_by: [], boot_pinned: false,
          pending_restart: false },
        { key: "extraction.api_key", kind: "secret", label: "API key", help: "", value: null,
          configured: false, env: "MATRIXARK_EXTRACTION_API_KEY", applies: "restart",
          source: "default", essential: false, default: "", overridable_by: [],
          boot_pinned: false, pending_restart: false },
        { key: "embedding.api_key", kind: "secret", label: "Embedding key", help: "", value: null,
          configured: true, env: "MATRIXARK_EMBEDDING_API_KEY", applies: "restart",
          source: "config", essential: false, default: "", overridable_by: [],
          boot_pinned: false, pending_restart: false }
      ] },
      group_meta: {}, presets: [], history: [], inventory: {}, essential_keys: [],
      config_file: "/tmp/runtime.json", pending_restart: []
    },
    warnings: []
  };
}

function reply(body) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body),
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
  setTimeout: (fn) => { if (typeof fn === "function") { fn(); } return 0; },
  setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url, opts) => {
    const method = ((opts || {}).method || "GET").toUpperCase();
    if (String(url).indexOf("/v1/admin/config") >= 0 && method === "POST") {
      return reply({ status: "ok", applied: [], restart_required: ["extraction.provider"] });
    }
    if (String(url).indexOf("/v1/admin/config") >= 0) { return reply(snapshot()); }
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
async function quiet() { for (let i = 0; i < 60; i += 1) { await settle(); } }

(async function () {
  ok("the import control is wired", typeof listeners["importCfg:click"] === "function");

  el("importText").value = JSON.stringify({ settings: { "extraction.model": "deepseek-chat" } });
  listeners["importCfg:click"]({});
  await quiet();

  const said = el("importMsg").innerHTML + " " + el("importMsg").textContent;
  ok("it still reports what it applied", /Applied 1 setting/.test(said), said);
  ok("it says the restart is needed", /restart/.test(said), said);
  ok("it names the secret this deployment does not have",
     /extraction\.api_key/.test(said), said);
  ok("it does not name the one that is set",
     !/embedding\.api_key/.test(said), said);
  ok("it says why the file could not carry it",
     /never carries/.test(said), said);

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}().catch(function (err) {
  console.log("FAILED harness threw: " + err.message);
  process.exit(1);
}));
