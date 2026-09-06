/* Run the Setup page and drive the deployment chooser the way a customer would.
 *
 * The thing worth proving here cannot be read off the source. The chooser's whole purpose is that a
 * request and its outcome differ -- ask for shared storage with no directory, or MatrixObject on a
 * build without it, and the engine starts a deployment on something else without erroring. A page
 * that renders the request back is indistinguishable, in source, from one that renders what the
 * plan resolved to. Only running it and reading the DOM separates them.
 *
 * Element identity is kept per id, and `change` events are dispatched through the real listeners,
 * so the select-driven reveal of the shared-directory field is exercised rather than assumed.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const routes = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

const posted = [];
const byId = new Map();

function makeEl(id) {
  const listeners = {};
  const attrs = {};
  const el = {
    id: id || "",
    hidden: false, innerHTML: "", textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    removeEventListener() {},
    dispatch(type, ev) { (listeners[type] || []).forEach((fn) => fn(ev || {})); },
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
  fetch: (url, opts) => {
    const key = String(url);
    if (opts && opts.method === "POST") { posted.push({ url: key, body: opts.body }); }
    if (key.indexOf("/v1/admin/deployment/plan") === 0) { return respond(routes.plan); }
    if (key.indexOf("/v1/admin/deployment") === 0) { return respond(routes.deployment); }
    if (key.indexOf("/v1/admin/config") === 0) { return respond(routes.config); }
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

/* Microtasks drain before timers, so one real timer is enough for the whole
   load -> loadDeployment -> previewPlan chain to settle. */
setTimeout(() => {
  const shapeOptions = (get("depShape").innerHTML.match(/<option/g) || []).length;

  /* Snapshot before touching anything: a preview that only happens because the harness dispatched
     a change is not the same feature as one the customer gets on arrival. */
  const postedOnLoad = posted.map((p) => p.url);
  const verdictOnLoad = get("depVerdict").innerHTML;

  /* Drive the selects the way the page's own listeners see it. */
  get("depShape").value = "shared";
  get("depShape").dispatch("change");
  const storageAfterShared = get("depStorage").innerHTML;
  get("depStorage").value = "path";
  get("depStorage").dispatch("change");
  const sharedFieldShownForPath = get("depSharedField").hidden === false;
  get("depStorage").value = "matrixobject";
  get("depStorage").dispatch("change");
  const sharedFieldHiddenForObject = get("depSharedField").hidden === true;

  setTimeout(() => {
    process.stdout.write(JSON.stringify({
      errors,
      shapeOptions,
      userData: get("depUserData").textContent,
      commands: get("depCommands").textContent,
      postedOnLoad,
      verdictOnLoad,
      live: get("liveShape").textContent,
      verdict: get("depVerdict").innerHTML,
      /* The sentence under the table. "Launchable" and "launchable, but not as the storage you
         chose" are different answers and only this element carries the difference. */
      msg: get("depMsg").innerHTML,
      envFile: get("depEnv").textContent,
      storageAfterShared,
      sharedFieldShownForPath,
      sharedFieldHiddenForObject,
      posted: posted.map((p) => ({ url: p.url, body: p.body }))
    }));
  }, 0);
}, 0);
