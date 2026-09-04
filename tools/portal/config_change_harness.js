/* Does an open overview page re-read when the configuration changes, and only then?
 *
 * Re-reading costs three listings on the backend, so "reacts to a change" and "reacts to every
 * frame" are very different behaviours that look nearly identical in source. The page's own stream
 * delivers a sequence of frames here and the harness counts what it fetched.
 *
 * Usage: node config_change_harness.js <overview_portal.html>
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

function frame(changedAt) {
  const f = { ts: Date.now() / 1000, traffic: { total_requests: 1 }, imports: {}, warnings: 0,
              embedding: { total: 1 } };
  if (changedAt !== undefined) { f.config_changed_at = changedAt; }
  return "event: status\ndata: " + JSON.stringify(f) + "\n\n";
}


const overviewReads = [];
const seenFetches = [];
const els = new Map();
const hidden = { value: false };

function makeEl(id) {
  return {
    id: id || "", hidden: false, innerHTML: "", textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
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

function json(body) {
  return Promise.resolve({ ok: true, status: 200,
                           json: () => Promise.resolve(body),
                           text: () => Promise.resolve(JSON.stringify(body)) });
}

/* Frames are released one at a time by the harness, not on a timer. Two attempts at timing this
   produced two different wrong answers -- first "0 re-reads for one change" about a page that had
   re-read correctly, then the re-read attributed to the wrong phase. Whether a frame has been
   PROCESSED is the thing being waited on, and a clock only approximates it. */
let release = null;
function streaming() {
  return Promise.resolve({
    ok: true, status: 200,
    body: { getReader: () => ({ read: () => new Promise((r) => { release = r; }) }) },
    json: () => Promise.resolve({}), text: () => Promise.resolve("")
  });
}

function settle() {
  return new Promise((r) => setImmediate(() => setImmediate(r)));
}

async function deliver(text) {
  while (!release) { await settle(); }
  const r = release;
  release = null;
  r({ done: false, value: Buffer.from(text, "utf8") });
  await settle();
  await settle();
}

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(), querySelectorAll: () => [],
    createElement: () => makeEl(), addEventListener() {}, body: makeEl(),
    get hidden() { return hidden.value; }
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
    seenFetches.push(u.slice(0, 40));
    if (u.indexOf("/v1/admin/events") >= 0) { return streaming(); }
    if (u.indexOf("/v1/admin/overview") >= 0) {
      overviewReads.push(Date.now());
      return json({ checks: [], counts: {} });
    }
    if (u.indexOf("/v1/metrics") >= 0) {
      return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve("") });
    }
    return json({ settings: {} });
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
  await settle();
  await settle();
  ok("the page read its checklist on load", overviewReads.length >= 1,
     "reads: " + overviewReads.length);
  const afterLoad = overviewReads.length;

  await deliver(frame(1000));          // first sighting: records, does not re-read
  await deliver(frame(1000));          // same value again: still nothing to do
  ok("an unchanged configuration does not cause a re-read",
     overviewReads.length === afterLoad,
     "re-read " + (overviewReads.length - afterLoad) + " time(s) with nothing changed");

  await deliver(frame(2000));          // changed
  ok("a changed configuration causes exactly one re-read",
     overviewReads.length === afterLoad + 1,
     "re-read " + (overviewReads.length - afterLoad) + " time(s) for one change");

  await deliver(frame(2000));          // and does not keep re-reading afterwards
  ok("it does not re-read again while the value stays put",
     overviewReads.length === afterLoad + 1,
     "re-read " + (overviewReads.length - afterLoad) + " time(s) in total");

  await deliver(frame(undefined));     // a frame with no timestamp at all
  ok("a frame without the field changes nothing",
     overviewReads.length === afterLoad + 1,
     "re-read " + (overviewReads.length - afterLoad) + " time(s) in total");

  console.log("     fetches: " + JSON.stringify(seenFetches));
  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}());
