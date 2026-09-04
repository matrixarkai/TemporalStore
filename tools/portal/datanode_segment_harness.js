/* Feed the shared strip a frame and read what the datanode segment did.
 *
 * The strip's render() is wrapped by its caller in a try/catch that ignores errors, so a segment
 * that throws is a segment that silently does nothing -- and the block defines only n, show and
 * plural, so reaching for any other helper fails exactly that way. Reading the source cannot tell
 * a working segment from one that throws on the first frame.
 *
 * Usage: node datanode_segment_harness.js <page.html>
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
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "",
    value: id === "key" ? "k-admin" : "", checked: false, className: "",
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
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: () => new Promise(() => {})
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw at load: " + e.message); failures += 1; }
});

ok("the strip exposes a way to feed it a frame",
   typeof win.__matrixarkLiveFrame === "function");

function feed(datanode) {
  const node = el("liveNode");
  node.hidden = true;
  node.innerHTML = "";
  const frame = { ts: 1, traffic: { total_requests: 1 }, imports: {}, warnings: 0,
                  embedding: { total: 1 } };
  if (datanode !== undefined) { frame.datanode = datanode; }
  win.__matrixarkLiveFrame(frame);
  return node;
}

let seg = feed("unreachable");
ok("an unreachable datanode is shown", !seg.hidden && /unreachable/.test(seg.innerHTML),
   JSON.stringify({ hidden: seg.hidden, html: seg.innerHTML }));

seg = feed("erroring");
ok("an erroring datanode is shown", !seg.hidden && /erroring/.test(seg.innerHTML),
   JSON.stringify({ hidden: seg.hidden, html: seg.innerHTML }));

seg = feed("ok");
ok("a healthy datanode takes no space", seg.hidden,
   JSON.stringify({ hidden: seg.hidden, html: seg.innerHTML }));

seg = feed(undefined);
ok("nothing is claimed before anything has looked", seg.hidden,
   "absent is not the same as reachable: " + JSON.stringify(seg.innerHTML));

seg = feed("<img src=x onerror=alert(1)>");
ok("an unexpected value renders nothing at all",
   seg.hidden && seg.innerHTML.indexOf("<img") < 0,
   "a value outside the known states reached the page: " + JSON.stringify(seg.innerHTML));

/* The rest of the strip must still work while the segment does its job. */
seg = feed("unreachable");
const req = el("liveReq");
ok("the other segments still render", /request/.test(req.innerHTML),
   "render threw partway: " + JSON.stringify(req.innerHTML));

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
