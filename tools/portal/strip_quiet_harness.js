// Drives the SHIPPED strip client from a generated page: opens, speaks, then goes quiet.
// The strip is an IIFE inside the shared nav script, so it is located by its content
// (`__matrixarkLive = "strip"`) rather than by a name, and evaluated with the globals it uses.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

// Find the IIFE whose body claims the strip.
function stripIife(text) {
  const marker = text.indexOf('__matrixarkLive = "strip"');
  if (marker < 0) throw new Error("strip client not found");
  let start = text.lastIndexOf("(function () {", marker);
  if (start < 0) throw new Error("no enclosing IIFE");
  let depth = 0;
  for (let i = text.indexOf("{", start); i < text.length; i++) {
    if (text[i] === "{") depth++;
    // The slice starts at the IIFE's opening paren, so close it: the body's brace alone
    // leaves  unbalanced.
    else if (text[i] === "}") { depth--; if (depth === 0) return text.slice(start, i + 1) + ")();"; }
  }
  throw new Error("unclosed IIFE");
}

const ORIGIN = 1_760_000_000_000;
let clock = ORIGIN;
let ticker = null;

const dot = { className: "live-dot", title: "live" };
const states = [];

// The strip looks up `liveStrip` first and RETURNS if it is missing, so every id has to answer.
const elements = { liveDot: dot };
function element(id) {
  if (!elements[id]) {
    elements[id] = {
      id: id, className: "", textContent: "", innerHTML: "", hidden: false,
      // The key lookup calls .value.trim(), so every element needs one.
      value: "",
      style: {}, dataset: {}, title: "",
      setAttribute() {}, getAttribute() { return null; },
      appendChild() {}, addEventListener() {},
      classList: { add() {}, remove() {}, toggle() {} },
    };
  }
  return elements[id];
}

const blocks = ["retry: 3000\n\n"];
const frame = { ts: 1760000000, traffic: {}, datanode: "ok" };
if (input.tickS) { frame.tick_s = input.tickS; }
blocks.push("event: status\ndata: " + JSON.stringify(frame) + "\n\n");

let releaseSecond = null;

const scope = {
  document: {
    hidden: false,
    getElementById: (id) => element(id),
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
  },
  window: { addEventListener: () => {}, location: { pathname: "/v1/admin/explore" } },
  sessionStorage: { getItem: () => "k-admin", setItem: () => {} },
  fetch: () => Promise.resolve({
    ok: true,
    status: 200,
    body: {
      getReader: () => {
        let sent = false;
        return {
          read: () => sent ? new Promise((r) => { releaseSecond = r; })
                           : ((sent = true), Promise.resolve({ done: false, value: blocks.join("") })),
        };
      },
    },
  }),
  TextDecoder: function () { this.decode = (v) => (v === undefined ? "" : String(v)); },
  AbortController: function () { this.abort = () => {}; this.signal = null; },
  Date: Object.assign(function () {}, { now: () => clock }),
  setTimeout: () => 0,
  setInterval: (fn) => { ticker = fn; return 1; },
  clearInterval: () => { ticker = null; },
  console: { log: () => {}, warn: () => {}, error: () => {} },
};

const names = Object.keys(scope);
// The IIFE also assigns onto `window`; give it the same object it reads.
new Function(...names, stripIife(page))(...names.map((k) => scope[k]));

const wait = () => new Promise((r) => globalThis.setTimeout(r, 5));

(async () => {
  await wait();
  states.push({ at: 0, className: dot.className, title: dot.title });
  for (const step of (input.advanceMs || [])) {
    clock += step;
    if (ticker) { ticker(); }
    states.push({ at: clock - ORIGIN, className: dot.className, title: dot.title });
  }
  if (input.thenAKeepalive) {
    clock += input.thenAKeepalive;
    if (releaseSecond) { releaseSecond({ done: false, value: ": keepalive\n\n" }); await wait(); }
    if (ticker) { ticker(); }
    states.push({ at: clock - ORIGIN, className: dot.className, title: dot.title });
  }
  process.stdout.write(JSON.stringify({ states, tickerRegistered: ticker !== null }));
})();
