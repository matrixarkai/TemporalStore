// Drives the SHIPPED stream client through a goodbye, and reports what the page did about it.
//
// A rotation and a fault are both `event: bye`. The client used to set `planned` for either, and
// `planned` is exactly what makes the next reconnect immediate -- so a gateway that had just said
// it was failing got reconnected into at once, by a page showing nothing wrong.
//
// Usage: node bye_reason_harness.js <page.html> '{"reason":"server_error","incident":"abc"}'
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3] || "{}");

function sliceFn(marker) {
  const start = page.indexOf(marker);
  if (start < 0) throw new Error("not found: " + marker);
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + marker);
}

const ORIGIN = 1_760_000_000_000;
const clockStart = page.indexOf("var liveServerMs = 0;");
const source = page.slice(clockStart, page.indexOf("function liveStream(options)"))
  + "\n" + sliceFn("function liveStream(options)");

let clock = ORIGIN;
const states = [];

/* The stream opens, sends one frame, then says goodbye and ends. Ending it is what makes the
   client decide whether to reconnect at once or back off, which is the behaviour under test. */
const chunks = [
  "retry: 3000\n\n" + "event: status\ndata: " + JSON.stringify({ ts: 1760000000, tick_s: 2 }) + "\n\n",
];

/* A PLANNED goodbye reconnects at once, so a harness that serves a fresh stream every time is an
   infinite loop -- it was, and it exhausted the heap twice. The second connection never answers,
   rather than refusing: refusing drives the client round again. This bounds the run at one
   reconnect and still records that one was attempted. */
let opens = 0;

const scope = {
  Date: { now: () => clock },
  fetch: () => (++opens > 1 ? new Promise(() => {}) : Promise.resolve({
    ok: true,
    body: {
      getReader: () => {
        let step = 0;
        return {
          read: () => {
            step += 1;
            if (step === 1) { return Promise.resolve({ done: false, value: chunks[0] }); }
            if (step === 2) {
              /* Long enough that the client believes a rotation really was one: it checks the
                 goodbye against how long the stream actually lasted. */
              clock += input.livedMs === undefined ? 60000 : input.livedMs;
              return Promise.resolve({
                done: false,
                value: "event: bye\ndata: " + JSON.stringify(input.bye || {}) + "\n\n",
              });
            }
            /* A case that only needs to see what the goodbye REPORTED leaves the stream
               open instead of ending it, so no reconnect is attempted at all. */
            if (input.keepOpen) { return new Promise(() => {}); }
            return Promise.resolve({ done: true });
          },
        };
      },
    },
  })),
  TextDecoder: function () { this.decode = (v) => (v === undefined ? "" : String(v)); },
  AbortController: function () { this.abort = () => {}; this.signal = null; },
  document: { hidden: false, addEventListener: () => {} },
  window: { addEventListener: () => {} },
  setTimeout: () => 0,
  setInterval: () => 1,
  clearInterval: () => {},
};

const names = Object.keys(scope);
const liveStream = new Function(...names, source + "; return liveStream;")(
  ...names.map((k) => scope[k]));

liveStream({
  headers: () => ({}),
  onState: (state, seconds, incident) =>
    states.push({ state: state, seconds: seconds, incident: incident || "" }),
  onFrame: () => {},
});

const wait = () => new Promise((r) => globalThis.setTimeout(r, 15));

(async () => {
  await wait();
  const seen = states.map((s) => s.state);
  process.stdout.write(JSON.stringify({
    states: states,
    /* "connecting" after the goodbye means it reconnected AT ONCE; "retrying" means it backed
       off. A fault must take the second road. */
    reconnectedImmediately: opens > 1,
    backedOff: seen.includes("retrying"),
    reportedAFault: seen.includes("failed"),
    faultIncident: (states.find((s) => s.state === "failed") || {}).incident || "",
  }, null, 1));
})();
