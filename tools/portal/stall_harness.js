// Drives the SHIPPED stream client with a connection that opens, speaks, and then goes quiet.
// The watchdog is the only part of this code no event reaches, so the harness holds the clock and
// fires the timer itself rather than waiting real seconds.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

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
let ticker = null;
let releaseSecondChunk = null;

const opening = ["retry: 3000\n\n"];
/* The cadence rides on the frame, so a case that wants an older gateway sends a frame without it
   rather than withholding a separate event. */
const first = Object.assign({ ts: 1760000000 }, input.frame || {});
if (input.hello !== false) { first.tick_s = input.tickS || 2; }
opening.push("event: status\ndata: " + JSON.stringify(first) + "\n\n");

const scope = {
  Date: { now: () => clock },
  fetch: () => Promise.resolve({
    ok: true,
    body: {
      getReader: () => {
        let sent = false;
        return {
          read: () => {
            if (!sent) { sent = true; return Promise.resolve({ done: false, value: opening.join("") }); }
            // Open and quiet: a promise that settles only if the case sends something later.
            return new Promise((resolve) => { releaseSecondChunk = resolve; });
          },
        };
      },
    },
  }),
  TextDecoder: function () { this.decode = (v) => (v === undefined ? "" : String(v)); },
  AbortController: function () { this.abort = () => {}; this.signal = null; },
  document: { hidden: false, addEventListener: () => {} },
  window: { addEventListener: () => {} },
  setTimeout: () => 0,
  setInterval: (fn) => { ticker = fn; return 1; },
  clearInterval: () => { ticker = null; },
};

const names = Object.keys(scope);
const liveStream = new Function(...names, source + "; return liveStream;")(
  ...names.map((k) => scope[k]));

liveStream({
  headers: () => ({}),
  onState: (state, seconds) => states.push({ state: state, seconds: seconds }),
  onFrame: () => {},
});

const wait = () => new Promise((r) => globalThis.setTimeout(r, 5));

(async () => {
  await wait();                       // let the opening chunk be parsed
  const timeline = [];
  for (const step of (input.advanceMs || [])) {
    clock += step;
    if (ticker) { ticker(); }
    timeline.push({ atMs: clock - ORIGIN, states: states.map((s) => s.state).join(",") });
  }
  if (input.thenAKeepaliveAfterMs) {
    clock += input.thenAKeepaliveAfterMs;
    if (releaseSecondChunk) {
      releaseSecondChunk({ done: false, value: ": keepalive\n\n" });
      await wait();
    }
    if (ticker) { ticker(); }
  }
  process.stdout.write(JSON.stringify({
    states: states,
    timeline: timeline,
    watchdogRegistered: ticker !== null,
  }));
})();
