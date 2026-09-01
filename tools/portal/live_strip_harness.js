/* Run the shipped strip script against real frames and print what it renders.
 *
 * The other tests assert that the source SAYS the right things. This one runs it: a strip that
 * contains the word "unknown" and never reaches that branch reads the same in a grep and is a
 * different product.
 *
 * A DOM stub rather than jsdom -- the strip touches five elements and four properties, and a
 * dependency to test twenty lines is a dependency to keep working.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("/* The shared live strip.");
if (start < 0) { throw new Error("the strip script is not on this page"); }
const scriptStart = page.lastIndexOf("<script>", start) + "<script>".length;
const scriptEnd = page.indexOf("</script>", scriptStart);
const source = page.slice(scriptStart, scriptEnd);

function makeEl(id) {
  return { id, hidden: true, innerHTML: "", className: "", _listeners: {},
           addEventListener() {} };
}
const els = {};
["liveStrip", "liveEnc", "liveImp", "liveReq", "liveWarn", "liveDot"].forEach((id) => {
  els[id] = makeEl(id);
});

const win = {
  document: {
    getElementById: (id) => els[id] || null,
    addEventListener() {},
    hidden: false
  },
  addEventListener() {},
  setTimeout: () => 0,
  sessionStorage: { getItem: () => "" },
  /* Claimed, so the script wires its hooks and then stands down instead of trying to connect.
     Exactly the path a page with its own stream takes. */
  __matrixarkLive: "page",
  Number, JSON, String, Math, Date, TextDecoder: function () {}, AbortController: function () {},
  fetch: () => Promise.reject(new Error("no network in this harness"))
};
win.window = win;

const run = new Function("window", "document", "setTimeout", "sessionStorage", "fetch",
                         "AbortController", "TextDecoder", source);
run(win, win.document, win.setTimeout, win.sessionStorage, win.fetch,
    win.AbortController, win.TextDecoder);

if (typeof win.__matrixarkLiveFrame !== "function") {
  throw new Error("the strip did not expose __matrixarkLiveFrame");
}

function snapshot() {
  return {
    stripHidden: els.liveStrip.hidden,
    enc: els.liveEnc.hidden ? null : els.liveEnc.innerHTML,
    encClass: els.liveEnc.className,
    imp: els.liveImp.hidden ? null : els.liveImp.innerHTML,
    impClass: els.liveImp.className,
    req: els.liveReq.hidden ? null : els.liveReq.innerHTML,
    reqClass: els.liveReq.className,
    warn: els.liveWarn.hidden ? null : els.liveWarn.innerHTML,
    dot: els.liveDot.className
  };
}

const cases = {
  before_any_frame: null,
  draining: {
    embedding: { total: 1840, encoded: 1188, pending: 652 },
    imports: { active: [{ job_id: "j1", total: 400, done: 137, failed: 0 }], retryable: 0 },
    traffic: { total_requests: 5120, total_errors: 0 },
    warnings: 0
  },
  all_encoded_idle: {
    embedding: { total: 1840, encoded: 1840, pending: 0 },
    imports: { active: [], retryable: 0 },
    traffic: { total_requests: 12, total_errors: 0 },
    warnings: 0
  },
  backend_unreachable: {
    imports: { active: [], retryable: 0 },
    traffic: { total_requests: 3, total_errors: 1 },
    warnings: 2
  },
  failures_waiting: {
    embedding: { total: 10, encoded: 10, pending: 0 },
    imports: { active: [], retryable: 7 },
    traffic: { total_requests: 40, total_errors: 0 },
    warnings: 1
  },
  empty_store: {
    embedding: { total: 0, encoded: 0, pending: 0 },
    imports: { active: [], retryable: 0 },
    traffic: { total_requests: 1, total_errors: 0 },
    warnings: 0
  }
};

const out = {};
out.before_any_frame = snapshot();
Object.keys(cases).forEach((name) => {
  if (!cases[name]) { return; }
  win.__matrixarkLiveFrame(cases[name]);
  out[name] = snapshot();
});
process.stdout.write(JSON.stringify(out, null, 2));
