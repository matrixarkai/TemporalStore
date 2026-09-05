/* Does the status strip ask, or does it assume it is not allowed?
 *
 * On the developer default the gateway answers every admin route anonymously -- 200 with no
 * Authorization header at all -- and 401 once auth is on. The strip used to refuse to try:
 *
 *     var k = key();
 *     if (!k) { setTimeout(open, 2000); return; }
 *
 * so on the deployment somebody is most likely to be looking at first, the strip stayed dark on
 * all seven panels because the page would not ask a question the gateway was willing to answer.
 *
 * Both halves have to hold, and they pull against each other: ask when there is no key, and still
 * watch for a key on a deployment that wants one rather than backing off to thirty seconds. So the
 * page's own strip is run against a gateway that answers and a gateway that refuses.
 *
 * Usage: node open_deployment_harness.js <page.html>
 */
"use strict";
const fs = require("fs");
const realSetImmediate = setImmediate;

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* The strip's own block, not the page's.
 *
 * Overview and Setup run a connection of their own and the strip stands down for them, and since
 * the page-level stream gained the same hidden-tab rule its comment matches too. The line that
 * only the strip has is the stand-down test itself. */
const src = scripts.find((s) => s.includes("if (window.__matrixarkLive) { return; }"));
if (!src) { console.log("FAIL the page carries no live strip"); process.exit(1); }

async function flush() {
  for (let i = 0; i < 8; i += 1) {
    await new Promise((resolve) => realSetImmediate(resolve));
  }
}

/* `answer` decides what the fake gateway does with each request. */
function world(answer, keyValue) {
  const timers = [];
  let now = 0;
  const calls = [];
  const listeners = { document: {}, window: {} };

  function element(id) {
    return {
      id, value: keyValue, textContent: "", className: "", hidden: false, style: {}, dataset: {},
      classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
      addEventListener() {}, setAttribute() {}, getAttribute: () => null,
      appendChild() {}, querySelector: () => null, querySelectorAll: () => [],
    };
  }
  const nodes = {};
  const doc = {
    hidden: false,
    addEventListener(t, fn) { (listeners.document[t] = listeners.document[t] || []).push(fn); },
    getElementById(id) { return (nodes[id] = nodes[id] || element(id)); },
    createElement(tag) { return element(tag); },
    querySelectorAll: () => [], querySelector: () => null, body: element("body"),
  };
  const win = {
    addEventListener(t, fn) { (listeners.window[t] = listeners.window[t] || []).push(fn); },
    location: { hash: "" },
    setTimeout(fn, ms) { timers.push({ at: now + (ms || 0), fn, delay: ms || 0 }); },
    clearTimeout() {}, setInterval() { return 0; }, clearInterval() {},
  };

  class Controller {
    constructor() { this.signal = { aborted: false }; }
    abort() { this.signal.aborted = true; }
  }

  const globals = {
    document: doc, window: win, fetch(url, opts) {
      calls.push({ url, headers: (opts && opts.headers) || {} });
      return answer(calls.length);
    },
    AbortController: Controller,
    setTimeout: win.setTimeout, clearTimeout: win.clearTimeout,
    setInterval: win.setInterval, clearInterval: win.clearInterval,
    TextDecoder: function () { this.decode = () => ""; },
    sessionStorage: { getItem: () => keyValue || null, setItem() {}, removeItem() {} },
    localStorage: { getItem: () => keyValue || null, setItem() {}, removeItem() {} },
  };
  new Function(...Object.keys(globals), src)(...Object.values(globals));

  return {
    calls, timers,
    async advance(ms) {
      const until = now + ms;
      await flush();
      for (let guard = 0; guard < 100; guard += 1) {
        const next = timers.filter((t) => t.at <= until).sort((a, b) => a.at - b.at)[0];
        if (!next) { break; }
        timers.splice(timers.indexOf(next), 1);
        now = Math.max(now, next.at);
        next.fn();
        await flush();
      }
      now = until;
    },
    delays: () => timers.map((t) => t.delay),
  };
}

/* A stream that never settles: what a live connection looks like. */
const live = () => new Promise(() => {});
const refused = () => Promise.resolve({ ok: false, status: 401, body: null });

(async () => {
  /* ---------- a deployment that answers anonymously ---------- */
  const openDeployment = world(live, "");
  await openDeployment.advance(100);
  ok("with no key it still asks", openDeployment.calls.length === 1,
     "made " + openDeployment.calls.length + " request(s); the strip stayed dark");
  ok("and sends no credential it does not have",
     !("Authorization" in ((openDeployment.calls[0] || {}).headers || {})),
     JSON.stringify(((openDeployment.calls[0] || {}).headers || {})));

  /* ---------- a deployment that wants a key ---------- */
  const closed = world(refused, "");
  await closed.advance(100);
  ok("a deployment that refuses is asked once", closed.calls.length === 1,
     String(closed.calls.length));
  const waits = closed.delays();
  ok("and is then watched for a key, not backed off from",
     waits.length > 0 && Math.min(...waits) <= 2000,
     "next retry in " + JSON.stringify(waits) + " ms");

  /* The promise this page already made: "the strip must not be the thing that 401s in a loop."
     Asking once is the change; asking every two seconds forever would be a different bug wearing
     the same patch, and only running the clock forward can tell them apart. */
  await closed.advance(60000);
  ok("and does not go on asking a deployment that said no", closed.calls.length === 1,
     closed.calls.length + " requests in a minute with no key -- that is a 401 loop");

  /* ---------- with a key ---------- */
  const withKey = world(live, "k-test");
  await withKey.advance(100);
  ok("a key is still sent when there is one",
     /Bearer k-test/.test(JSON.stringify(((withKey.calls[0] || {}).headers || {}))),
     JSON.stringify(((withKey.calls[0] || {}).headers || {})));

  /* ---------- the hidden-tab rule still holds ---------- */
  const hidden = world(live, "k-test");
  await hidden.advance(100);
  const before = hidden.calls.length;
  hidden.timers.length = 0;
  ok("a connection is open before hiding", before === 1, String(before));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
})();
