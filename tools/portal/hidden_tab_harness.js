/* Does a portal page in a background tab actually let go of its live connection?
 *
 * Every page says it does. The status strip carries the rule in as many words -- "A hidden tab
 * holds a connection open for nobody. Drop it, reconnect on return." -- and aborts the request
 * when the tab is hidden. What it does not do is stop the reconnect: aborting rejects the fetch,
 * the catch schedules setTimeout(open, backoff), and about a second later the page opens the
 * stream again while the tab is still hidden.
 *
 * That is not visible in the source. The abort is there, the comment is there, and the line that
 * undoes them is thirty lines away in a different function. So the page's own code is run here
 * against a fake clock: hide the tab, fire every timer that comes due, and count the connections
 * opened after the abort.
 *
 * Usage: node hidden_tab_harness.js <page.html>
 */
"use strict";
const fs = require("fs");
/* Captured before anything in this file shadows it: the page under test is handed a fake
   setTimeout, and the flush below must not go through it. */
const realSetImmediate = setImmediate;

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- a fake clock, so a backoff does not cost a real second ---------- */
function makeWorld() {
  const timers = [];
  let now = 0;
  const opened = [];
  const aborted = [];

  const listeners = { document: {}, window: {} };
  /* Enough of an element for the strip to read a key out of and to write a status into. It reads
     `key()` before connecting and gives up quietly when there is none, so a stub returning null
     produces a page that simply never opens a connection -- which would make every check below
     pass for the wrong reason. */
  function element(id) {
    return {
      id,
      value: "k-test",
      textContent: "",
      className: "",
      hidden: false,
      style: {},
      dataset: {},
      classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
      addEventListener() {},
      setAttribute() {},
      getAttribute: () => null,
      appendChild() {},
      querySelector: () => null,
      querySelectorAll: () => [],
    };
  }
  const nodes = {};
  const doc = {
    hidden: false,
    addEventListener(type, fn) { (listeners.document[type] = listeners.document[type] || []).push(fn); },
    getElementById(id) { return (nodes[id] = nodes[id] || element(id)); },
    createElement(tag) { return element(tag); },
    querySelectorAll() { return []; },
    querySelector() { return null; },
    body: element("body"),
  };
  const win = {
    addEventListener(type, fn) { (listeners.window[type] = listeners.window[type] || []).push(fn); },
    location: { hash: "" },
    setTimeout(fn, ms) { timers.push({ at: now + (ms || 0), fn }); return timers.length; },
    clearTimeout() {},
    setInterval() { return 0; },
    clearInterval() {},
  };

  class FakeController {
    constructor() {
      this.signal = { aborted: false };
      this.index = opened.length;
    }
    abort() {
      this.signal.aborted = true;
      aborted.push(this.index);
      const pending = opened[this.index];
      if (pending && pending.reject) { pending.reject(new Error("aborted")); }
    }
  }

  function fetchStub(url) {
    const record = { url, reject: null };
    opened.push(record);
    /* A connection that never resolves: a live stream does not settle until it breaks, and that
       is exactly the state we want the page to be holding when the tab is hidden. */
    return new Promise((_resolve, reject) => { record.reject = reject; });
  }

  return {
    doc, win, opened, aborted, listeners,
    fetch: fetchStub,
    AbortController: FakeController,
    fire(type, where) {
      (listeners[where][type] || []).forEach((fn) => fn({}));
    },
    /* Run every timer due within `ms`, including ones those timers schedule.
     *
     * The flush matters more than the clock. Aborting a fetch rejects it, and the reconnect is
     * registered from the .catch at the end of that rejection's microtask chain -- so a scan for
     * due timers that runs first finds none and reports a page that let go of its connection.
     * That is a false pass on exactly the behaviour this file exists to check, and it is what a
     * single `await Promise.resolve()` produced. setImmediate yields past the whole microtask
     * queue rather than one tick of it.
     */
    async flush() {
      for (let i = 0; i < 8; i += 1) {
        await new Promise((resolve) => realSetImmediate(resolve));
      }
    },
    async advance(ms) {
      const until = now + ms;
      await this.flush();
      for (let guard = 0; guard < 200; guard += 1) {
        const next = timers.filter((t) => t.at <= until).sort((a, b) => a.at - b.at)[0];
        if (!next) { break; }
        timers.splice(timers.indexOf(next), 1);
        now = Math.max(now, next.at);
        next.fn();
        await this.flush();
      }
      now = until;
    },
  };
}

/* ---------- pick the stream this page actually runs ---------- */
/* Two of the seven pages open their own connection and make the strip stand down for them
   (`window.__matrixarkLive = "page"`), so "the page's live connection" is a different block of
   code depending on which page it is. Running the strip's block on a page whose strip returns
   immediately would check a code path that page never takes. */
const MODE = process.argv[3] || "strip";

/* Just the factory, not the block it sits in.
 *
 * The page's own script calls wireTabs and paints half a dozen panels as soon as it is evaluated,
 * so running the whole block to reach one function fails on the first thing this harness has no
 * DOM for -- and reports it as "the stream would not run", which is not what was being asked. */
function sliceFactory(script) {
  const at = script.indexOf("function liveStream(options)");
  if (at === -1) { return null; }
  let depth = 0;
  for (let i = script.indexOf("{", at); i < script.length; i += 1) {
    if (script[i] === "{") { depth += 1; }
    else if (script[i] === "}") {
      depth -= 1;
      if (depth === 0) { return script.slice(at, i + 1); }
    }
  }
  return null;
}

const src = MODE === "page"
  ? sliceFactory(scripts.find((s) => s.includes("function liveStream(options)")) || "")
  : scripts.find((s) => s.includes("A hidden tab holds a connection open")
                     && s.includes("window.__matrixarkLive"));
if (!src) {
  console.log("FAIL the page carries no " + MODE + " stream");
  process.exit(1);
}

(async () => {
  const world = makeWorld();
  const globals = {
    document: world.doc,
    window: world.win,
    fetch: world.fetch,
    AbortController: world.AbortController,
    setTimeout: world.win.setTimeout,
    clearTimeout: world.win.clearTimeout,
    setInterval: world.win.setInterval,
    clearInterval: world.win.clearInterval,
    TextDecoder: function () { this.decode = () => ""; },
    sessionStorage: { getItem: () => "k-test", setItem() {}, removeItem() {} },
    localStorage: { getItem: () => "k-test", setItem() {}, removeItem() {} },
  };
  /* For the page stream the block only DEFINES the factory -- the page calls it from code that
     wants far more of a DOM than this. So the factory is handed back and called here, with the
     same options shape the page uses. */
  const tail = MODE === "page"
    ? "\n; return typeof liveStream === 'function' ? liveStream : null;"
    : "";
  let factory = null;
  try {
    factory = new Function(...Object.keys(globals), src + tail)(...Object.values(globals));
  } catch (error) {
    console.log("FAIL the " + MODE + " stream would not run: " + error.message);
    process.exit(1);
  }
  if (MODE === "page") {
    ok("the page defines its own stream", typeof factory === "function");
    if (typeof factory !== "function") { console.log("FAILED " + failures); process.exit(1); }
    factory({ headers: () => ({ Authorization: "Bearer k-test" }) });
  }

  await world.advance(3000);
  const beforeHide = world.opened.length;
  ok("the page opens a live connection", beforeHide > 0, String(beforeHide));

  world.doc.hidden = true;
  world.fire("visibilitychange", "document");
  await world.flush();
  ok("hiding the tab aborts the connection", world.aborted.length > 0);

  const atHide = world.opened.length;
  await world.advance(60000);
  const whileHidden = world.opened.length - atHide;
  ok("and it stays closed while the tab is hidden", whileHidden === 0,
     whileHidden + " connection(s) opened while hidden -- the reconnect ignores the tab");

  world.doc.hidden = false;
  world.fire("visibilitychange", "document");
  await world.advance(3000);
  ok("coming back opens it again", world.opened.length > atHide + whileHidden);

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
})();
