/* Run both shipped stream readers against the bytes a gateway actually writes.
 *
 * The gateway labels what it sends: `event: status` for a frame, `event: bye` before it closes a
 * stream that has reached its maximum age. Both readers looked for a `data:` line and nothing
 * else -- and a goodbye carries one, so its reason was handed to the frame handler as though it
 * were the deployment's state. None of that is provable from source: a reader that ignores the
 * label reads exactly like one that honours it until something is put through it.
 *
 * A DOM stub rather than jsdom, matching the strip harness beside this one.
 *
 * Usage: node stream_label_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* The exact shapes the gateway emits, copied from the stream handler: a retry directive, a status
   frame, a comment keepalive, and the goodbye. */
const STATUS = 'event: status\ndata: {"ts":1,"warnings":3,"imports":{"active":[]},'
  + '"traffic":{"total_requests":7,"total_errors":0,"routes":{}},"settings_waiting":0}\n\n';
const KEEPALIVE = ": keepalive\n\n";
const GOODBYE = 'event: bye\ndata: {"reason": "stream_max_age"}\n\n';
const UNKNOWN = 'event: something_new\ndata: {"warnings":99}\n\n';
const RETRY = "retry: 3000\n\n";

/* A clock the harness moves, so "the stream lasted ten minutes" and "the stream said goodbye
   immediately" are two different runs rather than two different waits.
 *
 * One per run, not one shared. These runs are all in flight at once, and a shared clock lets the
 * ten-minute run age the one that is meant to have lasted no time at all. */
function makeClock() {
  const clock = { t: 1000000 };
  function FakeDate(value) { return value === undefined ? new Date() : new Date(value); }
  FakeDate.now = () => clock.t;
  clock.Date = FakeDate;
  return clock;
}

function reader(clock, blocks, opts) {
  const held = (opts || {}).lastFor || 0;
  /* The connection after the one under test stays open. A second stream that also ends would
     report its own close, and every assertion here is about the FIRST one. */
  const hang = (opts || {}).thenHang;
  let index = 0;
  return {
    read() {
      if (index === 0 && held) { clock.t += held; }
      if (index >= blocks.length) {
        return hang ? new Promise(function () {}) : Promise.resolve({ done: true });
      }
      const value = Buffer.from(blocks[index++], "utf8");
      return Promise.resolve({ done: false, value });
    },
  };
}

/* ---- reader one: liveStream(), used by the pages that run their own stream ------------------- */
function extractLiveStream() {
  const start = page.indexOf("function liveStream(options)");
  if (start < 0) { return null; }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  return null;
}

function driveLiveStream(connections) {
  const source = extractLiveStream();
  if (!source) { throw new Error("liveStream is not on this page"); }

  const frames = [], states = [];
  let opened = 0;
  const clock = makeClock();
  const Date_ = clock.Date;
  const document_ = { hidden: false, addEventListener() {} };
  const window_ = { addEventListener() {} };
  const fetch_ = function () {
    const plan = connections[Math.min(opened, connections.length - 1)];
    opened += 1;
    return Promise.resolve({
      ok: true,
      body: { getReader: () => reader(clock, plan.blocks,
                                      { lastFor: plan.lastFor, thenHang: plan.thenHang }) },
    });
  };
  const setTimeout_ = function () { return 0; };  /* a scheduled retry is recorded, not run */
  const AbortController_ = function () { this.signal = {}; this.abort = function () {}; };

  const build = new Function(
    "document", "window", "fetch", "setTimeout", "AbortController", "TextDecoder", "Date",
    source + "; return liveStream;");
  const liveStream = build(document_, window_, fetch_, setTimeout_, AbortController_,
                           TextDecoder, Date_);

  liveStream({
    onFrame: (frame) => frames.push(frame),
    onState: (state, n) => states.push(n === undefined ? state : state + ":" + n),
    headers: () => ({}),
  });
  return { frames, states, opened: () => opened };
}

/* ---- reader two: the strip's own, used by the five pages without one -------------------------- */
function driveStrip(connections) {
  const marker = page.indexOf("/* The shared live strip.");
  if (marker < 0) { throw new Error("the strip script is not on this page"); }
  const scriptStart = page.lastIndexOf("<script>", marker) + "<script>".length;
  const source = page.slice(scriptStart, page.indexOf("</script>", scriptStart));

  const els = {};
  ["liveStrip", "liveEnc", "liveImp", "liveReq", "liveWarn", "liveDot", "liveNode",
   "liveWaiting", "key"].forEach((id) => {
    els[id] = { id, hidden: true, innerHTML: "", className: "", value: "",
                addEventListener() {}, setAttribute() {}, removeAttribute() {} };
  });

  let opened = 0;
  const clock = makeClock();
  const win = {
    document: { getElementById: (id) => els[id] || null, addEventListener() {}, hidden: false },
    addEventListener() {},
    setTimeout: () => 0,
    sessionStorage: { getItem: () => "" },
    Number, JSON, String, Math, Date: clock.Date,
    TextDecoder,
    AbortController: function () { this.signal = {}; this.abort = function () {}; },
    fetch: function () {
      const plan = connections[Math.min(opened, connections.length - 1)];
      opened += 1;
      return Promise.resolve({
        ok: true, status: 200,
        body: { getReader: () => reader(clock, plan.blocks,
                                        { lastFor: plan.lastFor, thenHang: plan.thenHang }) },
      });
    },
  };
  win.window = win;

  const run = new Function("window", "document", "setTimeout", "sessionStorage", "fetch",
                           "AbortController", "TextDecoder", "Date", source);
  run(win, win.document, win.setTimeout, win.sessionStorage, win.fetch,
      win.AbortController, win.TextDecoder, clock.Date);
  return { els, opened: () => opened };
}

/* ---- the checks ------------------------------------------------------------------------------ */
function settle(times, fn) {
  if (times <= 0) { return fn(); }
  setImmediate(() => settle(times - 1, fn));
}

const longLived = { blocks: [RETRY, STATUS, KEEPALIVE, UNKNOWN, GOODBYE], lastFor: 600000 };
const shortLived = { blocks: [RETRY, STATUS, GOODBYE], lastFor: 0 };
const droppedWithoutAWord = { blocks: [RETRY, STATUS], lastFor: 600000 };
/* Whatever comes after the connection under test just stays open. */
const staysOpen = { blocks: [RETRY, STATUS], lastFor: 600000, thenHang: true };

/* Only two pages run their own stream; the other five read through the strip. Skipped rather than
   quietly passed, so a page that lost its reader cannot look like a page that never had one. */
const hasLiveStream = extractLiveStream() !== null;
const planned = hasLiveStream ? driveLiveStream([longLived, staysOpen]) : null;
const tooSoon = hasLiveStream ? driveLiveStream([shortLived, staysOpen]) : null;
const unannounced = hasLiveStream ? driveLiveStream([droppedWithoutAWord, staysOpen]) : null;
const strip = driveStrip([longLived, staysOpen]);
const stripUnannounced = driveStrip([droppedWithoutAWord, staysOpen]);
const stripTooSoon = driveStrip([shortLived, staysOpen]);

settle(12, () => {
  if (!hasLiveStream) {
    console.log("skip liveStream is not on this page; it reads through the strip");
  } else {
    /* Non-vacuous first: every check below is about what did NOT arrive, and all of them pass
       against a reader that was never given anything. */
    ok("the reader was given a frame to deliver", planned.frames.length >= 1,
       JSON.stringify(planned.frames));

    ok("a status frame reaches the frame handler",
       planned.frames.some((f) => f && f.warnings === 3));
    ok("a goodbye does not reach the frame handler",
       !planned.frames.some((f) => f && f.reason === "stream_max_age"),
       JSON.stringify(planned.frames));
    ok("a keepalive does not reach the frame handler",
       planned.frames.every((f) => f && f.warnings !== undefined));
    ok("an event this page does not know does not reach the frame handler",
       !planned.frames.some((f) => f && f.warnings === 99),
       JSON.stringify(planned.frames));

    ok("a planned close is not reported as a fault",
       !planned.states.some((s) => s.indexOf("retrying") === 0),
       JSON.stringify(planned.states));
    ok("a planned close reconnects", planned.opened() > 1, "opened " + planned.opened());

    ok("a close with no goodbye is still reported as a fault",
       unannounced.states.some((s) => s.indexOf("retrying") === 0),
       JSON.stringify(unannounced.states));

    ok("a goodbye within a second of connecting does not reconnect at once",
       tooSoon.opened() === 1, "opened " + tooSoon.opened());
    ok("a goodbye within a second of connecting backs off instead",
       tooSoon.states.some((s) => s.indexOf("retrying") === 0),
       JSON.stringify(tooSoon.states));
  }

  /* The strip, which is the reader the other five pages use. */
  ok("the strip drew the frame it was given", strip.els.liveWarn.innerHTML.indexOf("3") >= 0,
     JSON.stringify(strip.els.liveWarn));
  ok("the strip did not render the goodbye over it",
     strip.els.liveWarn.innerHTML.indexOf("3") >= 0 && strip.els.liveWarn.hidden === false,
     JSON.stringify(strip.els.liveWarn));
  ok("a planned close leaves the strip's dot alone",
     strip.els.liveDot.className.indexOf("stale") < 0,
     JSON.stringify(strip.els.liveDot.className));
  ok("a planned close reconnects the strip", strip.opened() > 1, "opened " + strip.opened());
  ok("a close with no goodbye still marks the strip stale",
     stripUnannounced.els.liveDot.className.indexOf("stale") >= 0,
     JSON.stringify(stripUnannounced.els.liveDot.className));
  ok("a strip goodbye within a second of connecting does not reconnect at once",
     stripTooSoon.opened() === 1, "opened " + stripTooSoon.opened());

  console.log(failures ? "\n" + failures + " failure(s)" : "\nall good");
  process.exit(failures ? 1 : 0);
});
