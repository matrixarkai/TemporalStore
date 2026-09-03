/* Run the Setup page and check it does not read the encoding backlog twice.
 *
 * `onFrame` already calls renderEncoding with the backlog the stream carries. A timer also fetches
 * /v1/admin/embeddings, and that endpoint walks the record log on the backend. While the stream is
 * live, the timer is a second read of one answer.
 *
 * Source cannot answer this. Whether the timer fetches depends on a closure variable set by a
 * callback the stream invokes on connecting -- so the page is run twice: once with a stream that
 * connects, once with one that fails. The second run is what stops the first from being vacuous:
 * a page that never polls at all would satisfy "does not poll while live" and be broken.
 *
 * Usage: node encoding_poll_harness.js <setup_portal.html>
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

const FRAME = 'event: status\ndata: {"ts":1,"traffic":{"routes":{}},"imports":{},"warnings":0,' +
              '"embedding":{"total":10,"encoded":10,"pending":0,"encoder":{}}}\n\n';

function run(streamWorks) {
  const fetched = [];
  const intervals = [];
  const els = new Map();

  function makeEl(id) {
    return {
      id: id || "", hidden: true, innerHTML: "", textContent: "",
      /* The timer returns early without a key. A harness that forgot would see no fetch and call
         the gate proven. */
      value: id === "key" ? "k-admin" : "",
      checked: true, className: "", style: {}, dataset: {}, files: [], options: [], children: [],
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

  function streamingResponse() {
    let sent = false;
    return Promise.resolve({
      ok: true, status: 200,
      body: {
        getReader() {
          return {
            read() {
              if (sent) { return new Promise(() => {}); }   // stays open, like a real stream
              sent = true;
              return Promise.resolve({ done: false, value: Buffer.from(FRAME, "utf8") });
            }
          };
        }
      },
      json: () => Promise.resolve({}), text: () => Promise.resolve("")
    });
  }

  function json(body) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body),
                             text: () => Promise.resolve(JSON.stringify(body)) });
  }

  const win = {
    document: {
      getElementById: (id) => el(id),
      querySelector: () => makeEl(), querySelectorAll: () => [],
      createElement: () => makeEl(), addEventListener() {},
      body: makeEl(), hidden: false
    },
    addEventListener() {},
    location: { origin: "http://localhost", href: "http://localhost", reload() {} },
    sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
    localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
    navigator: {},
    setTimeout: () => 0,
    setInterval: (fn, ms) => { intervals.push({ fn, ms }); return intervals.length; },
    clearInterval() {},
    Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
    encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
    TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
    AbortController: function () { this.abort = () => {}; this.signal = {}; },
    FileReader: function () {}, Blob: function () {},
    URL: { createObjectURL: () => "", revokeObjectURL() {} },
    fetch: (url) => {
      const u = String(url);
      fetched.push(u);
      if (u.indexOf("/v1/admin/events") >= 0) {
        return streamWorks ? streamingResponse()
                           : Promise.resolve({ ok: false, status: 503, body: null });
      }
      if (u.indexOf("/v1/admin/embeddings") >= 0) {
        return json({ total: 10, encoded: 10, pending: 0, encoder: {} });
      }
      if (u.indexOf("/v1/admin/config") >= 0) { return json({ settings: {} }); }
      return new Promise(() => {});
    },
    __matrixarkOnFrame() {}
  };
  win.window = win;

  const names = Object.keys(win);
  const values = names.map((k) => win[k]);
  scripts.forEach((body, index) => {
    try {
      new Function(...names, body)(...values);
    } catch (e) {
      console.log("FAIL script " + index + " threw at load: " + e.message);
      failures += 1;
    }
  });

  return {
    fetched, intervals,
    reads: () => fetched.filter((u) => u.indexOf("/v1/admin/embeddings") >= 0).length,
    opened: () => fetched.filter((u) => u.indexOf("/v1/admin/events") >= 0).length,
    fire: () => intervals.forEach((t) => { try { t.fn(); } catch (e) { /* its own bug */ } })
  };
}

async function turns(n) {
  for (let i = 0; i < n; i += 1) { await new Promise((r) => setImmediate(r)); }
}

(async function () {
  // ---- the stream connects: the timer must not ask for what the stream is delivering ----
  const live = run(true);
  await turns(30);
  ok("the page opened the stream", live.opened() > 0,
     "no stream was opened, so 'live' was never reached and the next check means nothing");
  ok("the page set an interval to fall back on", live.intervals.length > 0,
     "no timer exists, so there is nothing for the gate to stop");
  const beforeLive = live.reads();
  live.fire();
  ok("while the stream is live the timer does not read the backlog",
     live.reads() === beforeLive,
     "the timer fetched " + (live.reads() - beforeLive) + " more time(s) while the stream was live");

  // ---- the stream fails: the timer is the only source and must run ----
  const dead = run(false);
  await turns(30);
  const beforeDead = dead.reads();
  dead.fire();
  ok("with no stream, the timer reads the backlog", dead.reads() > beforeDead,
     "the fallback never fires, so a page whose stream is blocked shows a stale backlog for ever");

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
}());
