/* Run the Setup page, hand it one overview response, and read what the three panels drew.
 *
 * The claim under test is not just that each panel renders. It is that ONE response feeds all
 * three: the overview already carried the footprint, the budget and the timing, and asking three
 * times for the same answer would be three times the probing. A fetch count is the only way to see
 * that from outside.
 *
 * Usage: node overview_panels_harness.js <setup_portal.html> [differ|aligned]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "differ";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const HOOK_BUDGET = mode === "aligned" ? 500000 : 10000;
const OVERVIEW = {
  status: "ok",
  footprint: {
    worker: { resident_bytes: 412 * 1048576, peak_bytes: 455 * 1048576,
              source: "/proc/self/status", workers: 1 },
    engine: { available: false }
  },
  latency: mode === "aligned" ? {
    available: true, deadline_ms: 0, deadline_is_cooperative: true,
    transport_request_timeout_ms: 60000, transport_io_timeout_ms: 60000,
    bounds_a_slow_call: "transport_request_timeout_ms",
    worst_case_single_call_ms: 60000, deadline_can_be_overrun_by_ms: 0
  } : {
    available: true, deadline_ms: 5000, deadline_is_cooperative: true,
    transport_request_timeout_ms: 60000, transport_io_timeout_ms: 60000,
    bounds_a_slow_call: "transport_request_timeout_ms",
    worst_case_single_call_ms: 60000, deadline_can_be_overrun_by_ms: 55000
  },
  config: {
    skills: {
      budgets: {
        available: true,
        context_budget_tokens: 500000,
        skills: { percent: 10, asked_percent: 10, guard_percent: 50, tokens: 50000,
                  ceiling_tokens: 262144, by_percentage_tokens: 50000,
                  bound_by: "percentage" },
        resources: { percent: 25, asked_percent: 40, guard_percent: 50, tokens: 125000,
                     ceiling_tokens: 262144, by_percentage_tokens: 125000,
                     bound_by: "share_guard" },
        paths: [
          { path: "api", label: "API callers", context_budget_tokens: 500000,
            variable: "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS",
            sections: { skills: 50000, resources: 125000 } },
          { path: "agent_hooks", label: "Agent hooks", context_budget_tokens: HOOK_BUDGET,
            variable: "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS",
            sections: { skills: HOOK_BUDGET / 10, resources: HOOK_BUDGET / 4 } }
        ],
        paths_differ: mode !== "aligned"
      }
    }
  }
};

const els = new Map();
function makeEl(id) {
  return {
    id: id || "", hidden: false, textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [], _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
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

let overviewCalls = 0;

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
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    const u = String(url);
    if (u.indexOf("/v1/admin/overview") >= 0) {
      overviewCalls += 1;
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve(OVERVIEW),
                               text: () => Promise.resolve(JSON.stringify(OVERVIEW)) });
    }
    if (u.indexOf("/v1/admin/events") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               body: { getReader: () => ({ read: () => new Promise(() => {}) }) },
                               json: () => Promise.resolve({}), text: () => Promise.resolve("") });
    }
    return Promise.resolve({ ok: true, status: 200,
                             json: () => Promise.resolve({ settings: {} }),
                             text: () => Promise.resolve("") });
  }
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw: " + e.message); failures += 1; }
});

setTimeout(function () {
  const budgets = el("budgets").innerHTML;
  const latency = el("latency").innerHTML;
  const footprint = el("footprint").innerHTML;

  /* The whole reason these three sit on one fetch. */
  ok("one response fed all three panels", overviewCalls === 1,
     "overview calls: " + overviewCalls);
  ok("the footprint still drew", /412\.0 MB/.test(footprint), footprint.slice(0, 200));

  ok("the budget panel names both callers",
     /API callers/.test(budgets) && /Agent hooks/.test(budgets), budgets.slice(0, 400));
  ok("the API budget is shown with separators", /500,000/.test(budgets), budgets.slice(0, 400));
  ok("a share says which limit set it", /set by percentage/.test(budgets),
     budgets.slice(0, 700));
  /* A percentage the guard is holding down is the defect the panel exists to show. */
  ok("a share held by the guard says so", /set by share guard/.test(budgets),
     budgets.slice(0, 700));

  if (mode === "aligned") {
    ok("no difference is claimed when there is none", !/different budgets/.test(budgets),
       budgets.slice(0, 700));
    ok("no deadline means nothing to overrun", /nothing to overrun/.test(latency),
       latency.slice(0, 500));
    /* The cell itself, not the note beside it. A mutation that printed
       `transport - deadline` here left the note reading "nothing to overrun" next to a number,
       and a check for one particular figure missed it. */
    ok("and no overrun figure is invented",
       /overrun the deadline by<\/td><td class="num">—</.test(latency),
       latency.slice(0, 700));
  } else {
    ok("the agent's smaller budget is shown, not the API one",
       /10,000/.test(budgets), budgets.slice(0, 400));
    ok("and the page says the two callers differ", /different budgets/.test(budgets),
       budgets.slice(0, 700));
    ok("the overrun an operator is looking for is shown", /55,000 ms/.test(latency),
       latency.slice(0, 500));
    ok("with the lever that closes it", /lower the transport timeout/.test(latency),
       latency.slice(0, 500));
  }

  ok("the deadline is described as cooperative", /cannot stop a call in flight/.test(latency),
     latency.slice(0, 500));
  ok("the transport timeout is named as what a call runs against",
     /what one slow call actually runs against/.test(latency), latency.slice(0, 500));

  console.log(failures ? "\n" + failures + " failed" : "\nall ok");
  process.exit(failures ? 1 : 0);
}, 60);
