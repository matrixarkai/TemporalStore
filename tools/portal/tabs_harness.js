/* Run a portal page's tabs the way someone using them would, with a mouse and with a keyboard.
 *
 * The markup cannot answer the questions that matter. Whether a tablist responds to an arrow key,
 * whether exactly one pane is ever visible, whether the tab a screen reader reports as selected is
 * the pane actually on screen -- all of that is behaviour, and a page whose tabs are wired to
 * nothing looks in source exactly like a page whose tabs work. The panel on the key portal shipped
 * broken three times for precisely that reason.
 *
 * So the tab buttons and panes are read out of the real page, the real helper is executed against
 * them, and events are dispatched through the listeners it registered.
 *
 * Usage: node tabs_harness.js <page.html>
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

/* ---------- the page's own tabs and panes ---------- */
function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}

const tabTags = [...page.matchAll(/<button[^>]*role="tab"[\s\S]*?>/g)].map((m) => m[0]);
const paneTags = [...page.matchAll(/<section[^>]*class="pane"[\s\S]*?>/g)].map((m) => m[0]);

if (!tabTags.length) { console.log("FAIL the page declares no tabs"); process.exit(1); }

/* ---------- a DOM just wide enough ---------- */
function makeEl(spec) {
  const listeners = {};
  const attrs = Object.assign({}, spec.attrs);
  return {
    id: spec.id,
    hidden: !!spec.hidden,
    tabIndex: 0,
    focused: false,
    dataset: Object.assign({}, spec.dataset),
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    dispatch(type, event) { (listeners[type] || []).forEach((fn) => fn(event || {})); },
    listens(type) { return (listeners[type] || []).length > 0; },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return k in attrs ? attrs[k] : null; },
    focus() { this.focused = true; },
    /* Real buttons have this, and showTab uses it to reach the same path a person's click takes. */
    click() { this.dispatch("click"); },
  };
}

const tabs = tabTags.map((tag) => makeEl({
  id: attr(tag, "id"),
  dataset: { pane: attr(tag, "data-pane") },
  attrs: { "aria-selected": attr(tag, "aria-selected") || "false" },
}));
const panes = paneTags.map((tag) => makeEl({ id: attr(tag, "id"), hidden: /\shidden[\s>]/.test(tag) }));

const byId = new Map();
tabs.concat(panes).forEach((el) => byId.set(el.id, el));

const doc = {
  querySelectorAll(selector) {
    if (selector === ".tabs button") { return tabs; }
    if (selector === ".pane") { return panes; }
    return [];
  },
  getElementById(id) { return byId.get(id) || null; },
};

/* ---------- the page's own helper ---------- */
const src = scripts.find((s) => s.includes("window.wireTabs"));
if (!src) { console.log("FAIL the page carries no tab helper"); process.exit(1); }
const win = {};
new Function("window", "document", src)(win, doc);
ok("the page defines wireTabs", typeof win.wireTabs === "function");
ok("the page defines markTab", typeof win.markTab === "function");

/* ---------- structure: every tab has its pane, and the reverse ---------- */
const paneIds = new Set(panes.map((p) => p.id));
const missing = tabs.filter((t) => !paneIds.has("pane-" + t.dataset.pane));
ok("every tab points at a pane that exists", missing.length === 0,
   missing.map((t) => t.id + " -> pane-" + t.dataset.pane).join(", "));
const tabTargets = new Set(tabs.map((t) => "pane-" + t.dataset.pane));
const orphans = panes.filter((p) => !tabTargets.has(p.id));
ok("every pane is reachable from a tab", orphans.length === 0,
   orphans.map((p) => p.id).join(", "));

/* ---------- wiring ---------- */
const shown = [];
win.wireTabs((pane) => shown.push(pane));

function visible() { return panes.filter((p) => !p.hidden).map((p) => p.id); }
function selected() {
  return tabs.filter((t) => t.getAttribute("aria-selected") === "true").map((t) => t.id);
}

ok("exactly one pane is visible after wiring", visible().length === 1, visible().join(", "));
ok("exactly one tab is selected after wiring", selected().length === 1, selected().join(", "));
ok("the visible pane is the one the selected tab controls",
   visible()[0] === "pane-" + tabs.find((t) => t.getAttribute("aria-selected") === "true").dataset.pane);
ok("only the selected tab is in the tab order",
   tabs.every((t) => t.tabIndex === (t.getAttribute("aria-selected") === "true" ? 0 : -1)),
   tabs.map((t) => t.id + ":" + t.tabIndex).join(" "));

/* ---------- mouse ---------- */
const third = tabs[Math.min(2, tabs.length - 1)];
third.dispatch("click");
ok("clicking a tab shows its pane", visible().join() === "pane-" + third.dataset.pane);
ok("clicking a tab leaves exactly one pane visible", visible().length === 1, visible().join(", "));
ok("clicking a tab selects only it", selected().join() === third.id, selected().join(", "));
ok("the caller is told which pane was shown", shown[shown.length - 1] === third.dataset.pane,
   JSON.stringify(shown));

/* ---------- keyboard ---------- */
ok("tabs listen for keys at all", tabs.every((t) => t.listens("keydown")));

function press(tab, key) {
  let prevented = false;
  tab.dispatch("keydown", { key, preventDefault() { prevented = true; } });
  return prevented;
}

const first = tabs[0];
const last = tabs[tabs.length - 1];

first.dispatch("click");
press(first, "ArrowRight");
ok("ArrowRight moves to the next tab", selected().join() === tabs[1].id, selected().join(", "));
ok("the arrow key moves focus with the selection", tabs[1].focused);

press(tabs[1], "ArrowLeft");
ok("ArrowLeft moves back", selected().join() === first.id, selected().join(", "));

press(first, "ArrowLeft");
ok("ArrowLeft from the first tab wraps to the last", selected().join() === last.id,
   selected().join(", "));

press(last, "ArrowRight");
ok("ArrowRight from the last tab wraps to the first", selected().join() === first.id,
   selected().join(", "));

press(first, "End");
ok("End jumps to the last tab", selected().join() === last.id, selected().join(", "));
press(last, "Home");
ok("Home jumps to the first tab", selected().join() === first.id, selected().join(", "));

ok("a handled key stops the page scrolling under it", press(first, "ArrowRight") === true);
ok("an unrelated key is left alone", press(first, "PageDown") === false);
const before = selected().join();
press(tabs[1], "Enter");
ok("Enter does not change the selection", selected().join() === before);

/* ---------- the badge ---------- */
const anyTab = tabs[0];
win.markTab(anyTab.dataset.pane, 3);
ok("markTab puts a count on the tab", anyTab.getAttribute("data-badge") === "3",
   String(anyTab.getAttribute("data-badge")));
win.markTab(anyTab.dataset.pane, 0);
ok("a count of zero clears the badge rather than showing 0",
   anyTab.getAttribute("data-badge") === "", String(anyTab.getAttribute("data-badge")));
win.markTab("no-such-pane", 5);
ok("marking a pane that does not exist is survivable", true);

/* ---------- showing a pane from code ---------- */
ok("the page defines showTab", typeof win.showTab === "function");
if (typeof win.showTab === "function") {
  first.dispatch("click");
  var target = tabs[tabs.length - 1];
  win.showTab(target.dataset.pane);
  ok("showTab selects the pane it was given", selected().join() === target.id, selected().join(", "));
  ok("showTab leaves exactly one pane visible", visible().length === 1, visible().join(", "));
  win.showTab("no-such-pane");
  ok("showTab on a pane that does not exist changes nothing",
     selected().join() === target.id, selected().join(", "));
}

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
