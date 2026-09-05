/* Run the "awaiting restart" panel: what it names, and where clicking a name goes.
 *
 * The strip counts the settings waiting for a restart from every page. Naming them is this panel's
 * whole job, and taking somebody to one is harder than scrolling: four of the nine groups render
 * shut, and 26 of the 47 restart-scoped settings live in one of them. A name that scrolls to a
 * folded row lands on a closed triangle, which looks exactly like a working link.
 *
 * Extracted and run rather than read, in the manner of the byte-hint harness beside this one: a
 * renderer that never opens the group reads the same in a diff as one that does.
 *
 * Usage: node awaiting_restart_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function extract(marker) {
  const start = page.indexOf(marker);
  if (start < 0) { throw new Error("not on this page: " + marker); }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  throw new Error("unbalanced: " + marker);
}

/* ---------- a DOM with real containment ---------- */
/* Flat stubs agree with whatever they are told, and everything here is a question about what is
   inside what, so the nodes below carry a parent and answer `closest` by walking it. */
function node(tag, attrs) {
  const el = {
    tagName: tag.toUpperCase(),
    parent: null,
    children: [],
    attrs: attrs || {},
    hidden: false,
    open: false,
    innerHTML: "",
    scrolled: false,
    focused: false,
    classes: new Set(),
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name)
      ? this.attrs[name] : null; },
    scrollIntoView() { this.scrolled = true; },
    focus() { this.focused = true; },
    append(child) { child.parent = this; this.children.push(child); return child; },
    closest(selector) {
      let cursor = this;
      while (cursor) {
        if (selector === "details" && cursor.tagName === "DETAILS") { return cursor; }
        if (selector === ".field" && cursor.classes.has("field")) { return cursor; }
        cursor = cursor.parent;
      }
      return null;
    },
    querySelector(selector) {
      const key = /\[data-key="([^"]+)"\]/.exec(selector);
      const walk = (n) => {
        if (key && n.attrs["data-key"] === key[1]) { return n; }
        for (const child of n.children) {
          const found = walk(child);
          if (found) { return found; }
        }
        return null;
      };
      return walk(this);
    },
  };
  el.classList = {
    add: (c) => el.classes.add(c),
    remove: (c) => el.classes.delete(c),
    contains: (c) => el.classes.has(c),
  };
  return el;
}

/* The shape the settings pane really has: a shut advanced group, holding a field row, holding the
   control. */
const groups = node("div");
const group = groups.append(node("details"));
group.open = false;
const field = group.append(node("div"));
field.classes.add("field");
field.classes.add("hidden");
const control = field.append(node("select", { "data-key": "ingestion.bulk_ingest" }));

const box = node("div");
box.hidden = true;
const saveMsg = node("div");

const byId = { groups, awaitingRestart: box, saveMsg };
const shown = [];
const said = [];

const $ = (id) => byId[id] || null;
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const say = (el, text) => said.push(text);
const window_ = { showTab: (pane) => shown.push(pane) };

const build = new Function("$", "esc", "say", "window",
  extract("function showSetting(key)") + "\n" +
  extract("function renderAwaiting(keys)") + "\n" +
  "return { showSetting: showSetting, renderAwaiting: renderAwaiting };");
const panel = build($, esc, say, window_);

/* ---------- what it names ---------- */
panel.renderAwaiting([]);
ok("nothing waiting: the panel is not shown", box.hidden === true);
ok("nothing waiting: it says nothing", box.innerHTML === "", JSON.stringify(box.innerHTML));

panel.renderAwaiting(["ingestion.bulk_ingest", "extraction.provider"]);
ok("the panel appears when something is waiting", box.hidden === false);
ok("it says how many", box.innerHTML.indexOf("2 settings are waiting") >= 0, box.innerHTML);
ok("it names the first", box.innerHTML.indexOf("ingestion.bulk_ingest") >= 0);
ok("it names the second", box.innerHTML.indexOf("extraction.provider") >= 0);
ok("each name is something you can click",
   (box.innerHTML.match(/data-goto="/g) || []).length === 2, box.innerHTML);

panel.renderAwaiting(["extraction.provider"]);
ok("one waiting reads as one", box.innerHTML.indexOf("1 setting is waiting") >= 0, box.innerHTML);

/* ---------- where a name takes you ---------- */
ok("FLOOR: the group starts shut", group.open === false);
ok("FLOOR: the row starts hidden", field.classes.has("hidden") === true);

const went = panel.showSetting("ingestion.bulk_ingest");
ok("it reports having gone somewhere", went === true);
ok("the group it was folded into is opened", group.open === true);
ok("the group is not left hidden", group.hidden === false);
ok("a filtered-out row is uncovered", field.classes.has("hidden") === false);
ok("the settings tab is opened", shown.indexOf("settings") >= 0, JSON.stringify(shown));
ok("the control is scrolled to", control.scrolled === true);
ok("and focused, so it is obvious which one", control.focused === true);

/* ---------- a name for a setting that is not loaded ---------- */
const missing = panel.showSetting("not.a.setting");
ok("an unloaded setting is refused rather than silently doing nothing", missing === false);
ok("and says why", said.some((t) => /admin key/.test(t)), JSON.stringify(said));

/* ---------- the wiring, run rather than read ---------- */
const listenerSource = (() => {
  const start = page.indexOf('$("awaitingRestart").addEventListener("click"');
  if (start < 0) { return null; }
  /* Match from addEventListener's OWN parenthesis. Starting at the first "(" after `start` closes
     on `$("awaitingRestart")` and yields a statement that registers nothing -- which then reads as
     "the page has no handler" rather than "the harness looked in the wrong place". */
  let depth = 0;
  for (let i = page.indexOf("(", page.indexOf("addEventListener", start)); i < page.length; i++) {
    if (page[i] === "(") { depth += 1; }
    else if (page[i] === ")") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  return null;
})();
ok("the panel has a click handler at all", listenerSource !== null);
if (listenerSource) {
  let handler = null;
  const capturing = (id) => ({ addEventListener: (_name, fn) => { handler = fn; } });
  new Function("$", "showSetting", listenerSource + ";")(capturing, () => {});
  ok("the handler is registered on click", typeof handler === "function");

  const visited = [];
  new Function("$", "showSetting", listenerSource + ";")(
    capturing, (key) => visited.push(key));
  handler({ target: { getAttribute: (n) => (n === "data-goto" ? "extraction.provider" : null) } });
  ok("clicking a name goes to that setting",
     visited.indexOf("extraction.provider") >= 0, JSON.stringify(visited));

  visited.length = 0;
  handler({ target: { getAttribute: () => null } });
  ok("clicking the surrounding text goes nowhere", visited.length === 0);
}

/* ---------- and that something actually calls it ---------- */
/* Everything above proves the panel works when run. None of it proves the page ever runs it, which
   is the failure this whole change is about -- a thing built and then delivered to nobody. So the
   renderer that draws the settings is extracted and called too, with the panel stubbed, and asked
   whether it passed the pending list along. */
{
  const groupsBox = node("div");
  const cfgTime = node("div");
  cfgTime.textContent = "";
  const ids = { groups: groupsBox, cfgTime: cfgTime };
  const handed = [];

  const renderGroups = new Function(
    "$", "esc", "fieldHtml", "renderAwaiting", "applyFilter", "markDirty",
    "var fields;" + extract("function renderGroups(settings)") + "; return renderGroups;")(
    (id) => ids[id] || node("div"),
    esc,
    (f) => '<div class="field" data-field="' + f.key + '"></div>',
    (keys) => handed.push(keys),
    () => {},
    () => {});

  renderGroups({
    groups: { ingestion: [{ key: "ingestion.bulk_ingest" }] },
    group_meta: { ingestion: { advanced: true, title: "Ingestion" } },
    pending_restart: ["ingestion.bulk_ingest"],
  });

  ok("FLOOR: the settings renderer drew its groups",
     groupsBox.innerHTML.indexOf("ingestion.bulk_ingest") >= 0, groupsBox.innerHTML.slice(0, 120));
  ok("drawing the settings also draws the waiting panel", handed.length === 1,
     "renderAwaiting was called " + handed.length + " times");
  ok("and hands it what the deployment says is waiting",
     JSON.stringify(handed[0]) === JSON.stringify(["ingestion.bulk_ingest"]),
     JSON.stringify(handed[0]));

  /* The group this setting is in renders SHUT, which is the reason the panel has to exist. */
  ok("FLOOR: an advanced group really does render shut",
     / open>/.test(groupsBox.innerHTML) === false, groupsBox.innerHTML.slice(0, 160));
}

console.log(failures ? "\n" + failures + " failure(s)" : "\nall good");
process.exit(failures ? 1 : 0);
