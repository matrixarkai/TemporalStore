/* Follow a link that names a place inside a tab, the way the status strip does.
 *
 * The live strip on every portal page links to "/v1/admin/setup#encoding" and
 * "/v1/admin/setup#traffic". Both name a div inside a tab pane, and every pane but the first loads
 * hidden -- so the browser's one attempt to scroll to the fragment finds nothing on screen, the
 * default tab comes up, and the badge that was reporting a problem has quietly delivered the reader
 * somewhere else. A link cannot report that it failed; it went to the page it named.
 *
 * Whether that now works is a question about NESTING -- which pane contains the target -- and a
 * flat stub of a DOM cannot answer it. It would happily agree with any answer, including the wrong
 * one, which is how three panes on the key portal shipped spliced into the wrong parents with every
 * harness green. So containment here is read out of the page's own markup by matching <section>
 * against </section>, and the extents that come out are checked for sanity before anything is built
 * on them.
 *
 * Usage: node deep_link_harness.js <page.html> [#fragment ...]
 */
"use strict";
const fs = require("fs");

const path = process.argv[2];
const wanted = process.argv.slice(3).map((f) => (f.charAt(0) === "#" ? f : "#" + f));
const page = fs.readFileSync(path, "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- containment, read from the markup ---------- */
/* Script blocks build markup out of strings -- "<section>" appears in them with no closing tag in
   sight -- and an unbalanced tag inside one would run the depth count off the end of the document.
   Blanked rather than removed so every index below still points into the real file. */
const markup = page.replace(/<script[\s\S]*?<\/script>/g, (m) => " ".repeat(m.length));

function extentFrom(start) {
  const scan = /<section\b|<\/section>/g;
  scan.lastIndex = start;
  let depth = 0;
  let match;
  while ((match = scan.exec(markup)) !== null) {
    depth += match[0] === "</section>" ? -1 : 1;
    if (depth === 0) { return match.index + match[0].length; }
  }
  return -1;
}

const panes = [];
const paneOpen = /<section[^>]*class="pane"[^>]*>/g;
let open;
while ((open = paneOpen.exec(markup)) !== null) {
  const id = (open[0].match(/\sid="([^"]*)"/) || [])[1] || null;
  panes.push({ id, start: open.index, end: extentFrom(open.index),
               hidden: /\shidden[\s>]/.test(open[0]) });
}

if (!panes.length) { console.log("FAIL the page declares no panes"); process.exit(1); }

/* ---------- the extents have to be worth trusting ---------- */
/* A scan that silently matched nothing, or ran to the end of the file, would make every
   containment answer below come out "no" -- and every assertion that a link does NOT go somewhere
   would pass. So the extents are checked before they are used. */
ok("every pane's extent was found", panes.every((p) => p.end > p.start),
   panes.map((p) => p.id + ":" + p.start + ".." + p.end).join(" "));
ok("every pane is named", panes.every((p) => p.id));
ok("the panes do not overlap and are in order",
   panes.every((p, i) => i === 0 || panes[i - 1].end <= p.start),
   panes.map((p) => p.id + ":" + p.start + ".." + p.end).join(" "));
ok("no pane's extent swallows the rest of the document",
   panes[panes.length - 1].end <= markup.length);

const ids = new Map();
const idAttr = /\sid="([^"]+)"/g;
let found;
while ((found = idAttr.exec(markup)) !== null) {
  if (!ids.has(found[1])) { ids.set(found[1], found.index); }
}

function paneHolding(id) {
  if (!ids.has(id)) { return null; }
  const at = ids.get(id);
  return panes.find((p) => at > p.start && at < p.end) || null;
}

const inside = [...ids.keys()].filter((id) => {
  const holder = paneHolding(id);
  return holder && holder.id !== id;
});
ok("some element is nested inside a pane", inside.length > 0,
   "nothing is contained by anything; the containment test would be vacuous");

/* ---------- a DOM that knows what contains what ---------- */
function makeEl(spec) {
  return Object.assign({
    id: spec.id,
    hidden: !!spec.hidden,
    tabIndex: 0,
    scrolled: 0,
    dataset: Object.assign({}, spec.dataset),
    attrs: Object.assign({}, spec.attrs),
    addEventListener() {},
    setAttribute(k, v) { this.attrs[k] = String(v); },
    getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; },
    focus() {},
    click() {},
    scrollIntoView() { this.scrolled += 1; },
    closest() { return null; },
  });
}

const tabTags = [...markup.matchAll(/<button[^>]*role="tab"[\s\S]*?>/g)].map((m) => m[0]);
if (!tabTags.length) { console.log("FAIL the page declares no tabs"); process.exit(1); }

function build(hash) {
  const tabs = tabTags.map((tag) => makeEl({
    id: (tag.match(/\sid="([^"]*)"/) || [])[1],
    dataset: { pane: (tag.match(/data-pane="([^"]*)"/) || [])[1] },
    attrs: { "aria-selected": (tag.match(/aria-selected="([^"]*)"/) || [])[1] || "false" },
  }));
  const paneEls = panes.map((p) => makeEl({ id: p.id, hidden: p.hidden }));
  const byId = new Map();
  tabs.concat(paneEls).forEach((el) => byId.set(el.id, el));

  const doc = {
    querySelectorAll(selector) {
      if (selector === ".tabs button") { return tabs; }
      if (selector === ".pane") { return paneEls; }
      return [];
    },
    getElementById(id) {
      if (byId.has(id)) { return byId.get(id); }
      if (!ids.has(id)) { return null; }
      const holder = paneHolding(id);
      const el = makeEl({ id });
      /* The whole point: the real ancestor, taken from the real markup. */
      el.closest = (selector) =>
        (selector === ".pane" && holder) ? byId.get(holder.id) || null : null;
      byId.set(id, el);
      return el;
    },
  };

  const listeners = {};
  const win = {
    location: { hash: hash || "" },
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
  };
  const src = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)]
    .map((m) => m[1]).find((s) => s.includes("window.wireTabs = function"));
  if (!src) { console.log("FAIL the page carries no tab helper"); process.exit(1); }
  new Function("window", "document", src)(win, doc);
  win.wireTabs();

  return {
    doc,
    selected: () => tabs.filter((t) => t.getAttribute("aria-selected") === "true").map((t) => t.id),
    visible: () => paneEls.filter((p) => !p.hidden).map((p) => p.id),
    go(next) {
      win.location.hash = next;
      (listeners.hashchange || []).forEach((fn) => fn({}));
    },
    listens: (type) => (listeners[type] || []).length > 0,
  };
}

/* ---------- what a fresh page with no fragment does ---------- */
const plain = build("");
const fallback = plain.selected().join();
ok("with no fragment the page opens on its own default tab", plain.selected().length === 1,
   plain.selected().join(", "));
ok("with no fragment exactly one pane is visible", plain.visible().length === 1);

/* ---------- the fragments the strip actually links to ---------- */
/* Derived, not listed: whichever ids the caller names, plus one taken from a pane that is not the
   default, so this file still has something to check on a page nobody passed fragments for. */
const derived = inside.filter((id) => {
  const holder = paneHolding(id);
  return holder && "tab-" + holder.id.replace(/^pane-/, "") !== fallback;
});
const checks = wanted.length ? wanted : (derived.length ? ["#" + derived[0]] : []);
ok("there is a fragment to follow", checks.length > 0,
   "no id lives inside a pane other than the default one");

checks.forEach((fragment) => {
  const name = fragment.replace(/^#/, "");
  const holder = paneHolding(name);
  ok("the page has somewhere to send " + fragment, !!holder,
     ids.has(name) ? "the id exists but is not inside any pane" : "no element has that id");
  if (!holder) { return; }

  const run = build(fragment);
  const want = "tab-" + holder.id.replace(/^pane-/, "");
  ok("arriving at " + fragment + " opens the tab that holds it",
     run.selected().join() === want, run.selected().join(", ") + " (wanted " + want + ")");
  ok("arriving at " + fragment + " leaves exactly one pane visible",
     run.visible().join() === holder.id, run.visible().join(", "));
  ok("arriving at " + fragment + " scrolls to it once the pane is showing",
     run.doc.getElementById(name).scrolled > 0,
     "the pane opened but the reader is left at the top of it");
});

/* ---------- the same page, no navigation ---------- */
/* The strip is on the setup page too. Clicking a segment that points into this page loads nothing:
   the URL gains a fragment and that is the entire event. */
if (checks.length) {
  const name = checks[0].replace(/^#/, "");
  const holder = paneHolding(name);
  if (holder) {
    const run = build("");
    ok("the page listens for the fragment changing under it", run.listens("hashchange"));
    run.go(checks[0]);
    ok("changing the fragment without reloading still opens the tab",
       run.selected().join() === "tab-" + holder.id.replace(/^pane-/, ""),
       run.selected().join(", "));
  }
}

/* ---------- fragments that mean nothing here ---------- */
const unknown = build("#no-such-anchor-exists");
ok("a fragment naming nothing leaves the default tab up", unknown.selected().join() === fallback,
   unknown.selected().join(", "));

const outside = [...ids.keys()].find((id) => !paneHolding(id));
if (outside) {
  const run = build("#" + outside);
  ok("a fragment outside the tabs (#" + outside + ") leaves the default tab up",
     run.selected().join() === fallback, run.selected().join(", "));
}

const weird = build("#%E0%A4%A");
ok("a fragment that is not valid escaping is survivable", weird.selected().join() === fallback,
   weird.selected().join(", "));

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
