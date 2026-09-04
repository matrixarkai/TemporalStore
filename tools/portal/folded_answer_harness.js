/* Follow a link that names something folded away.
 *
 * The key portal answers "where does the first admin key come from" inside a <details>, shut by
 * default because it is a question somebody has once. Three other pages link straight at it. A link
 * to a closed <details> arrives with the answer still folded up -- the reader is on the right page,
 * looking at a summary line, no worse informed than before -- and browsers disagree about whether
 * they expand it, so leaving it to them means it works for some customers and not others.
 *
 * The page's own helper is executed here rather than read: whether a <details> ends up open is
 * behaviour, and a page whose reveal helper is wired to nothing looks in source exactly like one
 * whose works.
 *
 * Usage: node folded_answer_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const path = process.argv[2];
const page = fs.readFileSync(path, "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- what the page actually declares ---------- */
const markup = page.replace(/<script[\s\S]*?<\/script>/g, (m) => " ".repeat(m.length));
const foldTags = [...markup.matchAll(/<details[^>]*>/g)].map((m) => m[0]);
const named = foldTags
  .map((tag) => ({ tag, id: (tag.match(/\sid="([^"]*)"/) || [])[1] }))
  .filter((d) => d.id);

ok("the page folds something away", foldTags.length > 0);
ok("a link can name it", named.length > 0,
   "no <details> carries an id, so nothing can be linked to");
if (!named.length) { console.log("FAILED " + failures); process.exit(1); }

named.forEach((fold) => {
  ok("#" + fold.id + " starts shut", !/\sopen[\s>]/.test(fold.tag),
     "it is open in the markup, so nothing below is testing anything");
});

/* ---------- a DOM with the one relationship that matters ---------- */
function makeEl(spec) {
  return {
    id: spec.id || null,
    tagName: spec.tagName,
    open: false,
    scrolled: 0,
    parentNode: spec.parentNode || null,
    scrollIntoView() { this.scrolled += 1; },
  };
}

const src = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)]
  .map((m) => m[1]).find((s) => s.includes("function reveal()"));
if (!src) { console.log("FAIL the page carries no reveal helper"); process.exit(1); }

/* `nest` puts the named element INSIDE the fold rather than being it -- the walk has to climb, and
   a helper that only ever looked at the element itself would pass every test that does not. */
function build(hash, nest) {
  const fold = makeEl({ tagName: "DETAILS", id: nest ? null : named[0].id });
  const wrapper = makeEl({ tagName: "DIV", parentNode: fold });
  const inner = makeEl({ tagName: "P", id: nest ? named[0].id : null, parentNode: wrapper });
  const loose = makeEl({ tagName: "P", id: "not-folded-at-all" });

  const byId = new Map();
  [fold, inner, loose].forEach((el) => { if (el.id) { byId.set(el.id, el); } });

  const listeners = {};
  const win = {
    location: { hash: hash || "" },
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
  };
  const doc = { getElementById: (id) => byId.get(id) || null };
  new Function("window", "document", src)(win, doc);

  return {
    fold,
    target: nest ? inner : fold,
    listens: (type) => (listeners[type] || []).length > 0,
    go(next) {
      win.location.hash = next;
      (listeners.hashchange || []).forEach((fn) => fn({}));
    },
  };
}

const anchor = "#" + named[0].id;

/* ---------- the link works ---------- */
const arrived = build(anchor, false);
ok("arriving at " + anchor + " unfolds it", arrived.fold.open === true,
   "the reader is on the right page looking at a folded summary");
ok("arriving at " + anchor + " scrolls to it", arrived.target.scrolled > 0);

const nested = build(anchor, true);
ok("a fragment naming something inside the fold unfolds it too", nested.fold.open === true,
   "the helper only looks at the element itself, not what encloses it");

/* ---------- and does not fire when it should not ---------- */
const plain = build("", false);
ok("with no fragment it stays shut", plain.fold.open === false,
   "opening it for everyone throws away the reason it is a <details>");

const other = build("#no-such-anchor", false);
ok("a fragment naming nothing leaves it shut", other.fold.open === false);

const loose = build("#not-folded-at-all", false);
ok("a fragment naming something outside any fold leaves it shut", loose.fold.open === false);

const weird = build("#%E0%A4%A", false);
ok("a fragment that is not valid escaping is survivable", weird.fold.open === false);

/* ---------- the same-page case ---------- */
const same = build("", false);
ok("the page listens for the fragment changing under it", same.listens("hashchange"));
same.go(anchor);
ok("changing the fragment without reloading unfolds it", same.fold.open === true);

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
