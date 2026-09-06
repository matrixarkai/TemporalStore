/* Run the Explore page's own loadBrowse with one of its two fetches failing, and read both panels.
 *
 * The two lists are loaded side by side by one function. When the subject list failed it was
 * BLANKED -- and an empty scope on this page has words ("No subjects hold memories in this scope
 * yet"), so a blank panel was a third state saying nothing at all. With the memory list succeeding
 * the screen contradicted itself: memories listed, and no subject holding any.
 *
 * `innerHTML = ""` and `innerHTML = "<div class=empty>...</div>"` are the same shape of statement,
 * so a diff cannot tell them apart. The shipped function is sliced out and run.
 *
 * Usage: node browse_failure_harness.js <explore_portal.html> [users|memories|both|none|empty]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const failing = process.argv[3] || "users";

const start = page.indexOf("function loadBrowse() {");
if (start < 0) { console.log("FAIL loadBrowse is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = {};
function el(id) {
  if (!els[id]) { els[id] = { id: id, innerHTML: "", textContent: "" }; }
  return els[id];
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const said = [];
function respond(body, good) {
  return Promise.resolve({
    ok: good, status: good ? 200 : 500,
    json: () => Promise.resolve(body)
  });
}

const scope = {
  $: el,
  esc,
  say: (element, text) => { element.innerHTML = text ? String(text) : ""; said.push(String(text)); },
  conn: () => {},
  auth: () => ({}),
  scopeQuery: () => "user_id=alice",
  failure: (status) => "The gateway answered " + status + ".",
  Date, JSON, String, Number, Promise,
  fetch: (url) => {
    const key = String(url);
    if (key.indexOf("/v1/users") === 0) {
      /* `empty` is a SUCCESSFUL call with nothing in it -- the state the failure message must not
         be confused with, and the one every other mode leaves untested because they all return a
         row. Without it, making the success path say "Not loaded." goes unnoticed. */
      return respond({ users: failing === "empty" ? [] : [{ user_id: "alice", count: 3 }] },
                     !(failing === "users" || failing === "both"));
    }
    if (key.indexOf("/v1/memories") === 0) {
      return respond({ memories: [{ id: "m1", memory: "a note", record_type: "note" }] },
                     !(failing === "memories" || failing === "both"));
    }
    return respond({}, true);
  }
};

const names = Object.keys(scope);
const loadBrowse = new Function(...names,
  page.slice(start, end) + "; return loadBrowse;")(...names.map((k) => scope[k]));

loadBrowse();

setTimeout(() => {
  const users = el("users").innerHTML;
  const memories = el("memories").innerHTML;
  const message = el("browseMsg").innerHTML;

  if (failing === "users" || failing === "both") {
    ok("a failed subject list is not left blank", users.trim().length > 0,
       JSON.stringify(users));
    ok("it says it did not load, not that the scope is empty",
       /Not loaded/.test(users) && !/No subjects/.test(users), users);
    ok("and the cause is named rather than left to be guessed",
       message.trim().length > 0, JSON.stringify(message));
  }
  if (failing === "users") {
    /* The contradiction this fixes: the other list loaded fine. */
    ok("FLOOR: the memory list still rendered, so the two disagreed",
       /a note/.test(memories), memories);
  }
  if (failing === "none") {
    ok("a working subject list is rendered", /alice/.test(users), users);
    ok("with no failure message", message.trim().length === 0, JSON.stringify(message));
    ok("and no panel claims it failed to load", !/Not loaded/.test(users), users);
  }
  if (failing === "empty") {
    ok("an empty scope says it is empty", /No subjects/.test(users), users);
    ok("and does not claim the request failed", !/Not loaded/.test(users), users);
    ok("with no error message", message.trim().length === 0, JSON.stringify(message));
  }
  if (failing === "memories") {
    ok("the sibling still reports its own failure",
       /Not loaded/.test(memories), memories);
    ok("and the subject list is unaffected", /alice/.test(users), users);
  }
  console.log(failures ? "\n" + failures + " failure(s)" : "\nall ok");
  process.exit(failures ? 1 : 0);
}, 30);
