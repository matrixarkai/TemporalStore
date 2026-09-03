/* Run the key portal's overrides renderer against real payload shapes.
 *
 * The page loading without throwing proves nothing about this panel: renderOverrides only runs on
 * a button press, so a call to a helper that does not exist -- which is exactly the bug this
 * harness was written after -- loads clean and fails when someone clicks.
 *
 * The renderer touches one table body, so it gets a stub that remembers what was written rather
 * than a full DOM.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join("\n");

function extract(name) {
  const start = scripts.indexOf("function " + name + "(");
  if (start < 0) { return null; }
  let depth = 0;
  for (let i = scripts.indexOf("{", start); i < scripts.length; i += 1) {
    if (scripts[i] === "{") { depth += 1; }
    else if (scripts[i] === "}") {
      depth -= 1;
      if (depth === 0) { return scripts.slice(start, i + 1); }
    }
  }
  return null;
}

const renderSrc = extract("renderOverrides");
const escapeSrc = extract("escapeHtml");
if (!renderSrc) { console.log("FAIL renderOverrides is not in the page"); process.exit(1); }
if (!escapeSrc) { console.log("FAIL escapeHtml is not in the page"); process.exit(1); }

let failures = 0;
const body = { innerHTML: "", appendChild(node) { this.innerHTML += node.innerHTML; } };
const table = { querySelector: () => body };
const doc = { createElement: () => ({ innerHTML: "" }) };
const factory = new Function(
  "$", "document",
  escapeSrc + "\n" + renderSrc + "\n; return renderOverrides;");
const renderOverrides = factory(() => table, doc);

function run(label, payload, checks) {
  body.innerHTML = "";
  try {
    renderOverrides(payload);
  } catch (err) {
    console.log("FAIL " + label + " threw: " + err.message);
    failures += 1;
    return;
  }
  for (const [what, ok] of checks(body.innerHTML)) {
    if (ok) { console.log("ok   " + label + " -- " + what); }
    else {
      console.log("FAIL " + label + " -- " + what + "\n     got: " + body.innerHTML.slice(0, 200));
      failures += 1;
    }
  }
}

run("a tenant override", { tenants: [{ tenant: "acme", settings: { top_k_per_layer: 24 } }], users: [] },
    (html) => [
      ["names the tenant", html.includes("acme")],
      ["names the level", html.includes("tenant")],
      ["shows the setting and value", html.includes("top_k_per_layer=24")],
    ]);

run("a user override", { tenants: [], users: [{ key: "acme/alice", settings: { recall_reinforcement: false } }] },
    (html) => [
      ["names the identity", html.includes("acme/alice")],
      ["marks it as a user row", html.includes("user")],
      ["shows the value", html.includes("recall_reinforcement=false")],
    ]);

run("nothing set", { tenants: [], users: [] },
    (html) => [
      ["says there are no overrides rather than rendering an empty table",
       html.toLowerCase().includes("no overrides")],
    ]);

run("missing keys entirely", {},
    (html) => [["survives a payload with no arrays", html.length > 0]]);

// A settings value must not be able to inject markup into the page.
run("a hostile value", { tenants: [{ tenant: "acme", settings: { note: "<img src=x onerror=alert(1)>" } }], users: [] },
    (html) => [
      ["escapes the value", !html.includes("<img")],
      ["still shows the row", html.includes("acme")],
    ]);


/* Wiring. Everything above proves the RENDERER works, and this panel shipped three separate ways of
 * being broken while the renderer was fine: a call to a helper the page does not define, missing
 * markup, and a button nothing listened to. Each loaded clean and each failed on click.
 *
 * These are SOURCE checks and are labelled as such -- driving the real click path needs the whole
 * page environment, which is what page_watchers_harness covers. What is verified here is that the
 * ids the renderer reaches for exist in the markup, and that the button calls the loader. */
const needs = ["overridesTable", "overridesMsg", "overridesBtn"];
for (const id of needs) {
  if (!page.includes('id="' + id + '"')) {
    console.log("FAIL the page has no element with id " + id + " (source check)");
    failures += 1;
  } else {
    console.log("ok   markup has #" + id + " (source check)");
  }
}
/* Both idioms are in use on this page: `$("id").onclick = fn` in the main script and
 * `el("id").addEventListener("click", fn)` further down. Accepting only one made this check
 * fail against correctly wired code, which is a guard rejecting correct usage. */
const wired = /overridesBtn"?\)?\s*\.onclick\s*=\s*loadOverrides/.test(page)
  || /overridesBtn[\s\S]{0,160}addEventListener\(\s*["']click["']\s*,\s*loadOverrides/.test(page);
if (!wired) {
  console.log("FAIL nothing calls loadOverrides on click (source check)");
  failures += 1;
} else {
  console.log("ok   the refresh button calls loadOverrides (source check)");
}

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
