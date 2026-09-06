/* Run the shipped renderSummary and read what it says about a configuration that is not in force.
 *
 * The distinction cannot be read off the source. A deployment with a corrupt config file and one
 * that has never been configured produce the SAME settings document -- `load()` returns an empty
 * one for both, on purpose, because the deployment has to start either way. So the only thing that
 * separates them on the screen is this banner, and the only way to know it is drawn is to draw it.
 *
 * Usage: node config_not_applied_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("function renderSummary(d) {");
if (start < 0) { console.log("renderSummary is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}

const els = {};
function el(id) {
  if (!els[id]) { els[id] = { innerHTML: "", textContent: "" }; }
  return els[id];
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const scope = { $: el, esc, fields: {} };
const names = Object.keys(scope);
const renderSummary = new Function(...names,
  page.slice(start, end) + "; return renderSummary;")(...names.map((k) => scope[k]));

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function warningsFor(status) {
  els.warnings = { innerHTML: "", textContent: "" };
  renderSummary({ settings: { config_file_status: status }, warnings: [], extraction: {},
                  embedding: {} });
  return els.warnings.innerHTML;
}

/* Absent is the ordinary state of a deployment nobody has configured. A banner here would fire on
   every fresh install, which is the shape of warning people stop reading. */
const absent = warningsFor({ state: "absent", path: "/root/.matrixark/runtime_config.json",
                             applied: true });
ok("FLOOR: no file at all is not a problem", absent.indexOf("not being used") < 0, absent);

const good = warningsFor({ state: "ok", path: "/root/.matrixark/runtime_config.json",
                           applied: true, settings: 12 });
ok("FLOOR: a file that IS applied says nothing", good.indexOf("not being used") < 0, good);

const broken = warningsFor({ state: "unparsable", path: "/etc/matrixark/runtime_config.json",
                             applied: false, detail: "Expecting ',' delimiter: line 1 column 42" });
ok("a file that is not in force says so", broken.indexOf("not being used") >= 0, broken);
ok("it names the file, so it can be found",
   broken.indexOf("/etc/matrixark/runtime_config.json") >= 0, broken);
ok("and says what was wrong with it",
   broken.indexOf("Expecting") >= 0, broken);
ok("it says the deployment is on defaults, not that it is unconfigured",
   /built-in\s+defaults/.test(broken), broken);
ok("and is styled as an error rather than a note",
   /class="note err"/.test(broken), broken);

const unreadable = warningsFor({ state: "unreadable", path: "/x/cfg.json", applied: false,
                                 detail: "Is a directory" });
ok("an unreadable file says so too", unreadable.indexOf("not being used") >= 0, unreadable);

/* Ordinary warnings must survive: the banner is added to them, not in place of them. */
els.warnings = { innerHTML: "", textContent: "" };
renderSummary({ settings: { config_file_status: { state: "unparsable", applied: false,
                                                  path: "/x", detail: "bad" } },
                warnings: ["an ordinary warning"], extraction: {}, embedding: {} });
const both = els.warnings.innerHTML;
ok("the existing warnings are still there", both.indexOf("an ordinary warning") >= 0, both);
ok("and the banner comes first, because it invalidates them",
   both.indexOf("not being used") < both.indexOf("an ordinary warning"), both);

/* A build that does not send the field at all must not be reported as broken. */
els.warnings = { innerHTML: "", textContent: "" };
renderSummary({ settings: {}, warnings: [], extraction: {}, embedding: {} });
ok("FLOOR: an older gateway that sends no status is not accused",
   els.warnings.innerHTML.indexOf("not being used") < 0, els.warnings.innerHTML);

console.log(failures ? "\n" + failures + " failure(s)" : "\nall ok");
process.exit(failures ? 1 : 0);
