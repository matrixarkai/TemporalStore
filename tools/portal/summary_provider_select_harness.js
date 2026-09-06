/* Does the shipped controlHtml render a select a customer can set BACK to blank? */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("function controlHtml(f)");
if (start < 0) { console.log("controlHtml is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const fieldId = (key) => "f_" + key.replace(/[^a-z0-9]/gi, "_");
const controlHtml = new Function("esc", "fieldId",
  page.slice(start, end) + "; return controlHtml;")(esc, fieldId);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const field = {
  key: "summary.provider",
  kind: "str",
  value: "",
  choices: ["", "deterministic", "openai_compatible"],
};
const blank = controlHtml(field);
ok("it renders a select", /^<select/.test(blank), blank.slice(0, 60));
ok("the blank option is there", blank.indexOf('<option value=""') >= 0, blank);
ok("and is the one selected when nothing is chosen",
   /<option value=""[^>]* selected>/.test(blank), blank);

const chosen = controlHtml(Object.assign({}, field, { value: "openai_compatible" }));
ok("a chosen provider is the one selected",
   /<option value="openai_compatible" selected>/.test(chosen), chosen);
ok("and blank is still offered, so it is not a one-way door",
   chosen.indexOf('<option value=""') >= 0, chosen);

/* The blank is the DEFAULT on this setting, so it is the row most people see selected. Rendered
   bare it was an empty line in the dropdown. */
ok("the blank reads as something rather than nothing",
   blank.indexOf(">(not set)</option>") >= 0, blank);

/* A list WITHOUT the blank used to mean nothing was selected and the browser showed the first --
   the screen then reported a provider the deployment was not running, and saving made it true.
   The running value is carried now, whatever the list holds. */
const withoutBlank = controlHtml(Object.assign({}, field,
  { choices: ["deterministic", "openai_compatible"] }));
ok("a running value the list does not hold is still selected",
   /<option value="" selected>/.test(withoutBlank), withoutBlank);
ok("and it is not silently presented as an offered choice",
   withoutBlank.indexOf("(not set)") >= 0, withoutBlank);

/* The case this is really for. summary_provider() maps oss, open_source, local_llm and oss_llm
   onto openai_compatible, and none of them are offered here, so a deployment configured with one
   is a deployment whose provider is not in this list. */
const alias = controlHtml(Object.assign({}, field, { value: "oss" }));
ok("an accepted alias is shown as the running value",
   /<option value="oss" selected>/.test(alias), alias);
ok("and is marked as one the list does not offer",
   alias.indexOf("oss (in use, not offered)") >= 0, alias);
ok("without dropping any offered choice",
   ["", "deterministic", "openai_compatible"].every(function (c) {
     return alias.indexOf('<option value="' + c + '"') >= 0;
   }), alias);
ok("and exactly one option is selected",
   (alias.match(/ selected>/g) || []).length === 1, alias);

/* The floor for all of the above: an offered value must NOT be marked. */
ok("FLOOR: a value the list does offer carries no marking",
   chosen.indexOf("not offered") < 0, chosen);

console.log(failures ? "\n" + failures + " failure(s)" : "\nall good");
process.exit(failures ? 1 : 0);
