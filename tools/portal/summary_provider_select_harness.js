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

/* The comparison: a list WITHOUT the blank cannot express "same as extraction". */
const withoutBlank = controlHtml(Object.assign({}, field,
  { choices: ["deterministic", "openai_compatible"] }));
ok("FLOOR: without the blank, nothing is selected and the browser shows the first",
   withoutBlank.indexOf("selected") < 0, withoutBlank);

console.log(failures ? "\n" + failures + " failure(s)" : "\nall good");
process.exit(failures ? 1 : 0);
