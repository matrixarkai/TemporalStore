// Runs the SHIPPED __matrixarkWhen from a generated page. Reading the source would not catch a
// formatter that returns "Invalid Date", which is what the old sites did for a missing timestamp.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("window.__matrixarkWhen = function (ms)");
if (start < 0) throw new Error("__matrixarkWhen not found on this page");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const body = page.slice(start, end).replace("window.__matrixarkWhen = ", "var when = ");
const when = new Function("Date", "isNaN", "Number", body + "; return when;")(Date, isNaN, Number);

const inputs = JSON.parse(process.argv[3]);
process.stdout.write(JSON.stringify(inputs.map((v) => when(v))));
