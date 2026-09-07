// Runs the SHIPPED loadUsage against a DOM stub. Reading the source would pass on a function that
// builds the sentence and never shows it.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

const start = page.indexOf("  function loadUsage() {");
if (start < 0) throw new Error("loadUsage not found");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}

const shown = [];
const nodes = {
  usageTable: {
    style: {},
    querySelector: () => ({ innerHTML: "", appendChild() {} }),
  },
};
const scope = {
  $: (id) => nodes[id] || { style: {}, innerHTML: "", appendChild() {} },
  hideMsg: () => {},
  showMsg: (id, kind, text) => shown.push({ id, kind, text }),
  escapeHtml: (s) => String(s == null ? "" : s),
  fmtTime: () => "",
  gwBase: () => "http://gw",
  apiFetch: () => Promise.resolve(input.response),
  document: { createElement: () => ({ innerHTML: "" }) },
};

const names = Object.keys(scope);
const loadUsage = new Function(...names, page.slice(start, end) + "; return loadUsage;")(
  ...names.map((k) => scope[k]));

loadUsage();
setTimeout(() => {
  process.stdout.write(JSON.stringify({
    shown,
    tableShown: nodes.usageTable.style.display,
  }));
}, 0);
