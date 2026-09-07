// Runs the SHIPPED exchange panel and call log from the generated page.
// The renderers are what a person debugging reads, so what matters is the HTML they produce.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

function fn(name) {
  const start = page.indexOf("function " + name + "(");
  if (start < 0) throw new Error("not found: " + name);
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + name);
}

const nodes = {};
const el = (id) => (nodes[id] = nodes[id] || { innerHTML: "" });

const scope = {
  $: el,
  esc: (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
  JSON: JSON,
  Object: Object,
  Date: Object.assign(function () {}, { now: () => 1760000000000 }),
  String: String,
  window: { __matrixarkWhen: (ms) => "AT:" + ms },
};

const names = Object.keys(scope);
const api = new Function(...names, [
  fn("headerRows"), fn("renderWire"), "var calls = [];", fn("recordCall"),
  "return { renderWire: renderWire, recordCall: recordCall, calls: function () { return calls; } };",
].join("\n"))(...names.map((k) => scope[k]));

(input.calls || []).forEach((c) => {
  api.renderWire(c.sent, c.answer);
  api.recordCall(c.op, c.sent, c.answer);
});

process.stdout.write(JSON.stringify({
  wire: (nodes.opWire || { innerHTML: "" }).innerHTML,
  log: (nodes.opLog || { innerHTML: "" }).innerHTML,
  count: api.calls().length,
}));
