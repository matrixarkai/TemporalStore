// Runs the SHIPPED catalog `load` against stubs. Reading the source would pass on a page that
// computes the sentence and never shows it, which is the mistake this whole family of checks
// exists to catch.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

const start = page.indexOf("  function load(keepMessage) {");
if (start < 0) throw new Error("load not found");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}

const said = [];
const values = {
  filter: input.filter || "",
  rtype: "",
  limit: String(input.typed),
};
const el = (id) => ({
  value: values[id] !== undefined ? values[id] : "",
  checked: false,
  textContent: "",
  innerHTML: "",
  getAttribute: () => "false",
});
const nodes = {};
const $ = (id) => (nodes[id] = nodes[id] || el(id));

const bodies = input.bodies;
const scope = {
  $,
  query: () => ["limit=" + values.limit],
  auth: () => ({}),
  conn: () => {},
  say: (node, text, cls) => said.push({ text, cls }),
  esc: (s) => String(s == null ? "" : s),
  matches: () => true,
  renderSummary: () => {},
  skillsHtml: () => "",
  resourcesHtml: () => "",
  fetch: (url) => Promise.resolve({
    ok: true,
    status: 200,
    json: () => Promise.resolve(url.indexOf("/v1/skills") === 0 ? bodies[0] : bodies[1]),
  }),
  Promise,
};

const names = Object.keys(scope);
const load = new Function(...names, page.slice(start, end) + "; return load;")(
  ...names.map((k) => scope[k]));

load(input.keepMessage === true);
setTimeout(() => {
  process.stdout.write(JSON.stringify({ said }));
}, 5);
