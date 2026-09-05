/* Run the setup page's modelOptions and see which encoders it OFFERS.
 *
 * The measured catalogue is five self-hosted models, and the numbers beside them come from encoding
 * a real corpus on the machine that runs them. A deployment on Voyage cannot serve any of them, and
 * was offered all five and none of the ones it can.
 *
 * The fix is a split, not a filter on one list: `applicable` narrows what is OFFERED without
 * narrowing what is SHOWN, so the comparison table still answers "what would self-hosting score".
 * That distinction only exists in the dropdown builder, so it is pulled out of the BUILT page and
 * executed here.
 *
 * Usage: node encoder_options_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const start = page.indexOf("function modelOptions(target) {");
if (start < 0) { console.log("FAIL modelOptions is not on the page"); process.exit(1); }
const end = page.indexOf("\n  }", start);
if (end < 0) { console.log("FAIL no end for modelOptions"); process.exit(1); }
const source = page.slice(start, end + 4);

function options(routeBody) {
  const modelData = { embedding: routeBody };
  const scope = new Proxy({}, {
    has: () => true,
    get: (_t, name) => {
      if (name === Symbol.unscopables) { return undefined; }
      if (name === "modelData") { return modelData; }
      if (name in globalThis) { return globalThis[name]; }
      return () => "";
    }
  });
  const NL = String.fromCharCode(10);
  const body = "return function build() {" + NL + source + NL +
    "return modelOptions('embedding');" + NL + "};";
  return new Function("scope", "with (scope) { " + body + " }")(scope)();
}

const MEASURED = [
  { model: "intfloat/multilingual-e5-large", dim: 512, hit_at_1: 76.2, texts_per_s: 1.7,
    note: "best measured", same_width_as: [] },
  { model: "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", dim: 384, hit_at_1: 59.4,
    texts_per_s: 31.7, note: "fastest", same_width_as: [] }
];

/* 1. Voyage: the measured rows are not offered, and what Voyage serves is. */
let names = options({
  current: "", catalogue: MEASURED.map((m) => Object.assign({}, m, { applicable: false })),
  provider_models: [{ model: "voyage-3", note: "Voyage's general encoder." }]
}).map((o) => o.model);
ok("a self-hosted encoder is not offered to a hosted provider",
   !names.some((n) => n.indexOf("/") >= 0), JSON.stringify(names));
ok("what the provider serves is offered", names.indexOf("voyage-3") >= 0, JSON.stringify(names));

/* 2. The note survives, because a name with nothing beside it is a guess. */
let entries = options({
  current: "", catalogue: [],
  provider_models: [{ model: "voyage-3", note: "Voyage's general encoder." }]
});
ok("a hosted encoder carries its note",
   entries.length === 1 && /Voyage's general encoder/.test(entries[0].note),
   JSON.stringify(entries));

/* 3. An OpenAI-compatible endpoint keeps both worlds. */
names = options({
  current: "", catalogue: MEASURED.map((m) => Object.assign({}, m, { applicable: true })),
  provider_models: [{ model: "text-embedding-3-small", note: "cheaper" }]
}).map((o) => o.model);
ok("a self-hosted encoder is still offered where it can run",
   names.filter((n) => n.indexOf("/") >= 0).length === 2, JSON.stringify(names));
ok("and the hosted one alongside it", names.indexOf("text-embedding-3-small") >= 0,
   JSON.stringify(names));

/* 4. The floor. A build that offered nothing would pass every "is not offered" check above. */
names = options({ current: "", catalogue: MEASURED, provider_models: [] }).map((o) => o.model);
ok("a route that says nothing about applicability offers everything", names.length === 2,
   JSON.stringify(names));

/* 5. The configured model is always offered, even when its provider would not list it -- a
      deployment already pointed at something must be able to see what that is. */
entries = options({
  current: "intfloat/multilingual-e5-large",
  catalogue: MEASURED.map((m) => Object.assign({}, m, { applicable: false })),
  provider_models: [{ model: "voyage-3", note: "" }]
});
ok("the model in use is still listed",
   entries.some((o) => o.model === "intfloat/multilingual-e5-large"),
   JSON.stringify(entries.map((o) => o.model)));

/* 6. What the endpoint itself reported is not filtered: it comes from asking the live provider,
      so it is by definition something that provider serves. */
names = options({
  current: "", catalogue: [], provider_models: [],
  discovered: { models: ["voyage-3-lite"] }
}).map((o) => o.model);
ok("a model the endpoint reported is offered", names.indexOf("voyage-3-lite") >= 0,
   JSON.stringify(names));

console.log(failures ? "\n" + failures + " failure(s)" : "\nall ok");
process.exit(failures ? 1 : 0);
