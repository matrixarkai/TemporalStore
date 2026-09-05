/* Run the setup page's model picker and see which SETTING a pick is written into.
 *
 * The picker used to write every pick into `extraction.model`. The Anthropic extraction path reads
 * MATRIXARK_ANTHROPIC_MODEL and ignores that field entirely, so on such a deployment picking a
 * model changed nothing: the form filled in, the page said it was set, and the model in use stayed
 * on its default. The route now names the field, and the page has to follow it.
 *
 * Both the resolver and the click handler are pulled OUT OF THE BUILT PAGE and executed -- this is
 * the copy a browser runs, and the mistake this replaced (a variable left dangling after the
 * rewrite) was a ReferenceError that no amount of reading the source would have shown.
 *
 * Usage: node model_key_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(from, to) {
  const start = page.indexOf(from);
  if (start < 0) { console.log("FAIL not on the page: " + from); process.exit(1); }
  const end = page.indexOf(to, start);
  if (end < 0) { console.log("FAIL no end for: " + from); process.exit(1); }
  return page.slice(start, end + to.length);
}

const resolverSource = slice("function modelKey(target) {", "\n  }");
const handlerSource = slice("var button = ev.target.closest",
                            'say($("saveMsg"), entry.title + " set to " + value + " — not saved yet. Save configuration " +\n      "below to apply it.", "info");');

/* One case: what the route said, what the person picked, and what the form holds. */
function run(routeBody, picked, fieldKeys) {
  const modelData = { extraction: routeBody, embedding: {} };
  const MODEL_TARGETS = [
    { target: "extraction", key: "extraction.model", title: "Extraction model", note: "" },
    { target: "embedding", key: "embedding.model", title: "Embedding model", note: "" }
  ];
  const edits = {};
  const pendingPick = {};
  const queried = [];
  const messages = [];
  const fields = {};
  fieldKeys.forEach(function (k) {
    fields[k] = { value: "", closest: () => null, scrollIntoView: () => {} };
  });

  const groups = {
    querySelector(selector) {
      const key = /\[data-key="([^"]+)"\]/.exec(selector);
      queried.push(key ? key[1] : selector);
      return key && Object.prototype.hasOwnProperty.call(fields, key[1]) ? fields[key[1]] : null;
    }
  };

  const scope = new Proxy({}, {
    has: () => true,
    get: (_t, name) => {
      switch (name) {
        case Symbol.unscopables: return undefined;
        case "modelData": return modelData;
        case "MODEL_TARGETS": return MODEL_TARGETS;
        case "edits": return edits;
        case "pendingPick": return pendingPick;
        case "pickedModel": return () => picked;
        case "$": return (id) => (id === "groups" ? groups : { id });
        case "say": return (_el, text) => messages.push(String(text));
        case "window": return { showTab: () => {} };
        case "ev": return { target: { closest: () => ({ dataset: { use: "extraction" } }) },
                            preventDefault: () => {} };
        default: return () => {};
      }
    }
  });

  /* The handler's own `var`s have to be declared INSIDE a function, or `with` resolves them
     against the proxy -- which answers every name with an inert function, so the body runs to
     completion having written nothing and every assertion below reads as a silent no-op.
     Locals of an inner function shadow the with-object; free names still fall through to it. */
  const NL = String.fromCharCode(10);
  const body = "return function handle() {" + NL + resolverSource + NL + handlerSource + NL + "};";
  let threw = null;
  try {
    new Function("scope", "with (scope) { " + body + " }")(scope)();
  } catch (err) {
    threw = err;
  }
  return { edits, queried, messages, threw };
}

/* 1. Anthropic: the route names its own field, and that is the one the page fills in. */
let r = run({ key: "extraction.anthropic_model", current: "claude-sonnet-5", catalogue: [] },
            "claude-opus-5", ["extraction.model", "extraction.anthropic_model"]);
ok("no exception on the path that sets a field", r.threw === null,
   r.threw && String(r.threw));
ok("the field looked up is the one the route named",
   r.queried.length === 1 && r.queried[0] === "extraction.anthropic_model",
   "queried " + JSON.stringify(r.queried));
ok("the pick is recorded against that key",
   r.edits["extraction.anthropic_model"] === "claude-opus-5" &&
   !Object.prototype.hasOwnProperty.call(r.edits, "extraction.model"),
   JSON.stringify(r.edits));
ok("the confirmation still names the control", r.messages.some((m) => /Extraction model set to/.test(m)),
   JSON.stringify(r.messages));

/* 2. OpenAI-compatible: unchanged from what shipped. */
r = run({ key: "extraction.model", current: "gpt-4o-mini", catalogue: [] },
        "gpt-4o", ["extraction.model", "extraction.anthropic_model"]);
ok("an openai-compatible pick still lands in extraction.model",
   r.edits["extraction.model"] === "gpt-4o" &&
   !Object.prototype.hasOwnProperty.call(r.edits, "extraction.anthropic_model"),
   JSON.stringify(r.edits));

/* 3. A gateway that does not say: the page keeps working on its own static key rather than
      querying `[data-key="undefined"]` and reporting the field as missing. */
r = run({ current: "gpt-4o-mini", catalogue: [] }, "gpt-4o",
        ["extraction.model", "extraction.anthropic_model"]);
ok("a route that names no field falls back to the static one",
   r.edits["extraction.model"] === "gpt-4o", JSON.stringify(r.edits));

/* 4. The floor. If the handler stopped writing anything, every assertion above about WHICH key is
      written would still pass. */
r = run({ key: "extraction.anthropic_model", current: "", catalogue: [] }, "",
        ["extraction.anthropic_model"]);
ok("nothing picked writes nothing and says so",
   Object.keys(r.edits).length === 0 && r.messages.some((m) => /Type a model name first/.test(m)),
   JSON.stringify(r.edits) + " " + JSON.stringify(r.messages));

/* 5. The field named by the route is not on the form: say so rather than silently doing nothing. */
r = run({ key: "extraction.anthropic_model", current: "", catalogue: [] }, "claude-opus-5",
        ["extraction.model"]);
ok("a field the form does not carry is reported, not skipped",
   Object.keys(r.edits).length === 0 &&
   r.messages.some((m) => /not loaded/.test(m)),
   JSON.stringify(r.edits) + " " + JSON.stringify(r.messages));

console.log(failures ? "\n" + failures + " failure(s)" : "\nall ok");
process.exit(failures ? 1 : 0);
