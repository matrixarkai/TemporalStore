/* Run the SHIPPED "why did this come back empty" sentence from the generated Explore page.
 *
 * Three causes look identical from the page's side, and naming the wrong one sends a reader to
 * check a setting that is already right -- which is worse than saying nothing, because they find
 * it right and conclude the page is broken.
 *
 * The third cause is shedding: under load the gateway answers with an empty pack in about 100 ms
 * and says so in `warnings`. Before this, the page rendered that warning as the backend's own
 * string and told the reader, in a full sentence above it, to go and look at their embedding
 * provider.
 *
 * Usage: node explore_empty_harness.js <explore_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function extract(marker) {
  const start = page.indexOf(marker);
  if (start < 0) { throw new Error("not on this page: " + marker); }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  throw new Error("unbalanced: " + marker);
}

const sandbox = {};
new Function("sandbox", [
  extract("function wasShed(d)"),
  extract("function emptyPackMessage(d)"),
  "sandbox.wasShed = wasShed;",
  "sandbox.emptyPackMessage = emptyPackMessage;",
].join("\n"))(sandbox);

/* The shed shape measured on this stack. */
const SHED = {
  context_pack_id: "p", groups: [], tokens: {},
  warnings: ["retrieval_deadline_exceeded:service_backpressure", "service_backpressure"],
  partial: true, insufficient_context: true,
};

ok("a backpressure warning is recognised", sandbox.wasShed(SHED) === true);
ok("an answer with no warnings is not called shed", sandbox.wasShed({ groups: [] }) === false);
ok("an unrelated warning is not called shed",
   sandbox.wasShed({ groups: [], warnings: ["summary_truncated"] }) === false);
ok("a missing answer does not throw", sandbox.wasShed(null) === false);

const shed = sandbox.emptyPackMessage(SHED);
ok("a shed search says the deployment did not run it",
   /did not run the search/.test(shed), shed);
ok("and clears the reader of blame for their setup",
   /Nothing is wrong with the query, the store or the embedding model/.test(shed), shed);
/* The whole point: it must NOT send them to the provider they have configured correctly. */
ok("and does not blame the embedding provider",
   !/deterministic/.test(shed), shed);

const encoder = sandbox.emptyPackMessage({ groups: [], embedding_conflicts: { encoder_change: 4 } });
ok("an encoder change still says so", /different embedding model/.test(encoder), encoder);
ok("and is not called shedding", !/did not run the search/.test(encoder), encoder);

const plain = sandbox.emptyPackMessage({ groups: [] });
ok("a plain empty result keeps the deterministic-provider hint",
   /deterministic/.test(plain), plain);
ok("and is not called shedding", !/did not run the search/.test(plain), plain);

/* Shedding wins when both are true: nothing was searched, so what the encoder did is beside the
   point and mentioning it is the misdirection this exists to remove. */
const both = sandbox.emptyPackMessage(
  Object.assign({}, SHED, { embedding_conflicts: { encoder_change: 4 } }));
ok("shedding is named ahead of an encoder change", /did not run the search/.test(both), both);

/* The positive control: the sentence could be perfect and never reach the page. Anchored on the
   assignment, because the function's own declaration contains its name. */
ok("the page actually renders it",
   /\+ emptyPackMessage\(d\) \+/.test(page), "nothing assigns the sentence");
/* Counted, not matched, and counted over CODE rather than prose.
   `/backpressure/i.test(String(w))` alone is also in `wasShed`, so it passed whether or not the
   skip existed; the skip's own comment survives a mutation that deletes only the code beneath it,
   so anchoring there passed as well. Two occurrences is the armed state -- one in `wasShed`, one
   in the skip -- and one means the raw warning is being dumped beside the sentence again. */
const skipSites = (page.match(/backpressure\/i\.test\(String\(w\)\)/g) || []).length;
ok("the raw backpressure string is not also dumped as a note",
   skipSites >= 2, "found " + skipSites + " site(s); the skip in the warnings loop is gone");

console.log(failures ? "\n" + failures + " failed" : "\nall passed");
process.exit(failures ? 1 : 0);
