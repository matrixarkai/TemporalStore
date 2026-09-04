/* Does the ingestion page notice when a stop is refused?
 *
 * fetch resolves for a 403 exactly as readily as for a 200 -- only a network failure rejects. A
 * handler that goes straight to .then(loadJobs) therefore reloads the table on a refusal and says
 * nothing, and the reader watching an import that is still running takes the stop for a slow one.
 *
 * That is invisible in source: the code reads as a completed action with a refresh at the end. So
 * the page's own cancelJob is run against canned responses and asked what it did.
 *
 * Usage: node stop_refusal_harness.js <ingestion_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(name) {
  const at = page.indexOf("function " + name + "(");
  if (at === -1) { return null; }
  let depth = 0;
  for (let i = page.indexOf("{", at); i < page.length; i += 1) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") {
      depth -= 1;
      if (depth === 0) { return page.slice(at, i + 1); }
    }
  }
  return null;
}

const cancel = slice("cancelJob");
const failure = slice("failure");
ok("the page defines cancelJob", !!cancel);
ok("the page defines its status map", !!failure);
if (!cancel || !failure) { console.log("FAILED " + failures); process.exit(1); }

async function flush() {
  for (let i = 0; i < 8; i += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

function run(status, body, reject) {
  const said = [];
  const reloads = [];
  const env = {
    $: (id) => ({ id }),
    say(_el, text, cls) { said.push({ text: String(text), cls }); },
    auth: () => ({}),
    loadJobs() { reloads.push(1); },
    encodeURIComponent: (s) => s,
    fetch: () => (reject
      ? Promise.reject(new Error("offline"))
      : Promise.resolve({
          ok: status >= 200 && status < 300,
          status,
          json: () => (body === undefined ? Promise.reject(new Error("no body"))
                                          : Promise.resolve(body)),
        })),
    window: {
      __matrixarkWhy(b, fallback) {
        b = b || {};
        let text = b.detail || b.error || fallback;
        if (b.required && b.required.length) { text += " — this needs " + b.required.join(" or "); }
        return text;
      },
      /* The shared map, if this page uses it; the page's own failure() closes over whichever. */
      __matrixarkFailure(code, overrides) {
        if (overrides && overrides[code]) { return overrides[code]; }
        if (code === 403) { return "This key lacks the scope for that call."; }
        if (code === 409) { return "That import is still running — stop it first."; }
        return "The gateway answered " + code + ".";
      },
    },
  };
  const src = failure + "\n" + cancel + "\n; return cancelJob;";
  const cancelJob = new Function(...Object.keys(env), src)(...Object.values(env));
  cancelJob("job-1");
  return flush().then(() => ({ said, reloads }));
}

(async () => {
  /* ---------- refused ---------- */
  const denied = await run(403, { error: "insufficient_scope", required: ["admin:api_key"] });
  ok("a refused stop is reported", denied.said.length > 0,
     "nothing was said; the table just reloaded");
  ok("and reported as a failure", denied.said.some((s) => s.cls === "err"),
     JSON.stringify(denied.said));
  ok("and says what the gateway said",
     denied.said.some((s) => /insufficient_scope|admin:api_key|scope/.test(s.text)),
     JSON.stringify(denied.said));
  ok("and does not reload the table over the explanation", denied.reloads.length === 0,
     "loadJobs ran and clears the message area");

  /* ---------- refused with no body at all ---------- */
  const bare = await run(409, undefined);
  ok("a refusal with no body still gets a sentence", bare.said.length > 0);
  ok("and not a bare number",
     bare.said.some((s) => !/^The gateway answered 409/.test(s.text)) ||
     bare.said.some((s) => /import/i.test(s.text)), JSON.stringify(bare.said));

  /* ---------- accepted ---------- */
  const okRun = await run(200, { status: "ok" });
  ok("a stop that worked refreshes the list", okRun.reloads.length === 1,
     String(okRun.reloads.length));
  ok("and does not cry failure", !okRun.said.some((s) => s.cls === "err"),
     JSON.stringify(okRun.said));

  /* ---------- offline ---------- */
  const off = await run(0, null, true);
  ok("a gateway that cannot be reached is reported", off.said.some((s) => s.cls === "err"),
     JSON.stringify(off.said));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
})();
