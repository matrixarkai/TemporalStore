/* Run the one-box profile and cap panels against what the endpoint actually returns.
 *
 * Both panels used to render from the settings registry -- they asked whether a knob by a given
 * name was offered, and drew the ones that were. That is not the question either heading asks.
 * When a later change stopped offering the six settings they named, both panels said "This build
 * offers none of them." while the profile went on deciding every result and the caps went on
 * cutting every retrieve. Nothing failed: a panel rendering an empty-state is a panel working.
 *
 * So the payload here is not written out by hand. It is produced by calling the gateway's own
 * effective_retrieval() and handed in as JSON, which means these assertions are made against the
 * shape the endpoint really serves. A renderer and a hand-written fixture agree with each other
 * forever, including after the endpoint stops agreeing with both.
 *
 * Usage: node onebox_effective_harness.js <onebox_portal.html> <payload.json>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const live = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));

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

/* The page's own escaper, not a stand-in: the renderers call it on every value, so a harness with
   its own would be testing text the page never produces. */
const sandbox = {};
const source = [
  extract("function esc(s)"),
  extract("function unreadable(data, what)"),
  extract("function renderProfile(data)"),
  extract("function renderCaps(data)"),
  "sandbox.renderProfile = renderProfile;",
  "sandbox.renderCaps = renderCaps;"
].join("\n");
new Function("sandbox", source)(sandbox);

/* ---------- what the endpoint really returned ---------- */
const profile = sandbox.renderProfile(live);
const caps = sandbox.renderCaps(live);

ok("the profile panel is not an empty state",
   !/offers none of them|Enter an admin key/.test(profile), profile.slice(0, 200));
ok("the cap panel is not an empty state",
   !/offers none of them|Enter an admin key/.test(caps), caps.slice(0, 200));

/* Every cap the endpoint reported reaches the table, with the value that is in force. A panel
   that renders three rows of the wrong number looks exactly like one that renders three rows. */
for (const cap of live.caps) {
  ok("cap " + cap.name + " is named", caps.indexOf(cap.name) >= 0);
  ok("cap " + cap.name + " shows the value in force (" + cap.value + ")",
     caps.indexOf(">" + cap.value + "<") >= 0,
     "looked for >" + cap.value + "< in the rendered table");
  ok("cap " + cap.name + " shows the build default (" + cap.build_default + ")",
     caps.indexOf(">" + cap.build_default + "<") >= 0);
  ok("cap " + cap.name + " names its variable", caps.indexOf(cap.env) >= 0);
  ok("cap " + cap.name + " says which level supplied it",
     ["tenant", "environment", "default"].indexOf(cap.source) >= 0,
     "source was " + JSON.stringify(cap.source));
}

ok("the cap the measurement blamed is marked as such",
   /this is the one that cut it/.test(caps));

ok("the profile says in words which of the two things scoring does",
   /score is its vector similarity alone|blends vector similarity/.test(profile));
ok("the profile names its variable", profile.indexOf(live.profile.env) >= 0);

/* ---------- a value that could not be read is not a value ---------- */
/* The failure this whole change exists to prevent: the first version of the matching gauge caught
   a failed import in an `except` and published 0, so every dashboard described the opposite of
   what was running. A panel that renders the build defaults when the read failed does the same
   thing, and looks healthier doing it. */
const unread = { known: false, detail: "This deployment could not be asked what a retrieve applies: boom" };
const unreadProfile = sandbox.renderProfile(unread);
const unreadCaps = sandbox.renderCaps(unread);
ok("an unreadable profile says so", /could not be asked/.test(unreadProfile), unreadProfile);
ok("an unreadable profile renders no table", unreadProfile.indexOf("<table") < 0, unreadProfile);
ok("unreadable caps say so", /could not be asked/.test(unreadCaps), unreadCaps);
ok("unreadable caps render no table", unreadCaps.indexOf("<table") < 0, unreadCaps);

/* ---------- an override is visible as an override ---------- */
/* The registry view could not show this at all: it only ever held the declared number, so a
   deployment running a cap somebody had set looked identical to one running the default. */
/* One payload per level. The badge names WHICH level supplied the value, because a tenant
   override beats the environment variable: naming the variable beside a value the variable did
   not supply sends an operator to change something that will not take effect. */
function atLevel(source) {
  const copy = JSON.parse(JSON.stringify(live));
  copy.caps[0].value = copy.caps[0].build_default + 7;
  copy.caps[0].source = source;
  return sandbox.renderCaps(copy);
}

const fromEnv = atLevel("environment");
const fromTenant = atLevel("tenant");
const overriddenOut = fromEnv;

ok("a cap set by the variable says so", /set by the variable/.test(fromEnv));
ok("a cap set by tenant policy says so", /set by tenant policy/.test(fromTenant));
ok("the two levels are not described the same way",
   !/set by tenant policy/.test(fromEnv) && !/set by the variable/.test(fromTenant));

/* The whole point of separating them. A tenant override makes the variable in the last column
   inert, and a reader who does not know that reads the column as the way to change the number. */
ok("a tenant override says the variable is not consulted",
   /is not consulted/.test(fromTenant), fromTenant.slice(0, 400));
/* And says WHY, not just that. "MATRIXARK_X is not consulted" tells a reader the variable is
   being ignored without telling them what is doing the ignoring, so they have no way to find the
   thing they actually need to change. The first version of this assertion matched only the
   second half of the sentence, and a mutation deleting the cause survived it. */
ok("a tenant override names the override as the cause",
   /tenant override supplies this/.test(fromTenant), fromTenant.slice(0, 400));
ok("a tenant override names the variable it makes inert",
   fromTenant.indexOf(live.caps[0].env) >= 0);
ok("a value from the variable does not say that",
   !/is not consulted/.test(fromEnv));

ok("a cap at its build default is marked with no level at all",
   !/set by the variable|set by tenant policy/.test(caps), caps.slice(0, 300));

/* The two columns carry different numbers ONLY when somebody has set one, so this is the only
   payload in which "shows the value in force" and "shows the build default" are distinguishable
   assertions. Against the live payload, where every cap sits at its default, a renderer printing
   the default twice satisfies both -- which is how a mutation that did exactly that survived the
   first mutation run. */
const setValue = live.caps[0].build_default + 7;
ok("an overridden cap shows the value IN FORCE, not the default it replaced",
   overriddenOut.indexOf(">" + setValue + "<") >= 0,
   "expected the set value " + setValue + " in the rendered row");
ok("an overridden cap still shows the default it replaced",
   overriddenOut.indexOf(">" + live.caps[0].build_default + "<") >= 0,
   "expected the build default " + live.caps[0].build_default + " beside it");

/* ---------- the blended profile renders as blended ---------- */
const blended = JSON.parse(JSON.stringify(live));
blended.profile.embedding_first = false;
blended.profile.dense_weight = 0.72;
blended.profile.lexical_weight = 0.28;
const blendedOut = sandbox.renderProfile(blended);
ok("blended scoring says it blends", /blends vector similarity/.test(blendedOut));
ok("blended scoring shows both weights",
   blendedOut.indexOf("0.72") >= 0 && blendedOut.indexOf("0.28") >= 0, blendedOut);
ok("blended scoring is reported as not the default",
   /this was set/.test(blendedOut), blendedOut);

/* ---------- the positive control ---------- */
/* Every assertion above is about text appearing in a string. A renderer returning one long string
   containing every word would satisfy a surprising number of them, and an extractor that silently
   grabbed the wrong function would fail loudly -- but an extractor that grabbed a function which
   no longer runs on the page would not. */
ok("the page actually calls these renderers",
   /\$\("profile"\)\.innerHTML = renderProfile\(/.test(page)
   && /\$\("caps"\)\.innerHTML = renderCaps\(/.test(page),
   "the renderers exist on the page but nothing assigns their output");
ok("the page asks the endpoint that runs the serving path's resolution",
   page.indexOf('fetch("/v1/admin/retrieval"') >= 0);
/* Every table on this page scrolls inside its own panel.  is nowrap, so a table is as
   wide as its widest row header and without a wrapper the PAGE BODY carries that width: at 375px
   this document scrolled to 579px against a 375px viewport. Checked here because the geometry is
   invisible in the markup -- a table that overflows and one that does not read identically. */
for (const [what, html] of [["profile", profile], ["caps", caps]]) {
  ok("the " + what + " table can scroll inside its panel",
     html.slice(0, 40).indexOf("tablewrap") >= 0, html.slice(0, 120));
}

ok("the page no longer renders these panels from the settings registry",
   page.indexOf("PROFILE_KEYS") < 0 && page.indexOf("CAP_KEYS") < 0);

console.log(failures ? "\n" + failures + " failed" : "\nall passed");
process.exit(failures ? 1 : 0);
