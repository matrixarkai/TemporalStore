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
  /* Both cap and policy rows go through one level map now, so it has to come along or every row
     throws on an undefined helper. Extracted rather than restated: a harness with its own copy of
     the labels would agree with itself forever. */
  extract("var LEVEL = {"),
  extract("function levelBadge(source)"),
  extract("function unreadable(data, what)"),
  extract("function renderProfile(data)"),
  extract("function renderCaps(data)"),
  extract("function subjectLine(data)"),
  extract("function retrieveCounts(text)"),
  extract("function workerCount(text)"),
  extract("function renderAnswering(text)"),
  extract("function renderPolicy(knobs)"),
  "sandbox.renderProfile = renderProfile;",
  "sandbox.renderCaps = renderCaps;",
  "sandbox.renderPolicy = renderPolicy;",
  "sandbox.subjectLine = subjectLine;",
  "sandbox.renderAnswering = renderAnswering;",
  "sandbox.retrieveCounts = retrieveCounts;"
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

const fromEnv = atLevel("env");
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

/* ---------- the return-all panel names its level too ---------- */
/* /v1/admin/policy has always sent `source` for every knob and this panel dropped it, so a row
   read as "this is the value" without saying who set it -- the same thing the cap panel was doing,
   except here the answer was already on the wire. */
const POLICY_KNOBS = {
  return_all_candidates: { value: true, source: "tenant", description: "Return every candidate." },
  return_all_candidate_threshold: { value: 80, source: "env", description: "At or below this." }
};
const policyOut = sandbox.renderPolicy(POLICY_KNOBS);
ok("a return-all knob set by tenant policy says so", /set by tenant policy/.test(policyOut));
ok("a return-all knob set by the variable says so", /set by the variable/.test(policyOut));
ok("the return-all values still render",
   policyOut.indexOf("80") >= 0, policyOut.slice(0, 200));

const POLICY_DEFAULTS = {
  return_all_candidates: { value: false, source: "default", description: "Return every candidate." }
};
ok("a return-all knob nobody set carries no level badge",
   !/set by tenant policy|set by the variable|set for this user/
      .test(sandbox.renderPolicy(POLICY_DEFAULTS)));

/* One vocabulary across both panels: the same source word must produce the same words on screen,
   or a reader has to learn two dialects of one idea on one page. */
const capTenant = atLevel("tenant");
ok("one source word reads the same in both panels",
   /set by tenant policy/.test(capTenant) && /set by tenant policy/.test(policyOut));

/* ---------- whose numbers are these ---------- */
/* The caps resolve per tenant, and a key bound to no tenant reads the deployment defaults --
   which is exactly what a tenant with no override gets, so the two tables are identical on screen.
   An operator holding the second reads the first unless told. */
const forTenant = sandbox.subjectLine(
  Object.assign({}, live, { known: true, answered_for: "tenant", tenant: "acme-corporation" }));
ok("a tenant reading its own caps is told whose they are",
   forTenant.indexOf("acme-corporation") >= 0, forTenant);
ok("and warned that another tenant may differ",
   /may be running different numbers/.test(forTenant), forTenant);

const asDefaults = sandbox.subjectLine(
  Object.assign({}, live, { known: true, answered_for: "deployment", tenant: null,
                            tenants_overriding_caps: 2 }));
ok("a keyless-tenant reading is called the deployment defaults",
   /deployment defaults/.test(asDefaults), asDefaults);
ok("and told how many tenants do not get them",
   /2 tenants have overridden/.test(asDefaults), asDefaults);

const nobodyDiffers = sandbox.subjectLine(
  Object.assign({}, live, { known: true, answered_for: "deployment", tenant: null,
                            tenants_overriding_caps: 0 }));
ok("zero overrides is stated as such rather than left silent",
   /No tenant has overridden/.test(nobodyDiffers), nobodyDiffers);

const cannotTell = sandbox.subjectLine(
  Object.assign({}, live, { known: true, answered_for: "deployment", tenant: null,
                            tenants_overriding_caps: -1 }));
/* The failure value has to READ as a failure. Rendering -1 as "no tenant has overridden" would be
   the disclosure line telling the reassuring lie the whole line exists to prevent. */
ok("a count that could not be taken says so, not zero",
   /could not be checked/.test(cannotTell), cannotTell);
ok("and does not claim nobody differs",
   !/No tenant has overridden/.test(cannotTell), cannotTell);

ok("an unreadable payload gets no subject line at all",
   sandbox.subjectLine({ known: false, detail: "boom" }) === "");

/* ---------- whether it is answering ---------- */
/* The panel that would be the whole point of opening this page on a deployment that has stopped
   retrieving. Under load the gateway sheds: HTTP 200, empty pack, ~100ms -- FASTER than doing the
   work, so no latency chart shows it. */
function scrape(served, empty, shed) {
  return [
    "# HELP matrixark_gateway_requests_total Requests.",
    "matrixark_gateway_requests_total{route=\"/v1/retrieve\",method=\"POST\",status=\"200\"} 99",
    "# TYPE matrixark_gateway_retrieve_outcomes_total counter",
    'matrixark_gateway_retrieve_outcomes_total{outcome="served"} ' + served,
    'matrixark_gateway_retrieve_outcomes_total{outcome="empty"} ' + empty,
    'matrixark_gateway_retrieve_outcomes_total{outcome="shed"} ' + shed
  ].join("\n");
}

const counts = sandbox.retrieveCounts(scrape(7, 2, 1));
ok("the three counters are read out of the scrape",
   counts.served === 7 && counts.empty === 2 && counts.shed === 1, JSON.stringify(counts));

const healthy = sandbox.renderAnswering(scrape(10, 0, 0));
ok("a worker answering everything shows the counts", healthy.indexOf(">10<") >= 0, healthy);
ok("and raises nothing", !/msg err/.test(healthy), healthy);

const shedding = sandbox.renderAnswering(scrape(0, 0, 40));
ok("a worker that answered nothing says so loudly",
   /Nothing has been answered/.test(shedding), shedding);
ok("and the shed count is shown", shedding.indexOf(">40<") >= 0);

const mostly = sandbox.renderAnswering(scrape(3, 5, 4));
ok("more empty than served is called out",
   /came back without a pack than with one/.test(mostly), mostly);

/* The one that matters most. Zero of everything is NO EVIDENCE, not health -- reporting "none
   shed" here would be a clean bill issued about a worker that has answered nothing at all, which
   is the exact shape of surface this whole effort has been removing. */
const untouched = sandbox.renderAnswering(scrape(0, 0, 0));
ok("a worker with no retrieves says there is nothing to report",
   /nothing to report either way/.test(untouched), untouched);
ok("and does NOT read as a clean bill of health",
   !/msg err/.test(untouched) && untouched.indexOf("<table") < 0, untouched);

/* A build that does not emit these must say so rather than have two of three read as zero. */
const partial = sandbox.renderAnswering(
  'matrixark_gateway_retrieve_outcomes_total{outcome="served"} 5');
ok("a partial read is refused rather than completed with zeros",
   /does not report/.test(partial), partial);
ok("an empty scrape is refused too",
   /does not report/.test(sandbox.renderAnswering("")));

ok("the panel says it speaks for one worker",
   /This worker only/.test(healthy), healthy);

/* Quantified where the scrape allows it. "This worker only" does not tell a reader whether they
   are looking at most of the traffic or an eighth of it. */
const fourWorkers = sandbox.renderAnswering(
  scrape(10, 0, 0) + "\nmatrixark_gateway_workers 4");
ok("with four workers it says one of four", /one of 4/.test(fourWorkers), fourWorkers);

/* And still does no arithmetic. Four workers do not answer alike, so multiplying one worker's
   counts by the worker count would invent a deployment-wide total out of one sample -- the exact
   shape of surface this work has been removing. */
ok("and still offers no deployment-wide total",
   /no total is offered/.test(fourWorkers) && fourWorkers.indexOf(">40<") < 0, fourWorkers);

ok("a single-worker deployment is not told it is one of one",
   !/one of 1/.test(sandbox.renderAnswering(
     scrape(10, 0, 0) + "\nmatrixark_gateway_workers 1")));

/* A scrape without the worker series still reads correctly -- it just does not quantify. This
   is NOT testing that null is kept distinct from 1: the sentence only quantifies above one, so
   those render identically and no assertion here can tell them apart. */
ok("a scrape without the worker series still reads correctly",
   /This worker only/.test(healthy) && !/one of/.test(healthy), healthy);

/* It must not be gated on the admin key: /v1/metrics needs none, and this is the one question
   here worth answering before somebody has found a key. */
ok("the scrape is fetched outside the key-gated load",
   /function loadAnswering\(\)/.test(page)
   && /loadAnswering\(\);/.test(page)
   && page.indexOf('fetch("/v1/metrics")') >= 0,
   "the answering panel is behind the admin key");

/* ---------- the positive control ---------- */
/* Every assertion above is about text appearing in a string. A renderer returning one long string
   containing every word would satisfy a surprising number of them, and an extractor that silently
   grabbed the wrong function would fail loudly -- but an extractor that grabbed a function which
   no longer runs on the page would not. */
ok("the page actually calls these renderers",
   /\$\("profile"\)\.innerHTML = renderProfile\(/.test(page)
   && /\$\("caps"\)\.innerHTML = subjectLine\(d\) \+ renderCaps\(/.test(page),
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
