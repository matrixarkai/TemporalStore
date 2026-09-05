/* The traffic panel is drawn from two sources. They must draw the same table.
 *
 * At load the Setup page reads /v1/metrics and parses the Prometheus text -- that needs no key, so
 * it works before the reader has authenticated. A second later the first live frame arrives and
 * redraws the same element from the metrics snapshot.
 *
 * They used to build different tables: four columns from the scrape, six from the frame. The panel
 * changed shape under the reader on every load, and a deployment whose stream never connects stayed
 * on the four-column one -- with no way to tell that a tail statistic existed at all.
 *
 * Two builders producing "the same" table is not something source reading settles, so both are run
 * over equivalent inputs and their output compared.
 *
 * Usage: node traffic_table_harness.js <setup_portal.html>
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

const parts = ["statusChips", "trafficTable", "renderTraffic", "renderLiveTraffic"];
const sliced = {};
for (const name of parts) {
  sliced[name] = slice(name);
  ok("the page defines " + name, !!sliced[name]);
}
if (parts.some((n) => !sliced[n])) { console.log("FAILED " + failures); process.exit(1); }

ok("both renderers go through one builder",
   /trafficTable\(/.test(sliced.renderTraffic) && /trafficTable\(/.test(sliced.renderLiveTraffic),
   "two builders is how the two views came to disagree");

function run(which, argument) {
  const written = { html: "" };
  const env = {
    $: () => ({ set innerHTML(v) { written.html = v; }, get innerHTML() { return written.html; } }),
    esc: (s) => String(s == null ? "" : s),
  };
  const src = sliced.statusChips + "\n" + sliced.trafficTable + "\n" + sliced[which]
    + "\n; return " + which + ";";
  new Function(...Object.keys(env), src)(...Object.values(env))(argument);
  return written.html;
}

function headers(html) {
  const row = /<thead><tr>([\s\S]*?)<\/tr>/.exec(html);
  return row ? row[1].match(/<th>([^<]*)<\/th>/g).map((h) => h.replace(/<\/?th>/g, "")) : null;
}

/* ---------- the same deployment, described by each source ---------- */
const scrape = run("renderTraffic", {
  matrixark_gateway_requests_total: [
    { labels: { route: "/v1/retrieve", method: "POST", status: "200" }, value: 90 },
    { labels: { route: "/v1/retrieve", method: "POST", status: "401" }, value: 10 },
    { labels: { route: "/v1/ingest", method: "POST", status: "202" }, value: 5 },
  ],
  matrixark_gateway_request_duration_seconds_sum: [
    { labels: { route: "/v1/retrieve" }, value: 2.0 },
  ],
  matrixark_gateway_request_duration_seconds_count: [
    { labels: { route: "/v1/retrieve" }, value: 100 },
  ],
});

const frame = run("renderLiveTraffic", {
  routes: {
    "/v1/retrieve": { requests: 100, errors: 10, statuses: { "200": 90, "401": 10 },
                      avg_ms: 20, p95_ms: 250 },
    "/v1/ingest": { requests: 5, errors: 0, statuses: { "202": 5 }, avg_ms: null, p95_ms: null },
  },
});

ok("the scrape draws a table", /<table>/.test(scrape));
ok("the frame draws a table", /<table>/.test(frame));
ok("both draw the same columns",
   JSON.stringify(headers(scrape)) === JSON.stringify(headers(frame)),
   JSON.stringify(headers(scrape)) + " vs " + JSON.stringify(headers(frame)));
ok("and the columns are the six the frame can fill",
   JSON.stringify(headers(frame)) ===
     JSON.stringify(["Route", "Requests", "Answers", "Errors", "Mean", "95% within"]),
   JSON.stringify(headers(frame)));

/* ---------- what each source can actually say ---------- */
ok("the scrape shows the status breakdown it was throwing away",
   /200×90/.test(scrape) && /401×10/.test(scrape),
   "a route answering 401 read the same as one answering 500");
ok("the scrape marks a 4xx as bad", /status-chip bad/.test(scrape));
ok("the scrape computes the mean it can", /20 ms/.test(scrape), scrape.slice(0, 200));
ok("the scrape leaves the tail column empty rather than guessing",
   !/~\d+ ms/.test(scrape),
   "computing it here would be a second implementation of the gateway's bucket walk");
ok("the frame fills the tail column", /~250 ms/.test(frame));

/* ---------- ordering and emptiness ---------- */
ok("the busiest route is first in both",
   scrape.indexOf("/v1/retrieve") < scrape.indexOf("/v1/ingest")
   && frame.indexOf("/v1/retrieve") < frame.indexOf("/v1/ingest"));

const emptyScrape = run("renderTraffic", {});
const emptyFrame = run("renderLiveTraffic", { routes: {} });
ok("an empty deployment reads the same either way", emptyScrape === emptyFrame,
   emptyScrape + " vs " + emptyFrame);
ok("and says so rather than drawing an empty table", /No requests recorded/.test(emptyFrame));

/* ---------- a column nobody can fill is a dash, not a missing column ---------- */
ok("a route with no timing keeps its row", /\/v1\/ingest/.test(frame));
const ingestRow = /<tr>(?:(?!<\/tr>)[\s\S])*\/v1\/ingest[\s\S]*?<\/tr>/.exec(frame);
ok("and shows a dash where the number is unknown",
   ingestRow && (ingestRow[0].match(/—/g) || []).length >= 2,
   ingestRow && ingestRow[0]);

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
