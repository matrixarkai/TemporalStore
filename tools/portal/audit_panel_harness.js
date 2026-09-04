/* An empty audit table has to say WHICH kind of empty it is.
 *
 * MATRIXARK_AUDIT_MODE defaults to off, so on most deployments an empty audit log means nothing
 * was KEPT -- not that nothing happened. A panel that renders "no records" over both is at its
 * most reassuring exactly when it should not be: every refusal was discarded and the page implies
 * there were none.
 *
 * The endpoint reports the recording mode beside the rows for this reason, and whether the panel
 * uses it is behaviour, not markup. So the page's own three functions are sliced out and run
 * against canned payloads: the network is stubbed because it is the boundary, and everything that
 * decides what the reader sees comes from the page.
 *
 * Usage: node audit_panel_harness.js <api_key_portal.html>
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

const parts = ["sayRecording", "auditDetail", "loadAudit"].map((n) => [n, slice(n)]);
for (const [name, body] of parts) {
  ok("the page defines " + name, !!body);
}
if (parts.some(([, body]) => !body)) { console.log("FAILED " + failures); process.exit(1); }

/* ---------- one run of the panel against one canned response ---------- */
function run(payload, reject) {
  const nodes = {};
  function node(id) {
    const rows = [];
    return {
      id,
      innerHTML: "",
      style: {},
      rows,
      appendChild(child) { rows.push(child); },
      querySelector() { return nodes[id + ":tbody"] || (nodes[id + ":tbody"] = node(id + ":tbody")); },
    };
  }
  const env = {
    $: (id) => (nodes[id] = nodes[id] || node(id)),
    hideMsg(id) { env.$(id).innerHTML = ""; },
    showMsg(id, cls, text) { env.$(id).innerHTML = '<div class="msg ' + cls + '">' + text + "</div>"; },
    escapeHtml: (s) => String(s == null ? "" : s),
    fmtTime: (ms) => "t" + ms,
    gwBase: () => "",
    apiFetch: () => (reject ? Promise.reject(new Error(reject)) : Promise.resolve(payload)),
    document: { createElement: () => node("tr") },
  };

  const src = parts.map(([, body]) => body).join("\n") + "\n; return loadAudit;";
  const loadAudit = new Function(...Object.keys(env), src)(...Object.values(env));
  loadAudit();
  /* The page's loaders do not return their chain -- none of them do, and asking this one to would
     make it the odd one out for the sake of a harness. So the queue is drained instead: the stub
     settles immediately, and what is being waited for is the .then that writes into the panel.
     A single tick is not enough; that is how a check of this shape passes on a panel that never
     rendered anything. */
  return flush().then(() => ({
    recording: env.$("auditRecording").innerHTML,
    message: env.$("auditMsg").innerHTML,
    table: env.$("auditTable"),
    body: env.$("auditTable").querySelector().rows,
  }));
}

async function flush() {
  for (let i = 0; i < 8; i += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

(async () => {
  /* ---------- nothing kept ---------- */
  const off = await run({ status: "ok", audit_logs: [], recording: "off" });
  ok("an empty log with recording off says nothing is being recorded",
     /Nothing is being recorded/.test(off.recording), off.recording);
  ok("and warns rather than reassures", /msg warn/.test(off.recording), off.recording);
  ok("and says an empty list is not evidence",
     /not evidence that nothing happened/.test(off.recording), off.recording);
  ok("and says where to change it", /\/v1\/admin\/setup/.test(off.recording), off.recording);
  ok("it does not also claim nothing has happened yet",
     !/Nothing recorded yet/.test(off.message), off.message);

  /* ---------- nothing happened ---------- */
  const on = await run({ status: "ok", audit_logs: [], recording: "async" });
  ok("an empty log with recording on says nothing has happened yet",
     /Nothing recorded yet/.test(on.message), on.message);
  ok("and does not warn", !/msg warn/.test(on.recording), on.recording);
  ok("and still names the mode", /async/.test(on.recording), on.recording);

  /* ---------- the two are not the same message ---------- */
  ok("the two empties do not read the same", off.recording !== on.recording);

  /* ---------- records ---------- */
  const rows = await run({
    status: "ok", recording: "async",
    audit_logs: [
      { action: "admin.revoke_api_key", status: "denied", api_key_id: "ak_1",
        created_at_ms: 2, details: { requested_tenant_id: "tenant_b" } },
      { action: "admin.create_api_key", status: "ok", api_key_id: "ak_1", created_at_ms: 1 },
    ],
  });
  ok("records are rendered", rows.body.length === 2, String(rows.body.length));
  ok("the table is shown once there is something in it", rows.table.style.display === "");
  const denied = rows.body[0].innerHTML;
  ok("a refusal is marked as one", /pill failed/.test(denied) && /denied/.test(denied), denied);
  ok("and says what was asked for", /tenant_b/.test(denied), denied);
  ok("a success is not marked as a refusal", /pill completed/.test(rows.body[1].innerHTML),
     rows.body[1].innerHTML);
  ok("with records present it does not say nothing was recorded",
     !/Nothing recorded yet/.test(rows.message), rows.message);

  /* ---------- a read that fails is not an empty trail ---------- */
  const bad = await run(null, "insufficient_scope (admin:audit)");
  ok("a failed read says so", /Audit read failed/.test(bad.message), bad.message);
  ok("and names what went wrong", /admin:audit/.test(bad.message), bad.message);
  ok("a failed read leaves no table pretending to be an empty trail",
     bad.body.length === 0 && !/Nothing recorded yet/.test(bad.message), bad.message);

  /* ---------- and it never puts that read on a timer ---------- */
  ok("the panel is not polled",
     !/setInterval\([^)]*loadAudit/.test(page) && !/loadAudit[^)]*\)\s*,\s*[0-9]{3,}/.test(page));

  console.log(failures === 0 ? "PASS" : "FAILED " + failures);
  process.exit(failures === 0 ? 0 : 1);
})();
