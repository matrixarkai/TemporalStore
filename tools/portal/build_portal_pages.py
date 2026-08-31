#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Build the generated portal pages.

    python3 tools/portal/build_portal_pages.py

Five of the seven portal pages -- overview, api, explore, setup, catalog -- are EMITTED from this
script and must not be hand-edited: change the body or the script block here and re-run. They are
generated rather than hand-written so they share one stylesheet and one nav, and so adding a page
updates the nav on all the others in the same step.

The other two -- api_key_portal.html and ingestion_portal.html -- are hand-maintained and predate
this. This script only injects the shared nav into them, replacing the one already there.

The stylesheet is EXTRACTED from ingestion_portal.html rather than duplicated, so the design system
has exactly one home. `--faint` in that file is deliberately darker than it looks like it should be;
the comment there explains why.
"""
import io
import os
import re
import sys

PORTAL = os.path.dirname(os.path.abspath(__file__))

style_src = io.open(os.path.join(PORTAL, "ingestion_portal.html"), encoding="utf-8").read()
match = re.search(r"<style>\n(.*?)\n</style>", style_src, re.S)
if not match:
    print("could not extract the ingestion portal stylesheet")
    sys.exit(1)
BASE_CSS = match.group(1)

NAV_CSS = """
  /* shared portal nav (same markup on every page; both portal stylesheets define these vars) */
  .portalnav{display:flex;gap:6px;flex-wrap:wrap;margin:0 0 20px;padding-bottom:2px;
             border-bottom:1px solid var(--line)}
  .portalnav a{display:inline-block;padding:7px 12px;font-size:13.5px;font-weight:550;
               text-decoration:none;color:var(--muted);border-bottom:2px solid transparent;
               margin-bottom:-1px}
  .portalnav a:hover{color:var(--accent)}
  .portalnav a[aria-current=page]{color:var(--accent);border-bottom-color:var(--accent)}
"""

EXTRA_CSS = """
  /* setup + catalog additions */
  .field{margin-bottom:2px}
  .field .env{font-size:11.5px;color:var(--faint);font-family:"IBM Plex Mono",monospace}
  .badge{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
         padding:2px 7px;border-radius:20px;margin-left:7px;white-space:nowrap}
  .badge.restart{background:var(--warn-soft);color:var(--warn)}
  .badge.essential{background:var(--crit-soft);color:var(--crit)}
  .badge.portal{background:var(--accent-soft);color:var(--accent)}
  .badge.environment{background:var(--line);color:var(--muted)}
  select{width:100%;padding:9px 11px;border:1px solid var(--line);border-radius:7px;
         background:var(--panel-2);color:var(--ink);font:inherit;font-size:14px}
  .presets{display:flex;gap:8px;flex-wrap:wrap}
  .preset{flex:1 1 210px;text-align:left;background:var(--panel-2);color:var(--ink);
          border:1px solid var(--line);border-radius:8px;padding:11px 13px;font-weight:500}
  .preset b{display:block;font-weight:650;margin-bottom:3px}
  .preset span{display:block;font-size:12.5px;color:var(--muted);font-weight:400;line-height:1.45}
  .preset:hover{border-color:var(--accent)}
  .probe{border:1px solid var(--line);border-radius:8px;padding:11px 13px;margin-bottom:9px;
         background:var(--panel-2)}
  .probe.ok{border-color:var(--ok)} .probe.bad{border-color:var(--crit)}
  .probe h3{margin:0 0 5px;font-size:14px;font-weight:650;display:flex;gap:9px;align-items:center}
  .probe p{margin:0;font-size:13px;color:var(--muted)}
  tr.rowlink{cursor:pointer} tr.rowlink:hover td{background:var(--panel-2)}
  .tag{font-size:11px;background:var(--line);color:var(--muted);border-radius:4px;padding:1px 6px;
       margin-right:5px;white-space:nowrap}
  /* setup page: grouped, filterable settings */
  details.group{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);
                padding:0;margin-bottom:12px;box-shadow:var(--shadow);overflow:hidden}
  details.group>summary{list-style:none;cursor:pointer;padding:15px 20px;display:flex;
                        align-items:baseline;gap:10px;flex-wrap:wrap}
  details.group>summary::-webkit-details-marker{display:none}
  details.group>summary::before{content:"\25B8";color:var(--faint);font-size:12px;
                                transition:transform .15s ease;display:inline-block}
  details.group[open]>summary::before{transform:rotate(90deg)}
  details.group>summary b{font-size:15px;font-weight:650;letter-spacing:-.01em}
  details.group>summary .n{font-size:11.5px;color:var(--faint);
                           font-family:"IBM Plex Mono",monospace}
  details.group>summary .adv{font-size:10px;font-weight:700;text-transform:uppercase;
                             letter-spacing:.05em;color:var(--muted);background:var(--line);
                             border-radius:20px;padding:2px 7px}
  details.group>summary .dirty{font-size:10px;font-weight:700;text-transform:uppercase;
                               letter-spacing:.05em;color:var(--warn);background:var(--warn-soft);
                               border-radius:20px;padding:2px 7px}
  .group-body{padding:0 20px 18px}
  .group-note{color:var(--muted);font-size:13.5px;margin:0 0 14px;max-width:70ch;line-height:1.55}
  .field.changed input,.field.changed select{border-color:var(--warn);
                                             background:var(--warn-soft)}
  .field .row{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
  .field .reset{margin-left:auto}
  .field.hidden{display:none}
  .helptext{font-size:13px;color:var(--faint);margin-top:9px;line-height:1.5;max-width:70ch}
  .helptext.long{max-height:3.6em;overflow:hidden;position:relative;cursor:pointer}
  .helptext.long::after{content:"more";position:absolute;right:0;bottom:0;background:var(--panel);
                        padding-left:10px;color:var(--accent);font-weight:600}
  .helptext.long.open{max-height:none}
  .helptext.long.open::after{content:none}
  .savebar{position:sticky;bottom:0;z-index:5;background:var(--panel);border:1px solid var(--line);
           border-radius:var(--radius);box-shadow:var(--shadow);padding:12px 16px;margin:16px 0;
           display:flex;gap:10px;align-items:center;flex-wrap:wrap}
  .savebar .count{font-size:13.5px;color:var(--muted);margin-right:auto}
  .savebar .count b{color:var(--warn)}
  .toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
  .toolbar input[type=text]{flex:1 1 240px;width:auto}
  .inv{width:100%;border-collapse:collapse;font-size:13.5px;margin-bottom:16px}
  .inv th{width:1%;white-space:nowrap;text-align:left;padding:6px 14px 6px 0;font-weight:500;
          color:var(--muted);text-transform:none;letter-spacing:0;font-size:13.5px}
  .inv td{padding:6px 0;border-bottom:1px solid var(--line);
          font-family:"IBM Plex Mono",monospace;font-size:12.5px;overflow-wrap:anywhere}
  .inv td.unset{color:var(--faint);font-style:italic;font-family:inherit}
  .inv .envname{display:block;font-size:11px;color:var(--faint);
                font-family:"IBM Plex Mono",monospace}
  #history td div{font-family:"IBM Plex Mono",monospace;font-size:12.5px;padding:1px 0;
                  overflow-wrap:anywhere}
  #history em{font-style:normal;color:var(--muted)}
  /* overview checklist + explore */
  .checkrow{display:flex;gap:13px;align-items:flex-start;padding:13px 0;
            border-bottom:1px solid var(--line)}
  .checkrow:last-child{border-bottom:0}
  .checkrow .mark{flex:0 0 20px;height:20px;border-radius:50%;display:grid;place-items:center;
                  font-size:12px;font-weight:800;margin-top:1px}
  .checkrow .mark.ok{background:var(--ok-soft);color:var(--ok)}
  .checkrow .mark.warn{background:var(--warn-soft);color:var(--warn)}
  .checkrow .mark.todo{background:var(--crit-soft);color:var(--crit)}
  .checkrow .body{flex:1 1 auto;min-width:0}
  .checkrow .body b{display:block;font-size:14.5px;font-weight:650;margin-bottom:2px}
  .checkrow .body span{font-size:13.5px;color:var(--muted);line-height:1.5;
                       overflow-wrap:anywhere;display:block}
  .checkrow .go{flex:0 0 auto;font-size:13px;white-space:nowrap}
  .importrow{border:1px solid var(--line);border-radius:8px;padding:11px 13px;margin-bottom:9px;
             background:var(--panel-2)}
  .importhead{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
  .importhead .mono{font-family:"IBM Plex Mono",monospace;font-size:12.5px}
  .importmeta{margin-left:auto;font-size:12.5px;color:var(--muted);
              font-variant-numeric:tabular-nums}
  .importcur{font-family:"IBM Plex Mono",monospace;font-size:11.5px;color:var(--faint);
             margin-top:6px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .how{margin-top:8px}
  .how>summary{cursor:pointer;font-size:12.5px;color:var(--accent);font-weight:550;
               list-style:none;display:inline-block}
  .how>summary::-webkit-details-marker{display:none}
  .how>summary::before{content:"\25B8 ";font-size:10px}
  .how[open]>summary::before{content:"\25BE "}
  .how ol{margin:8px 0 0;padding-left:20px}
  .how li{font-size:13px;color:var(--muted);line-height:1.55;margin-bottom:5px;
          overflow-wrap:anywhere}
  .progress{height:8px;border-radius:5px;background:var(--line);overflow:hidden;margin:4px 0 18px}
  .progress>i{display:block;height:100%;background:var(--ok);transition:width .5s ease}
  .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;
         margin-bottom:6px}
  .tile{display:block;text-decoration:none;color:inherit;background:var(--panel);
        border:1px solid var(--line);border-radius:var(--radius);padding:14px 16px;
        box-shadow:var(--shadow)}
  .tile:hover{border-color:var(--accent)}
  .tile b{display:block;font-size:14.5px;font-weight:650;margin-bottom:3px}
  .tile span{font-size:12.5px;color:var(--muted);line-height:1.45;display:block}
  .packitem{border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:7px;
            padding:11px 13px;margin-bottom:9px;background:var(--panel-2)}
  .packitem .meta{font-size:11.5px;color:var(--faint);margin-bottom:5px;
                  font-family:"IBM Plex Mono",monospace}
  .packitem p{margin:0;font-size:13.5px;line-height:1.55;overflow-wrap:anywhere}
  .tabs{display:flex;gap:4px;border-bottom:1px solid var(--line);margin-bottom:16px;flex-wrap:wrap}
  .tabs button{background:none;border:0;border-bottom:2px solid transparent;color:var(--muted);
               padding:8px 12px;font-weight:550;font-size:13.5px;margin-bottom:-1px;
               border-radius:0}
  .tabs button[aria-selected=true]{color:var(--accent);border-bottom-color:var(--accent)}
  .pane[hidden]{display:none}
  .upl{border:1px solid var(--line);border-radius:8px;padding:10px 12px;margin-top:9px;
       background:var(--panel-2)}
  .upl-head{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
  .upl-name{font-family:"IBM Plex Mono",monospace;font-size:12.5px;overflow-wrap:anywhere;
            flex:1 1 auto;min-width:0}
  .upl-size{font-size:12px;color:var(--faint);font-variant-numeric:tabular-nums}
  .upl-bar{height:5px;background:var(--line);border-radius:4px;overflow:hidden;margin-top:8px}
  .upl-bar>i{display:block;height:100%;background:var(--accent);width:0;transition:width .2s linear}
  .upl.done .upl-bar>i{background:var(--ok)}
  .upl.failed .upl-bar>i{background:var(--crit)}
  .upl-detail{font-size:12.5px;color:var(--muted);margin-top:7px;overflow-wrap:anywhere}
  .upl-actions{margin-top:8px}
  /* API page */
  .route{border-bottom:1px solid var(--line);padding:14px 0}
  .route:last-child{border-bottom:0}
  .route-head{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
  .verb{font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;letter-spacing:.04em;
        padding:2px 7px;border-radius:4px;background:var(--line);color:var(--muted)}
  .verb.GET{background:var(--accent-soft);color:var(--accent)}
  .verb.POST{background:var(--ok-soft);color:var(--ok)}
  .verb.PUT{background:var(--warn-soft);color:var(--warn)}
  .route-path{font-family:"IBM Plex Mono",monospace;font-size:13.5px;font-weight:600;
              overflow-wrap:anywhere}
  .route-scope{margin-left:auto;font-size:11.5px;font-family:"IBM Plex Mono",monospace;
               color:var(--faint);white-space:nowrap}
  .route p{margin:6px 0 0;font-size:13.5px;color:var(--muted);line-height:1.55;max-width:78ch}
  .route .curl{margin-top:9px}
  .route pre{margin-top:8px;display:none}
  .route.open pre{display:block}
  .visually-hidden{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;
                   clip:rect(0 0 0 0);white-space:nowrap;border:0}
"""

NAV_LINKS = [
    ("/v1/admin", "Overview"),
    ("/v1/admin/setup", "Setup &amp; metrics"),
    ("/v1/admin/catalog", "Skills &amp; resources"),
    ("/v1/admin/explore", "Explore"),
    ("/v1/admin/ingestion", "Ingestion"),
    ("/v1/admin/portal", "API keys"),
    ("/v1/admin/api", "API"),
]


def nav(active):
    parts = []
    for href, label in NAV_LINKS:
        current = ' aria-current="page"' if href == active else ""
        parts.append('    <a href="%s"%s>%s</a>' % (href, current, label))
    return '  <nav class="portalnav">\n' + "\n".join(parts) + "\n  </nav>"


HEAD = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%(title)s</title>
<style>
%(css)s
</style>
</head>
<body>
<div class="wrap">
"""

# ================================================================================================
SETUP_BODY = """
  <header>
    <h1>Setup &amp; metrics</h1>
    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>
%(nav)s
  <p class="lede">Point this deployment at the models it should use, confirm the endpoints actually
    answer, tune what it does with them, and see the traffic it is serving — without shell access
    to the server.</p>

  <div class="summary" id="summary"></div>

  <section>
    <h2>Access <span class="aux"><label class="check"><input type="checkbox" id="remember"> remember for this browser tab</label></span></h2>
    <label for="key">Admin API key</label>
    <input id="key" type="password" placeholder="Key carrying an admin scope" autocomplete="off" spellcheck="false">
    <div class="hint">Reading this page needs nothing; every action calls an admin-gated endpoint, so
      the page is inert without a key. “Remember” keeps it in this tab only and forgets it when the
      tab closes — it is never written to disk or sent anywhere but this gateway.</div>
  </section>

  <div id="warnings"></div>

  <section>
    <h2>Start from a provider</h2>
    <p class="hint" style="margin-top:0">A preset only fills the fields below — it writes nothing you
      could not type by hand. Review, add your key, then save.</p>
    <div class="presets" id="presets"><div class="empty">Loading…</div></div>
  </section>

  <form id="settingsForm">
    <div class="toolbar">
      <label for="filter" class="visually-hidden">Filter settings</label>
      <input id="filter" type="search" placeholder="Filter settings — name, help text, or variable" spellcheck="false" autocomplete="off">
      <label class="check"><input type="checkbox" id="onlyEssential"> only what I must set</label>
      <label class="check"><input type="checkbox" id="showAdvanced"> show advanced groups</label>
      <label class="check"><input type="checkbox" id="onlyChanged"> only changed</label>
    </div>
    <div id="groups"><section><div class="empty">Enter an admin key to load this deployment’s
      configuration.</div></section></div>

    <div class="savebar">
      <span class="count" id="dirtyCount">No unsaved changes</span>
      <button id="save" type="submit">Save configuration</button>
      <button id="test" class="ghost" type="button">Test endpoints</button>
      <button id="reload" class="link" type="button">Discard changes</button>
    </div>
    <div id="saveMsg" role="status" aria-live="polite"></div>
    <div class="hint" id="cfgTime"></div>
  </form>

  <section>
    <h2>Recent changes</h2>
    <p class="hint" style="margin-top:0">The last writes to this deployment's configuration, newest
      first. A key rotation is recorded as having happened; the key itself is never written down.</p>
    <div id="history"><div class="empty">Nothing has been changed from the portal yet.</div></div>
  </section>

  <section>
    <h2>Move this configuration <span class="aux">
      <button class="link" id="exportCfg" type="button">export</button>
      <button class="link" id="copyCfgCurl" type="button">copy as curl</button></span></h2>
    <p class="hint" style="margin-top:0">Export emits exactly what the write endpoint accepts, so
      making another deployment match this one is one request rather than 79 fields retyped.
      Secrets are never exported — set the keys on the target afterwards.</p>
    <label for="importText">Paste a configuration to apply</label>
    <textarea id="importText" placeholder='{"settings": {"embedding.provider": "openai_compatible", ...}}' spellcheck="false"></textarea>
    <div class="actions"><button id="importCfg" class="ghost" type="button">Apply pasted configuration</button></div>
    <div id="importMsg" role="status" aria-live="polite"></div>
  </section>

  <section>
    <h2>Endpoint test</h2>
    <p class="hint" style="margin-top:0">Reading configuration back proves only that it was stored.
      This calls the endpoints as configured. Both extraction and embedding fall back to a local
      deterministic path when a call fails — that path answers 200, so a rejected key and a working
      one are indistinguishable at the API surface until you probe them.</p>
    <div id="probe" role="status" aria-live="polite"><div class="empty">Not run yet.</div></div>
  </section>

  <section>
    <h2>Traffic <span class="aux"><label class="check"><input type="checkbox" id="auto" checked> auto-refresh</label></span></h2>
    <div id="traffic"><div class="empty">Loading…</div></div>
  </section>

  <section>
    <h2>Deployment</h2>
    <p class="hint" style="margin-top:0">Where this deployment lives and how it is reached. Read
      only, deliberately: repointing a storage directory or a cluster address from a browser form
      does not reconfigure a running deployment, it strands its data. Change these where the process
      is started.</p>
    <div id="inventory"><div class="empty">Enter an admin key to see the deployment.</div></div>
  </section>

  <section>
    <h2>Grafana <span class="aux"><button class="link" id="copyScrape" type="button">copy scrape config</button></span></h2>
    <p class="hint" style="margin-top:0">Everything on this page is also exported at
      <span class="mono">/v1/metrics</span> in Prometheus text format — aggregate counters only, no
      keys and no tenant identifiers, so it is safe to scrape without credentials. Import
      <span class="mono">docs/ops/matrixark-gateway-dashboard.json</span> for the request/latency
      panels and <span class="mono">docs/ops/matrixark-ingestion-dashboard.json</span> for import
      backlog.</p>
    <table>
      <tr><td class="mono">matrixark_gateway_embedding_semantic</td><td>0 means retrieval is running on hash
        vectors. Alert on it — nothing else distinguishes that deployment from a healthy one.</td></tr>
      <tr><td class="mono">matrixark_gateway_extraction_model_active</td><td>0 means ingest stores only what the
        local rules extract, with no model call.</td></tr>
      <tr><td class="mono">matrixark_gateway_config_warnings</td><td>How many configuration warnings are live
        right now. Steady state is 0.</td></tr>
      <tr><td class="mono">matrixark_gateway_request_duration_seconds</td><td>Edge latency histogram, by route.</td></tr>
      <tr><td class="mono">matrixark_gateway_requests_total</td><td>Edge requests by route, method and status.</td></tr>
    </table>
    <pre id="scrape"></pre>
  </section>
"""

SETUP_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  var loaded = null;      /* last GET /v1/admin/config payload */
  var fields = {};        /* setting key -> field descriptor from the server */
  var edits = {};         /* setting key -> value typed/reset since the last load */
  var timer = null;

  function auth() {
    var k = $("key").value.trim();
    return k ? { Authorization: "Bearer " + k } : {};
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function say(el, text, cls) {
    el.innerHTML = text ? '<div class="msg ' + (cls || "info") + '">' + esc(text) + "</div>" : "";
  }
  function conn(state, text) {
    $("dot").className = "dot " + state;
    $("conn").textContent = text;
  }
  /* One phrasing for every failure on the page. "The gateway answered 403" tells a customer
     nothing they can act on; naming the cause turns most of these into a self-service fix. */
  function failure(status) {
    if (status === 401) { return "This key was not accepted."; }
    if (status === 403) { return "This key lacks an admin scope. Issue one with admin:api_key."; }
    if (status === 413) { return "That request was too large."; }
    if (status === 429) { return "Rate limited — the edge is throttling this key."; }
    if (status === 504) { return "The backend did not answer in time."; }
    return "The gateway answered " + status + ".";
  }

  /* ---------- summary + warnings ---------- */
  function renderSummary(d) {
    var det = { "": 1, deterministic: 1, rules: 1, local: 1 };
    var semantic = !det[(d.embedding || {}).provider];
    var extracting = !det[(d.extraction || {}).provider];
    var warn = (d.warnings || []).length;
    var cards = [
      { n: extracting ? "model" : "rules only", l: "extraction", c: extracting ? "ok" : "warn" },
      { n: semantic ? "semantic" : "hash vectors", l: "retrieval", c: semantic ? "ok" : "warn" },
      { n: warn, l: warn === 1 ? "warning" : "warnings", c: warn ? "warn" : "ok" },
      { n: Object.keys(fields).length, l: "settings", c: "" }
    ];
    $("summary").innerHTML = cards.map(function (c) {
      return '<div class="stat ' + c.c + '"><b>' + esc(c.n) + "</b><span>" + esc(c.l) + "</span></div>";
    }).join("");
    $("warnings").innerHTML = (d.warnings || []).map(function (w) {
      return '<div class="note">' + esc(w) + "</div>";
    }).join("");
  }

  /* ---------- presets ---------- */
  function renderPresets(presets) {
    var names = Object.keys(presets || {});
    if (!names.length) { $("presets").innerHTML = '<div class="empty">None available.</div>'; return; }
    $("presets").innerHTML = names.map(function (name) {
      var p = presets[name];
      return '<button type="button" class="preset" data-preset="' + esc(name) + '"><b>' +
        esc(p.label) + "</b><span>" + esc(p.note) + "</span></button>";
    }).join("");
  }

  /* ---------- settings ---------- */
  function fieldId(key) { return "f_" + key.replace(/[^a-z0-9]/gi, "_"); }

  function controlHtml(f) {
    var id = fieldId(f.key), attr = ' id="' + id + '" data-key="' + esc(f.key) + '"';
    if (f.kind === "secret") {
      return '<input type="password"' + attr + ' autocomplete="off" spellcheck="false" placeholder="' +
        (f.configured ? "configured — leave blank to keep" : "not set") + '">';
    }
    if (f.kind === "bool") {
      return "<select" + attr + ">" +
        '<option value="0"' + (f.value === "1" ? "" : " selected") + ">off</option>" +
        '<option value="1"' + (f.value === "1" ? " selected" : "") + ">on</option></select>";
    }
    if (f.choices && f.choices.length) {
      return "<select" + attr + ">" + f.choices.map(function (c) {
        return '<option value="' + esc(c) + '"' + (c === f.value ? " selected" : "") + ">" +
          esc(c) + "</option>";
      }).join("") + "</select>";
    }
    return '<input type="text"' + attr + ' spellcheck="false" value="' + esc(f.value || "") + '">';
  }

  function fieldHtml(f) {
    var badges = "";
    if (f.essential) { badges += '<span class="badge essential">required</span>'; }
    if (f.applies === "restart") { badges += '<span class="badge restart">needs restart</span>'; }
    if (f.source && f.source !== "default") {
      badges += '<span class="badge ' + esc(f.source) + '">' + esc(f.source) + "</span>";
    }
    var resettable = f.kind !== "secret" &&
      ((f.value || "") !== (f.default == null ? "" : String(f.default)));
    var reset = resettable
      ? '<button type="button" class="link reset" data-reset="' + esc(f.key) + '">reset</button>' : "";
    var help = esc(f.help) + (f.env ? ' <span class="env">' + esc(f.env) + "</span>" : "");
    var longCls = f.help.length > 190 ? " long" : "";
    return '<div class="field" data-field="' + esc(f.key) + '" data-essential="' +
      (f.essential ? "1" : "0") + '" data-search="' +
      esc((f.label + " " + f.env + " " + f.help + " " + f.key).toLowerCase()) + '">' +
      '<label class="row" for="' + fieldId(f.key) + '">' + esc(f.label) + badges + reset + "</label>" +
      controlHtml(f) + '<div class="helptext' + longCls + '">' + help + "</div></div>";
  }

  function renderGroups(settings) {
    var groups = (settings || {}).groups || {};
    var meta = (settings || {}).group_meta || {};
    fields = {};
    Object.keys(groups).forEach(function (g) {
      groups[g].forEach(function (f) { fields[f.key] = f; });
    });
    var order = Object.keys(groups).sort(function (a, b) {
      return ((meta[a] || {}).order || 999) - ((meta[b] || {}).order || 999);
    });
    $("groups").innerHTML = order.map(function (g) {
      var m = meta[g] || {};
      var open = m.advanced ? "" : " open";
      return '<details class="group" data-group="' + esc(g) + '"' + open + '><summary><b>' +
        esc(m.title || g) + '</b><span class="n">' + groups[g].length + " settings</span>" +
        (m.advanced ? '<span class="adv">advanced</span>' : "") +
        '<span class="dirty" data-dirty="' + esc(g) + '" hidden></span></summary>' +
        '<div class="group-body">' +
        (m.note ? '<p class="group-note">' + esc(m.note) + "</p>" : "") +
        '<div class="grid2">' + groups[g].map(fieldHtml).join("") + "</div></div></details>";
    }).join("");
    $("cfgTime").textContent = settings && settings.updated_at
      ? "Stored in " + (settings.config_file || "the runtime config") + " · last saved " +
        new Date(settings.updated_at * 1000).toLocaleString()
      : "Stored in " + ((settings || {}).config_file || "the runtime config") +
        " · nothing saved from the portal yet";
    applyFilter();
    markDirty();
  }

  /* ---------- change log ---------- */
  function renderHistory(settings) {
    var entries = (settings || {}).history || [];
    if (!entries.length) { return; }
    $("history").innerHTML = "<table><thead><tr><th>When</th><th>By</th><th>Change</th>" +
      "</tr></thead><tbody>" + entries.map(function (e) {
        var what = (e.changes || []).map(function (c) {
          if (c.action === "reset") {
            return "<div>" + esc(c.key) + " <em>reset to default</em></div>";
          }
          if (c.secret) {
            return "<div>" + esc(c.key) + " <em>set</em></div>";
          }
          return "<div>" + esc(c.key) + " <em>" + esc(c.from || "unset") + " → " +
            esc(c.to || "unset") + "</em></div>";
        }).join("");
        var restart = (e.restart_required || []).length
          ? '<div class="hint" style="margin:4px 0 0">needed a restart: ' +
            esc(e.restart_required.join(", ")) + "</div>" : "";
        return "<tr><td>" + esc(new Date(e.at * 1000).toLocaleString()) + "</td><td>" +
          esc(e.by) + "</td><td>" + what + restart + "</td></tr>";
      }).join("") + "</tbody></table>";
  }

  /* ---------- deployment inventory (read-only) ---------- */
  function renderInventory(settings) {
    var blocks = (settings || {}).inventory || [];
    if (!blocks.length) { return; }
    $("inventory").innerHTML = blocks.map(function (b) {
      return "<h3 style='font-size:13px;margin:14px 0 6px;font-weight:650'>" + esc(b.group) +
        '</h3><table class="inv">' + b.rows.map(function (r) {
          var value = r.set
            ? '<td>' + esc(r.redacted ? "configured" : r.value) + "</td>"
            : '<td class="unset">not set</td>';
          return "<tr><th>" + esc(r.label) + '<span class="envname">' + esc(r.env) +
            "</span></th>" + value + "</tr>";
        }).join("") + "</table>";
    }).join("");
  }

  /* ---------- dirty tracking ---------- */
  function isChanged(key, value) {
    var f = fields[key];
    if (!f) { return false; }
    if (f.kind === "secret") { return value !== ""; }
    return value !== (f.value || "");
  }

  function pendingPatch() {
    var patch = {};
    Object.keys(edits).forEach(function (k) {
      var v = edits[k];
      if (v === null) { patch[k] = null; return; }        /* explicit reset */
      if (isChanged(k, v)) { patch[k] = v; }
    });
    return patch;
  }

  function markDirty() {
    var patch = pendingPatch(), keys = Object.keys(patch);
    var perGroup = {};
    Array.prototype.forEach.call(document.querySelectorAll("[data-field]"), function (el) {
      var on = Object.prototype.hasOwnProperty.call(patch, el.dataset.field);
      el.classList.toggle("changed", on);
      if (on) {
        var g = (fields[el.dataset.field] || {}).group ||
          (el.closest("details") || { dataset: {} }).dataset.group;
        perGroup[g] = (perGroup[g] || 0) + 1;
      }
    });
    Array.prototype.forEach.call(document.querySelectorAll("[data-dirty]"), function (el) {
      var n = perGroup[el.dataset.dirty] || 0;
      el.hidden = !n;
      el.textContent = n + " changed";
    });
    $("dirtyCount").innerHTML = keys.length
      ? "<b>" + keys.length + "</b> unsaved change" + (keys.length === 1 ? "" : "s")
      : "No unsaved changes";
    $("save").disabled = !keys.length;
  }

  /* ---------- filtering ---------- */
  function applyFilter() {
    var q = $("filter").value.trim().toLowerCase();
    var advanced = $("showAdvanced").checked;
    var onlyChanged = $("onlyChanged").checked;
    var onlyEssential = $("onlyEssential").checked;
    var patch = pendingPatch();
    Array.prototype.forEach.call(document.querySelectorAll("details.group"), function (group) {
      var shown = 0;
      Array.prototype.forEach.call(group.querySelectorAll("[data-field]"), function (el) {
        var ok = (!q || el.dataset.search.indexOf(q) >= 0) &&
                 (!onlyEssential || el.dataset.essential === "1") &&
                 (!onlyChanged || Object.prototype.hasOwnProperty.call(patch, el.dataset.field));
        el.classList.toggle("hidden", !ok);
        if (ok) { shown++; }
      });
      var meta = ((loaded || {}).settings || {}).group_meta || {};
      var isAdvanced = (meta[group.dataset.group] || {}).advanced;
      group.hidden = shown === 0;
      /* A search or a changed-only view has to open the collapsed groups, otherwise the matches
         are behind a closed triangle and the box reads as "no results". */
      if (q || onlyChanged || onlyEssential) { group.open = shown > 0; }
      else if (isAdvanced && !advanced) { group.open = false; }
      var counter = group.querySelector("summary .n");
      if (counter) {
        counter.textContent = shown +
          (q || onlyChanged || onlyEssential ? " matching" : " settings");
      }
    });
  }

  /* ---------- load ---------- */
  function load() {
    fetch("/v1/admin/config", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        conn("live", "connected");
        loaded = d; edits = {};
        renderGroups(d.settings);
        renderSummary(d);
        renderPresets((d.settings || {}).presets);
        renderHistory(d.settings);
        renderInventory(d.settings);
      })
      .catch(function (e) {
        if (e === 401 || e === 403) {
          conn("live", "connected");
          $("groups").innerHTML = '<section><div class="empty">Enter an admin-scoped key above to ' +
            "load and change this deployment’s configuration.</div></section>";
          $("presets").innerHTML = '<div class="empty">Enter an admin key to see the presets.</div>';
        } else {
          conn("down", "gateway unreachable");
          $("groups").innerHTML = '<section><div class="msg err">Could not reach the gateway.</div></section>';
        }
      });
  }

  /* ---------- save ---------- */
  function save(ev) {
    ev.preventDefault();
    var patch = pendingPatch();
    if (!Object.keys(patch).length) { say($("saveMsg"), "Nothing changed.", "info"); return; }
    $("save").disabled = true;
    fetch("/v1/admin/config", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({ settings: patch })
    })
      .then(function (r) { return r.json().then(function (b) { return { ok: r.ok, body: b }; }); })
      .then(function (res) {
        $("save").disabled = false;
        if (!res.ok) {
          say($("saveMsg"), res.body.detail || res.body.error || "Save failed.", "err");
          return;
        }
        var restart = res.body.restart_required || [];
        var one = restart.length === 1;
        say($("saveMsg"), restart.length
          ? "Saved. " + restart.length + " setting" + (one ? "" : "s") + " (" +
            restart.join(", ") + ") " + (one ? "is" : "are") + " read once at startup — restart " +
            "the gateway for " + (one ? "it" : "them") + " to take effect. Everything else is " +
            "live now."
          : "Saved and live now.", restart.length ? "info" : "ok");
        load();
      })
      .catch(function () {
        $("save").disabled = false;
        say($("saveMsg"), "Could not reach the gateway.", "err");
      });
  }

  /* ---------- probe ---------- */
  function probeHtml(r) {
    var cls = r.ok ? "ok" : (r.skipped ? "" : "bad");
    var title = r.target === "extraction" ? "Extraction" : "Embedding";
    var detail;
    if (r.ok) {
      var extra = r.response && r.response.dimensions ? " · " + r.response.dimensions + " dimensions" : "";
      var model = r.response && r.response.model ? " · " + r.response.model : "";
      detail = "answered in " + r.latency_ms + " ms" + model + extra;
    } else if (r.skipped) {
      detail = r.detail || r.error;
    } else {
      detail = (r.error || "failed") + (r.detail ? " — " + r.detail : "");
    }
    return '<div class="probe ' + cls + '"><h3>' + esc(title) +
      '<span class="pill ' + (r.ok ? "completed" : (r.skipped ? "queued" : "failed")) + '">' +
      (r.ok ? "ok" : (r.skipped ? "not configured" : "failed")) + "</span></h3>" +
      "<p>" + esc(detail) + (r.url ? ' <span class="mono">' + esc(r.url) + "</span>" : "") + "</p></div>";
  }

  function test() {
    $("test").disabled = true;
    $("probe").innerHTML = '<div class="empty">Calling the configured endpoints…</div>';
    fetch("/v1/admin/config/test", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: "{}"
    })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        $("test").disabled = false;
        $("probe").innerHTML = (d.results || []).map(probeHtml).join("") ||
          '<div class="empty">Nothing to probe.</div>';
      })
      .catch(function (e) {
        $("test").disabled = false;
        $("probe").innerHTML = '<div class="msg err">' +
          esc(typeof e === "number" ? failure(e) : "Could not reach the gateway.") + "</div>";
      });
  }

  /* ---------- traffic ---------- */
  /* Parsed from the same /v1/metrics text Prometheus scrapes, so what this shows and what the
     dashboard shows cannot drift, and loading it here also proves the scrape endpoint answers. */
  function parseProm(text) {
    var series = {};
    text.split("\n").forEach(function (line) {
      if (!line || line.charAt(0) === "#") { return; }
      var m = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[0-9.eE+]+|NaN)$/.exec(line.trim());
      if (!m) { return; }
      var labels = {};
      if (m[2]) {
        m[2].slice(1, -1).replace(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g,
          function (_all, k, v) { labels[k] = v; return ""; });
      }
      (series[m[1]] = series[m[1]] || []).push({ labels: labels, value: parseFloat(m[3]) });
    });
    return series;
  }

  function renderTraffic(series) {
    var byRoute = {};
    (series.matrixark_gateway_requests_total || []).forEach(function (s) {
      var r = byRoute[s.labels.route] = byRoute[s.labels.route] || { n: 0, err: 0 };
      r.n += s.value;
      if (parseInt(s.labels.status, 10) >= 400) { r.err += s.value; }
    });
    (series.matrixark_gateway_request_duration_seconds_sum || []).forEach(function (s) {
      (byRoute[s.labels.route] = byRoute[s.labels.route] || { n: 0, err: 0 }).sum = s.value;
    });
    (series.matrixark_gateway_request_duration_seconds_count || []).forEach(function (s) {
      (byRoute[s.labels.route] = byRoute[s.labels.route] || { n: 0, err: 0 }).cnt = s.value;
    });
    var routes = Object.keys(byRoute).sort(function (a, b) { return byRoute[b].n - byRoute[a].n; });
    if (!routes.length) {
      $("traffic").innerHTML = '<div class="empty">No requests recorded yet on this worker.</div>';
      return;
    }
    $("traffic").innerHTML = "<table><thead><tr><th>Route</th><th>Requests</th><th>Errors</th>" +
      "<th>Mean latency</th></tr></thead><tbody>" + routes.map(function (r) {
        var v = byRoute[r];
        var mean = v.cnt ? (v.sum / v.cnt * 1000).toFixed(1) + " ms" : "—";
        return "<tr><td><span class='mono'>" + esc(r) + "</span></td><td class='num'>" +
          v.n + "</td><td class='num'>" + (v.err || 0) + "</td><td class='num'>" + mean + "</td></tr>";
      }).join("") + "</tbody></table>";
  }

  function loadTraffic() {
    fetch("/v1/metrics?ts=" + Date.now())
      .then(function (r) { return r.ok ? r.text() : Promise.reject(r.status); })
      .then(function (t) { renderTraffic(parseProm(t)); })
      .catch(function () {
        $("traffic").innerHTML = '<div class="msg err">Could not read /v1/metrics.</div>';
      });
  }

  function scrapeConfig() {
    return "scrape_configs:\n" +
      "  - job_name: matrixark_gateway\n" +
      "    metrics_path: /v1/metrics\n" +
      "    static_configs:\n" +
      "      - targets: ['" + location.host + "']\n";
  }

  /* ---------- wiring ---------- */
  $("key").addEventListener("change", function () {
    if ($("remember").checked) {
      try { sessionStorage.setItem("matrixark_admin_key", $("key").value); } catch (e) { /* ignore */ }
    }
    load();
  });
  $("remember").addEventListener("change", function () {
    try {
      if ($("remember").checked) { sessionStorage.setItem("matrixark_admin_key", $("key").value); }
      else { sessionStorage.removeItem("matrixark_admin_key"); }
    } catch (e) { /* ignore */ }
  });
  $("groups").addEventListener("input", function (ev) {
    var key = ev.target && ev.target.dataset ? ev.target.dataset.key : null;
    if (!key) { return; }
    edits[key] = ev.target.value;
    markDirty();
  });
  $("groups").addEventListener("change", function (ev) {
    var key = ev.target && ev.target.dataset ? ev.target.dataset.key : null;
    if (!key) { return; }
    edits[key] = ev.target.value;
    markDirty();
  });
  $("groups").addEventListener("click", function (ev) {
    var help = ev.target.closest ? ev.target.closest(".helptext.long") : null;
    if (help) { help.classList.toggle("open"); return; }
    var button = ev.target.closest ? ev.target.closest("[data-reset]") : null;
    if (!button) { return; }
    ev.preventDefault();
    var key = button.dataset.reset, f = fields[key];
    var el = $("groups").querySelector('[data-key="' + key + '"]');
    if (el && f) { el.value = f.default == null ? "" : String(f.default); }
    /* null tells the server to DROP the stored override, not to store an empty string. */
    edits[key] = null;
    markDirty();
  });
  $("presets").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-preset]") : null;
    if (!button) { return; }
    var preset = ((loaded || {}).settings || {}).presets[button.dataset.preset];
    if (!preset) { return; }
    Object.keys(preset.values).forEach(function (k) {
      var el = $("groups").querySelector('[data-key="' + k + '"]');
      if (el) { el.value = preset.values[k]; edits[k] = preset.values[k]; }
    });
    markDirty();
    say($("saveMsg"), preset.note + " Review the fields, add your key, then save.", "info");
  });
  /* ---------- export / import ---------- */
  function exportConfig() {
    return fetch("/v1/admin/config/export", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); });
  }

  $("exportCfg").addEventListener("click", function () {
    exportConfig().then(function (d) {
      var text = JSON.stringify({ settings: d.settings }, null, 2);
      var url = URL.createObjectURL(new Blob([text], { type: "application/json" }));
      var a = document.createElement("a");
      a.href = url;
      a.download = "matrixark-config.json";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function () { URL.revokeObjectURL(url); }, 2000);
      say($("importMsg"), (d.secrets_omitted || []).length
        ? "Exported. " + d.secrets_omitted.join(", ") + " left out — set the key on the target."
        : "Exported.", "ok");
    }).catch(function (e) {
      say($("importMsg"), typeof e === "number" ? failure(e) : "Could not reach the gateway.",
          "err");
    });
  });

  $("copyCfgCurl").addEventListener("click", function () {
    exportConfig().then(function (d) {
      var text = "curl -sX POST " + location.origin + "/v1/admin/config \\\n" +
        "  -H \"Authorization: Bearer $MATRIXARK_ADMIN_KEY\" \\\n" +
        "  -H 'Content-Type: application/json' \\\n  -d '" +
        JSON.stringify({ settings: d.settings }) + "'\n";
      if (navigator.clipboard) { navigator.clipboard.writeText(text); }
      $("copyCfgCurl").textContent = "copied";
      setTimeout(function () { $("copyCfgCurl").textContent = "copy as curl"; }, 1400);
    }).catch(function (e) {
      say($("importMsg"), typeof e === "number" ? failure(e) : "Could not reach the gateway.",
          "err");
    });
  });

  $("importCfg").addEventListener("click", function () {
    var raw = $("importText").value.trim();
    if (!raw) { say($("importMsg"), "Paste an exported configuration first.", "info"); return; }
    var parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      say($("importMsg"), "That is not valid JSON.", "err");
      return;
    }
    var settings = parsed && parsed.settings ? parsed.settings : parsed;
    $("importCfg").disabled = true;
    fetch("/v1/admin/config", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({ settings: settings })
    })
      .then(function (r) { return r.json().then(function (b) { return { ok: r.ok, s: r.status, b: b }; }); })
      .then(function (res) {
        $("importCfg").disabled = false;
        if (!res.ok) {
          say($("importMsg"), res.b.detail || res.b.error || failure(res.s), "err");
          return;
        }
        var n = Object.keys(settings).length;
        var restart = res.b.restart_required || [];
        say($("importMsg"), "Applied " + n + " setting" + (n === 1 ? "" : "s") + "." +
          (restart.length
            ? " " + restart.length + (restart.length === 1 ? " of them needs" : " of them need") +
              " a gateway restart."
            : ""),
          restart.length ? "info" : "ok");
        $("importText").value = "";
        load();
      })
      .catch(function () {
        $("importCfg").disabled = false;
        say($("importMsg"), "Could not reach the gateway.", "err");
      });
  });

  $("settingsForm").addEventListener("submit", save);
  $("test").addEventListener("click", test);
  $("reload").addEventListener("click", function () { edits = {}; load(); });
  $("filter").addEventListener("input", applyFilter);
  $("showAdvanced").addEventListener("change", function () {
    Array.prototype.forEach.call(document.querySelectorAll("details.group"), function (group) {
      var meta = ((loaded || {}).settings || {}).group_meta || {};
      if ((meta[group.dataset.group] || {}).advanced) { group.open = $("showAdvanced").checked; }
    });
    applyFilter();
  });
  $("onlyChanged").addEventListener("change", applyFilter);
  $("onlyEssential").addEventListener("change", function () {
    /* The required settings live in the two model groups, both of which are open by default; but
       ingestion.root is in an advanced one, so narrowing to "must set" has to reveal it. */
    if ($("onlyEssential").checked) { $("showAdvanced").checked = true; }
    applyFilter();
  });
  $("copyScrape").addEventListener("click", function () {
    var text = scrapeConfig();
    if (navigator.clipboard) { navigator.clipboard.writeText(text); }
    $("copyScrape").textContent = "copied";
    setTimeout(function () { $("copyScrape").textContent = "copy scrape config"; }, 1400);
  });
  $("auto").addEventListener("change", function () {
    if (timer) { clearInterval(timer); timer = null; }
    if ($("auto").checked) { timer = setInterval(loadTraffic, 5000); }
  });
  /* Leaving with unsaved edits loses them silently otherwise; on a form this long that is a
     genuine loss of work, not a nuisance. */
  window.addEventListener("beforeunload", function (ev) {
    if (Object.keys(pendingPatch()).length) { ev.preventDefault(); ev.returnValue = ""; }
  });

  try {
    var saved = sessionStorage.getItem("matrixark_admin_key");
    if (saved) { $("key").value = saved; $("remember").checked = true; }
  } catch (e) { /* ignore */ }
  $("scrape").textContent = scrapeConfig();
  markDirty();
  load();
  loadTraffic();
  timer = setInterval(loadTraffic, 5000);
}());
</script>
"""

# ================================================================================================
CATALOG_BODY = """
  <header>
    <h1>Skills &amp; resources</h1>
    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>
%(nav)s
  <p class="lede">What this deployment actually holds: every governed skill and resource visible to a
    scope, and the stored text behind any one of them.</p>

  <div class="summary" id="summary"></div>

  <section>
    <h2>Access <span class="aux"><label class="check"><input type="checkbox" id="remember"> remember for this browser tab</label></span></h2>
    <label for="key">API key</label>
    <input id="key" type="password" placeholder="Key carrying skill:read / resource:read" autocomplete="off" spellcheck="false">
    <div class="hint">These are ordinary scoped reads, not admin actions: a key with
      <span class="mono">skill:read</span> and <span class="mono">resource:read</span> is enough. The
      tenant is pinned from the key, so a key can never list another tenant’s catalog.</div>
  </section>

  <section>
    <h2>Scope</h2>
    <div class="grid2">
      <div>
        <label for="user">User</label>
        <input id="user" type="text" placeholder="leave blank for the whole tenant" spellcheck="false">
        <label for="agent">Agent</label>
        <input id="agent" type="text" placeholder="optional" spellcheck="false">
      </div>
      <div>
        <label for="session">Session</label>
        <input id="session" type="text" placeholder="optional" spellcheck="false">
        <label for="limit">Limit</label>
        <input id="limit" type="text" value="100" spellcheck="false">
      </div>
    </div>
    <div class="grid2">
      <div>
        <label for="filter">Filter</label>
        <input id="filter" type="text" placeholder="name, URI, trigger or type" spellcheck="false">
      </div>
      <div>
        <label for="rtype">Resource type</label>
        <input id="rtype" type="text" placeholder="md, json, pdf — blank for all" spellcheck="false">
      </div>
    </div>
    <div class="actions">
      <button id="refresh" type="button">List catalog</button>
      <label class="check"><input type="checkbox" id="disabled"> include disabled skills</label>
    </div>
    <div id="listMsg" role="status" aria-live="polite"></div>
  </section>

  <section>
    <h2>Skills</h2>
    <div id="skills"><div class="empty">Not loaded.</div></div>
  </section>

  <section>
    <h2>Resources</h2>
    <div id="resources"><div class="empty">Not loaded.</div></div>
  </section>

  <section>
    <h2>Stored text <span class="aux" id="contentTitle"></span></h2>
    <p class="hint" style="margin-top:0">Select a row above to read what was actually stored — the
      chunks retrieval will draw from, not the source file on disk.</p>
    <pre id="content">Nothing selected.</pre>
  </section>
"""

CATALOG_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };

  function auth() {
    var k = $("key").value.trim();
    return k ? { Authorization: "Bearer " + k } : {};
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function say(el, text, cls) {
    el.innerHTML = text ? '<div class="msg ' + (cls || "info") + '">' + esc(text) + "</div>" : "";
  }
  function conn(state, text) {
    $("dot").className = "dot " + state;
    $("conn").textContent = text;
  }
  function when(ms) { return ms ? new Date(ms).toLocaleString() : "—"; }

  function query() {
    var parts = [];
    ["user", "agent", "session"].forEach(function (f) {
      var v = $(f).value.trim();
      if (v) { parts.push(f + "_id=" + encodeURIComponent(v)); }
    });
    var limit = parseInt($("limit").value, 10);
    parts.push("limit=" + (limit > 0 ? Math.min(limit, 500) : 100));
    return parts;
  }

  function renderSummary(skills, resources) {
    var chunks = 0, tokens = 0;
    resources.forEach(function (r) {
      chunks += r.chunk_count || 0;
      tokens += r.token_estimate || 0;
    });
    var cards = [
      { n: skills.length, l: skills.length === 1 ? "skill" : "skills" },
      { n: resources.length, l: resources.length === 1 ? "resource" : "resources" },
      { n: chunks, l: "stored chunks" },
      { n: tokens ? tokens.toLocaleString() : "—", l: "estimated tokens" }
    ];
    $("summary").innerHTML = cards.map(function (c) {
      return '<div class="stat"><b>' + esc(c.n) + "</b><span>" + esc(c.l) + "</span></div>";
    }).join("");
  }

  function skillsHtml(rows) {
    if (!rows.length) { return '<div class="empty">No skills in this scope.</div>'; }
    return "<table><thead><tr><th>Name</th><th>Status</th><th>Triggers</th><th>Version</th>" +
      "<th>Updated</th><th></th></tr></thead><tbody>" + rows.map(function (s) {
        var triggers = (s.triggers || []).slice(0, 4).map(function (t) {
          return '<span class="tag">' + esc(t) + "</span>";
        }).join("");
        var disabled = s.status === "disabled";
        var toggle = '<button type="button" class="' + (disabled ? "ghost" : "danger") +
          '" data-toggle="' + esc(s.skill_hash) + '" data-to="' +
          (disabled ? "active" : "disabled") + '">' + (disabled ? "Enable" : "Disable") + "</button>";
        return '<tr class="rowlink" data-hash="' + esc(s.skill_hash) + '" data-name="' +
          esc(s.name || s.raw_uri) + '"><td><b>' + esc(s.name || "—") + "</b><br>" +
          '<span class="hint" style="margin:0">' + esc((s.description || "").slice(0, 110)) +
          "</span></td><td>" + '<span class="pill ' +
          (disabled ? "queued" : "completed") + '">' + esc(s.status) + "</span></td><td>" +
          (triggers || "—") + "</td><td>" + esc(s.version) + "</td><td>" + esc(when(s.updated_at_ms)) +
          "</td><td>" + toggle + "</td></tr>";
      }).join("") + "</tbody></table>";
  }

  /* A disabled skill stops competing for pack slots with the one that replaced it. Retiring it
     from here is the difference between a catalog that only grows and one a customer curates. */
  function toggleSkill(hash, to) {
    say($("listMsg"), "");
    fetch("/v1/skills/update", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({ skill_hash: parseInt(hash, 10), status: to })
    })
      .then(function (r) {
        return r.json().catch(function () { return {}; }).then(function (b) {
          return { ok: r.ok, status: r.status, body: b };
        });
      })
      .then(function (res) {
        if (!res.ok) {
          say($("listMsg"), res.status === 403
            ? "This key cannot change skills. It needs skill:manage."
            : (res.body.detail || res.body.error || "Could not update the skill."), "err");
          return;
        }
        /* load() clears the message area, so refresh first and confirm after -- otherwise the
           only feedback for the action is the row quietly changing state. */
        load(true);
        say($("listMsg"), "Skill " + (to === "disabled" ? "disabled" : "enabled") + ".", "ok");
      })
      .catch(function () { say($("listMsg"), "Could not reach the gateway.", "err"); });
  }

  function resourcesHtml(rows) {
    if (!rows.length) { return '<div class="empty">No resources in this scope.</div>'; }
    return "<table><thead><tr><th>Resource</th><th>Type</th><th>Chunks</th><th>Tokens</th>" +
      "<th>Updated</th></tr></thead><tbody>" + rows.map(function (r) {
        var uri = r.requested_raw_uri || r.raw_uri || "";
        var warn = (r.parse_warning_count || 0)
          ? ' <span class="tag">' + r.parse_warning_count + " parse warnings</span>" : "";
        return '<tr class="rowlink" data-hash="' + esc(r.resource_hash) + '" data-name="' +
          esc(uri) + '"><td><span class="mono">' + esc(uri) + "</span>" + warn + "</td><td>" +
          esc(r.resource_type || "—") + "</td><td class='num'>" + (r.chunk_count || 0) +
          "</td><td class='num'>" + (r.token_estimate || 0) + "</td><td>" +
          esc(when(r.updated_at_ms)) + "</td></tr>";
      }).join("") + "</tbody></table>";
  }

  function matches(row, text) {
    if (!text) { return true; }
    return JSON.stringify(row).toLowerCase().indexOf(text) >= 0;
  }

  function load(keepMessage) {
    var qs = query().join("&");
    var skillQs = qs + ($("disabled").checked ? "&include_disabled=1" : "");
    var rtype = $("rtype").value.trim();
    if (rtype) { qs += "&resource_type=" + encodeURIComponent(rtype); }
    if (!keepMessage) { say($("listMsg"), ""); }
    Promise.all([
      fetch("/v1/skills?" + skillQs, { headers: auth() }),
      fetch("/v1/resources?" + qs, { headers: auth() })
    ]).then(function (responses) {
      var bad = responses.filter(function (r) { return !r.ok; })[0];
      if (bad) { return Promise.reject(bad.status); }
      return Promise.all(responses.map(function (r) { return r.json(); }));
    }).then(function (bodies) {
      conn("live", "connected");
      var text = $("filter").value.trim().toLowerCase();
      var skills = (bodies[0].skills || []).filter(function (s) { return matches(s, text); });
      var resources = (bodies[1].resources || []).filter(function (r) { return matches(r, text); });
      renderSummary(skills, resources);
      $("skills").innerHTML = skillsHtml(skills);
      $("resources").innerHTML = resourcesHtml(resources);
    }).catch(function (e) {
      if (e === 401 || e === 403) {
        conn("live", "connected");
        say($("listMsg"), "This key cannot read the catalog. It needs skill:read and resource:read.",
            "err");
      } else {
        conn("down", "gateway unreachable");
        say($("listMsg"), "Could not reach the gateway.", "err");
      }
    });
  }

  function openContent(hash, name) {
    $("contentTitle").textContent = name;
    $("content").textContent = "Loading…";
    var body = { resource_hash: parseInt(hash, 10), chunk_limit: 40, max_chars: 40000, scope: {} };
    ["user", "agent", "session"].forEach(function (f) {
      var v = $(f).value.trim();
      if (v) { body.scope[f + "_id"] = v; }
    });
    fetch("/v1/resource/content", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify(body)
    })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        var chunks = d.chunks || [];
        $("content").textContent = chunks.length
          ? chunks.map(function (c) { return c.text || c.chunk_text || ""; }).join("\n\n")
          : (d.text || "No stored text returned.");
      })
      .catch(function (e) {
        $("content").textContent = e === 401 || e === 403
          ? "This key cannot read resource content (needs context:retrieve)."
          : "Could not read the stored text.";
      });
  }

  ["skills", "resources"].forEach(function (id) {
    $(id).addEventListener("click", function (ev) {
      var toggle = ev.target.closest ? ev.target.closest("[data-toggle]") : null;
      if (toggle) {
        /* The button sits inside a clickable row; without this the row handler also fires and
           the stored text panel swaps under the operator as they retire a skill. */
        ev.stopPropagation();
        toggleSkill(toggle.dataset.toggle, toggle.dataset.to);
        return;
      }
      var row = ev.target.closest ? ev.target.closest("tr.rowlink") : null;
      if (row) { openContent(row.dataset.hash, row.dataset.name); }
    });
  });
  $("refresh").addEventListener("click", function () { load(); });
  $("filter").addEventListener("input", function () { load(); });
  $("rtype").addEventListener("change", function () { load(); });
  $("disabled").addEventListener("change", function () { load(); });
  $("key").addEventListener("change", function () {
    if ($("remember").checked) {
      try { sessionStorage.setItem("matrixark_admin_key", $("key").value); } catch (e) { /* ignore */ }
    }
    load();
  });
  $("remember").addEventListener("change", function () {
    try {
      if ($("remember").checked) { sessionStorage.setItem("matrixark_admin_key", $("key").value); }
      else { sessionStorage.removeItem("matrixark_admin_key"); }
    } catch (e) { /* ignore */ }
  });

  try {
    var saved = sessionStorage.getItem("matrixark_admin_key");
    if (saved) { $("key").value = saved; $("remember").checked = true; }
  } catch (e) { /* ignore */ }
  conn("live", "ready");
  load();
}());
</script>
"""


# ================================================================================================
OVERVIEW_BODY = """
  <header>
    <h1>MatrixArk</h1>
    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>
%(nav)s
  <p class="lede">Where this deployment stands. Every item below fails quietly on its own — an
    unconfigured model still answers 200, an unset ingestion root only surfaces when an import is
    refused — so they are worth reading once even when nothing looks wrong.</p>

  <div class="summary" id="summary"></div>

  <section>
    <h2>Access <span class="aux"><label class="check"><input type="checkbox" id="remember"> remember for this browser tab</label></span></h2>
    <label for="key">Admin API key</label>
    <input id="key" type="password" placeholder="Key carrying an admin scope" autocomplete="off" spellcheck="false">
    <div class="hint">Reading this page needs nothing; the checklist needs an admin-scoped key.</div>
  </section>

  <section id="importsSection" hidden>
    <h2>Import in progress <span class="aux" id="importsAux"></span></h2>
    <div id="imports"></div>
  </section>

  <section>
    <h2>Setup <span class="aux"><label class="check"><input type="checkbox" id="autoOverview" checked> auto-refresh</label>
      <span id="progressText" style="margin-left:12px"></span></span></h2>
    <div class="progress"><i id="progressBar" style="width:0%%"></i></div>
    <div id="checks"><div class="empty">Enter an admin key to check this deployment.</div></div>
  </section>

  <section>
    <h2>Diagnostics <span class="aux"><button class="link" id="copyDiag" type="button">copy</button>
      <button class="link" id="downloadDiag" type="button">download</button></span></h2>
    <p class="hint" style="margin-top:0">Everything on this page plus the effective configuration
      and the current counters, as one JSON document — the thing to attach to a support request.
      Secrets are never in it: the configuration half reports whether a key resolves, never its
      value.</p>
    <div id="diagMsg" role="status" aria-live="polite"></div>
  </section>

  <section>
    <h2>Go to</h2>
    <div class="tiles">
      <a class="tile" href="/v1/admin/setup"><b>Setup &amp; metrics</b><span>Choose the models, store the provider key, probe the endpoints, watch the traffic.</span></a>
      <a class="tile" href="/v1/admin/catalog"><b>Skills &amp; resources</b><span>What this deployment holds, and the stored text behind any entry.</span></a>
      <a class="tile" href="/v1/admin/explore"><b>Explore</b><span>Run a real retrieve, read a memory, ingest a note.</span></a>
      <a class="tile" href="/v1/admin/ingestion"><b>Ingestion</b><span>Import a directory of documents and watch the job.</span></a>
      <a class="tile" href="/v1/admin/portal"><b>API keys</b><span>Create, rotate and revoke keys; per-key usage.</span></a>
    </div>
  </section>
"""

OVERVIEW_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  function auth() {
    var k = $("key").value.trim();
    return k ? { Authorization: "Bearer " + k } : {};
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function conn(state, text) {
    $("dot").className = "dot " + state;
    $("conn").textContent = text;
  }

  var MARK = { ok: "✓", warn: "!", todo: "·" };

  function secs(s) {
    if (!s) { return "—"; }
    if (s < 60) { return Math.round(s) + "s"; }
    if (s < 3600) { return Math.floor(s / 60) + "m " + Math.round(s % 60) + "s"; }
    return Math.floor(s / 3600) + "h " + Math.round((s % 3600) / 60) + "m";
  }

  /* An import running elsewhere is a state a customer would otherwise only find by opening the
     Ingestion page, which is not where anyone starts. */
  function renderImports(imports) {
    var active = (imports && imports.active) || [];
    $("importsSection").hidden = active.length === 0;
    if (!active.length) { return; }
    $("importsAux").textContent = imports.documents_remaining + " remaining" +
      (imports.eta_s ? " · eta " + secs(imports.eta_s) : "");
    $("imports").innerHTML = active.map(function (job) {
      var processed = (job.done || 0) + (job.failed || 0);
      var pct = job.total ? Math.round((processed / job.total) * 100) : 0;
      return '<div class="importrow"><div class="importhead"><span class="mono">' +
        esc(job.job_id) + '</span><span class="pill ' + esc(job.state) + '">' + esc(job.state) +
        "</span><span class='importmeta'>" + pct + "% of " + job.total +
        (job.failed ? " · " + job.failed + " failed" : "") + "</span></div>" +
        '<div class="bar"><i style="width:' + pct + '%"></i></div>' +
        (job.current ? '<div class="importcur">' + esc(job.current) + "</div>" : "") + "</div>";
    }).join("") + '<div class="hint"><a href="/v1/admin/ingestion">Open Ingestion</a> to watch it ' +
      "in detail or stop it.</div>";
  }

  function render(d) {
    var checks = d.checks || [];
    $("checks").innerHTML = checks.map(function (c) {
      var go = c.href
        ? '<a class="go" href="' + esc(c.href) + '">' + esc(c.action || "Open") + " →</a>" : "";
      /* The steps for the state it is in. Open for a `todo` -- something that has to be done
         before the deployment works should not be behind a click -- and collapsed for a warning,
         which is advice. */
      var how = "";
      if (c.how && c.how.length) {
        how = "<details class='how'" + (c.status === "todo" ? " open" : "") +
          "><summary>how</summary><ol>" +
          c.how.map(function (step) { return "<li>" + esc(step) + "</li>"; }).join("") +
          "</ol></details>";
      }
      return '<div class="checkrow"><span class="mark ' + esc(c.status) + '">' +
        (MARK[c.status] || "") + '</span><span class="body"><b>' + esc(c.title) +
        "</b><span>" + esc(c.detail) + "</span>" + how + "</span>" + go + "</div>";
    }).join("") || '<div class="empty">Nothing to check.</div>';

    var pct = d.total ? Math.round((d.done / d.total) * 100) : 0;
    $("progressBar").style.width = pct + "%";
    $("progressText").textContent = d.done + " of " + d.total + " ready";

    renderImports(d.imports);
    /* Keep up with an import while one is running, and go back to idle pacing when it ends. */
    pace(!!((d.imports || {}).active || []).length);

    var counts = d.counts || {}, traffic = d.traffic || {};
    var cards = [
      { n: counts.skills == null ? "—" : counts.skills, l: "skills" },
      { n: counts.resources == null ? "—" : counts.resources, l: "resources" },
      { n: counts.users == null ? "—" : counts.users, l: "subjects with memory" },
      { n: traffic.total_requests || 0, l: "requests served",
        c: traffic.total_errors ? "warn" : "" }
    ];
    $("summary").innerHTML = cards.map(function (c) {
      return '<div class="stat ' + (c.c || "") + '"><b>' + esc(c.n) + "</b><span>" +
        esc(c.l) + "</span></div>";
    }).join("");
  }

  /* ---------- diagnostics ---------- */
  var lastReport = null;

  function compactConfig(config) {
    if (!config) { return null; }
    var settings = config.settings || {};
    var groups = settings.groups || {}, out = {};
    Object.keys(groups).forEach(function (g) {
      groups[g].forEach(function (f) {
        if (f.source === "default" && !f.configured) { return; }
        out[f.key] = {
          env: f.env, source: f.source, applies: f.applies,
          value: f.kind === "secret" ? (f.configured ? "<configured>" : "") : f.value
        };
      });
    });
    return {
      extraction: config.extraction,
      embedding: config.embedding,
      warnings: config.warnings,
      config_file: settings.config_file,
      updated_at: settings.updated_at,
      inventory: settings.inventory,
      /* Only what differs from the built-in default, which is the whole question when someone
         asks why this deployment behaves differently from another one. */
      non_default_settings: out
    };
  }

  function bundle() {
    return Promise.all([
      fetch("/v1/admin/config", { headers: auth() })
        .then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
      fetch("/v1/metrics").then(function (r) { return r.ok ? r.text() : ""; })
        .catch(function () { return ""; })
    ]).then(function (parts) {
      return JSON.stringify({
        collected_at: new Date().toISOString(),
        origin: location.origin,
        overview: lastReport,
        config: compactConfig(parts[0]),
        metrics: parts[1]
      }, null, 2);
    });
  }

  function withBundle(fn) {
    if (!lastReport) {
      say($("diagMsg"), "Enter an admin key first — the bundle needs the checklist.", "info");
      return;
    }
    bundle().then(fn).catch(function () {
      say($("diagMsg"), "Could not assemble the bundle.", "err");
    });
  }

  function say(el, text, cls) {
    el.innerHTML = text ? '<div class="msg ' + (cls || "info") + '">' + esc(text) + "</div>" : "";
  }

  function load() {
    fetch("/v1/admin/overview", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) { conn("live", "connected"); lastReport = d; render(d); })
      .catch(function (e) {
        if (e === 401 || e === 403) {
          conn("live", "connected");
          $("checks").innerHTML = '<div class="empty">Enter an admin-scoped key above to check ' +
            "this deployment.</div>";
        } else {
          conn("down", "gateway unreachable");
          $("checks").innerHTML = '<div class="msg err">Could not reach the gateway.</div>';
        }
      });
  }

  /* Someone fixing the checklist has this page open in one tab and the setup page in another;
     without this they fix a thing and the list keeps telling them it is broken. Each refresh costs
     three listings on the backend, so it does not run for a tab nobody is looking at -- an idle
     background tab was otherwise a standing load on the store. */
  var IDLE_MS = 30000, ACTIVE_MS = 4000, overviewMs = 0, overviewTimer = null;

  function pace(active) {
    var want = active ? ACTIVE_MS : IDLE_MS;
    if (want === overviewMs && overviewTimer) { return; }
    if (overviewTimer) { clearInterval(overviewTimer); }
    overviewMs = want;
    overviewTimer = setInterval(function () {
      if (document.hidden) { return; }
      if ($("autoOverview").checked && $("key").value.trim()) { load(); }
    }, want);
  }
  pace(false);
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden && $("autoOverview").checked && $("key").value.trim()) { load(); }
  });
  $("autoOverview").addEventListener("change", function () {
    if ($("autoOverview").checked) { load(); }
  });
  window.addEventListener("pagehide", function () { clearInterval(overviewTimer); });

  $("copyDiag").addEventListener("click", function () {
    withBundle(function (text) {
      if (navigator.clipboard) { navigator.clipboard.writeText(text); }
      say($("diagMsg"), "Copied " + text.length + " characters to the clipboard.", "ok");
    });
  });
  $("downloadDiag").addEventListener("click", function () {
    withBundle(function (text) {
      var url = URL.createObjectURL(new Blob([text], { type: "application/json" }));
      var a = document.createElement("a");
      a.href = url;
      a.download = "matrixark-diagnostics-" + new Date().toISOString().slice(0, 19)
        .replace(/[:T]/g, "-") + ".json";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function () { URL.revokeObjectURL(url); }, 2000);
      say($("diagMsg"), "Downloaded.", "ok");
    });
  });
  $("key").addEventListener("change", function () {
    if ($("remember").checked) {
      try { sessionStorage.setItem("matrixark_admin_key", $("key").value); } catch (e) { /* ignore */ }
    }
    load();
  });
  $("remember").addEventListener("change", function () {
    try {
      if ($("remember").checked) { sessionStorage.setItem("matrixark_admin_key", $("key").value); }
      else { sessionStorage.removeItem("matrixark_admin_key"); }
    } catch (e) { /* ignore */ }
  });
  try {
    var saved = sessionStorage.getItem("matrixark_admin_key");
    if (saved) { $("key").value = saved; $("remember").checked = true; }
  } catch (e) { /* ignore */ }
  load();
}());
</script>
"""

# ================================================================================================
EXPLORE_BODY = """
  <header>
    <h1>Explore</h1>
    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>
%(nav)s
  <p class="lede">Run the pipeline against your own data: ingest a note, ask a question, read what
    comes back. A retrieve that returns the right thing is the only check that covers ingestion,
    extraction, encoding and ranking at once.</p>

  <section>
    <h2>Access</h2>
    <div class="grid2">
      <div>
        <label for="key">API key</label>
        <input id="key" type="password" placeholder="Key with context:ingest and context:retrieve" autocomplete="off" spellcheck="false">
      </div>
      <div>
        <label for="user">User</label>
        <input id="user" type="text" value="default" spellcheck="false">
      </div>
    </div>
    <div class="grid2">
      <div>
        <label for="agent">Agent</label>
        <input id="agent" type="text" placeholder="optional" spellcheck="false">
      </div>
      <div>
        <label for="session">Session</label>
        <input id="session" type="text" placeholder="optional" spellcheck="false">
      </div>
    </div>
    <div class="hint">The tenant is pinned from the key. These fields choose the scope WITHIN it.</div>
  </section>

  <div class="tabs" role="tablist">
    <button type="button" role="tab" id="tab-ask" aria-controls="pane-ask" data-pane="ask" aria-selected="true">Ask</button>
    <button type="button" role="tab" id="tab-add" aria-controls="pane-add" data-pane="add" aria-selected="false">Add a memory</button>
    <button type="button" role="tab" id="tab-browse" aria-controls="pane-browse" data-pane="browse" aria-selected="false">Browse</button>
  </div>

  <section class="pane" role="tabpanel" aria-labelledby="tab-ask" id="pane-ask">
    <h2>Ask <span class="aux" id="askStats"></span></h2>
    <label for="query">Question</label>
    <input id="query" type="text" placeholder="What did we decide about the checkout coupon?" spellcheck="false">
    <div class="grid2">
      <div>
        <label for="budget">Token budget</label>
        <input id="budget" type="text" value="2048" spellcheck="false">
      </div>
      <div>
        <label for="topk">Max items</label>
        <input id="topk" type="text" value="10" spellcheck="false">
      </div>
    </div>
    <div class="actions"><button id="ask" type="button">Retrieve</button>
      <span class="hint" style="margin:0">Calls <span class="mono">POST /v1/retrieve</span> exactly as an agent would.</span></div>
    <div id="askMsg" role="status" aria-live="polite"></div>
    <div id="pack"><div class="empty">Nothing retrieved yet.</div></div>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-add" id="pane-add" hidden>
    <h2>Add a memory</h2>
    <p class="hint" style="margin-top:0">Writes one turn through <span class="mono">POST /v1/ingest</span>,
      the same path an agent hook uses. Useful for checking that extraction and encoding are
      working before you point a real workload at the deployment.</p>
    <label for="note">Text</label>
    <textarea id="note" placeholder="The team agreed to keep the coupon stacking rule for ACME until Q4."></textarea>
    <div class="actions"><button id="add" type="button">Ingest</button>
      <label class="check"><input type="checkbox" id="finalize" checked> extract immediately</label></div>
    <div id="addMsg" role="status" aria-live="polite"></div>
    <div class="hint">Without “extract immediately” the write is acknowledged and extraction runs in
      the background, which is how a production ingest behaves — it just means a retrieve issued a
      moment later may not see it yet.</div>

    <div class="panel-head compact section-gap"><h2 style="margin-top:22px">Upload a document</h2></div>
    <p class="hint" style="margin-top:0">Streams the file through
      <span class="mono">POST /v1/ingest_file</span> to the blob tier and ingests it in one call —
      the same path the SDK uses. For a whole directory, use
      <a href="/v1/admin/ingestion">Ingestion</a> instead.</p>
    <div class="grid2">
      <div>
        <label for="file">Files</label>
        <input id="file" type="file" multiple>
        <div class="hint">Uploaded one at a time so a failure stops at the file it failed on.</div>
      </div>
      <div>
        <label for="kind">Store as</label>
        <select id="kind"><option value="skill">Skill</option><option value="resource">Resource</option></select>
        <label for="sharing">Visibility</label>
        <select id="sharing">
          <option value="">server default</option>
          <option value="private_user">private to this user</option>
          <option value="tenant_shared">shared across the tenant</option>
          <option value="global_shared">shared globally</option>
        </select>
      </div>
    </div>
    <div class="actions"><button id="upload" type="button">Upload</button>
      <label class="check"><input type="checkbox" id="wait" checked> wait for extraction</label>
      <button id="clearDone" class="link" type="button">clear finished</button></div>
    <div id="uploadMsg" role="status" aria-live="polite"></div>
    <div id="uploadQueue"></div>
    <div class="hint" id="retryNote" hidden>Retry re-sends the file from this browser, so it works
      only while this tab stays open — the bytes never existed on the server. A bulk import from a
      server directory is retryable after a restart; see <a href="/v1/admin/ingestion">Ingestion</a>.</div>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-browse" id="pane-browse" hidden>
    <h2>Browse <span class="aux"><button class="link" id="refresh" type="button">refresh</button></span></h2>
    <div id="users" class="metric-list"></div>
    <div id="browseMsg" role="status" aria-live="polite"></div>
    <div id="memories"><div class="empty">Not loaded.</div></div>
    <div class="panel-head compact"><h2 style="margin-top:18px">Memory detail</h2></div>
    <pre id="detail">Select a memory to see its stored record and change history.</pre>
  </section>
"""

EXPLORE_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  function auth() {
    var k = $("key").value.trim();
    return k ? { Authorization: "Bearer " + k } : {};
  }
  function jsonAuth() {
    return Object.assign({ "Content-Type": "application/json" }, auth());
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function say(el, text, cls) {
    el.innerHTML = text ? '<div class="msg ' + (cls || "info") + '">' + esc(text) + "</div>" : "";
  }
  function conn(state, text) {
    $("dot").className = "dot " + state;
    $("conn").textContent = text;
  }
  function scope() {
    var s = {};
    ["user", "agent", "session"].forEach(function (f) {
      var v = $(f).value.trim();
      if (v) { s[f + "_id"] = v; }
    });
    return s;
  }
  function scopeQuery() {
    return Object.keys(scope()).map(function (k) {
      return k + "=" + encodeURIComponent(scope()[k]);
    }).join("&");
  }
  function failure(status) {
    if (status === 401) { return "This key was not accepted."; }
    if (status === 403) { return "This key lacks the scope for that call."; }
    if (status === 413) { return "That file is larger than this deployment accepts."; }
    if (status === 429) { return "Rate limited — the edge is throttling this key."; }
    if (status === 502) { return "The gateway could not reach the storage behind it."; }
    if (status === 504) { return "The backend did not answer in time."; }
    return "The gateway answered " + status + ".";
  }
  /* `detail` is a sentence when the server has something specific to say; `error` is a code word
     like "unauthorized", which is the machine's word for it and answers nothing. Prefer the
     sentence, then our own phrasing, and fall back to the code word only when neither exists. */
  function reason(body, status) {
    return (body && body.detail) || failure(status) || (body && body.error) || "";
  }

  /* ---------- tabs ---------- */
  Array.prototype.forEach.call(document.querySelectorAll(".tabs button"), function (button) {
    button.addEventListener("click", function () {
      Array.prototype.forEach.call(document.querySelectorAll(".tabs button"), function (b) {
        b.setAttribute("aria-selected", String(b === button));
      });
      Array.prototype.forEach.call(document.querySelectorAll(".pane"), function (pane) {
        pane.hidden = pane.id !== "pane-" + button.dataset.pane;
      });
      if (button.dataset.pane === "browse") { loadBrowse(); }
    });
  });

  /* ---------- ask ---------- */
  /* The response shape differs between backends and versions; pick the first list of items that
     looks like a pack rather than insisting on one field name, so this keeps working. */
  function packItems(d) {
    var candidates = [d.pack, d.context_pack, d.items, d.results, d.memories, d.refs];
    for (var i = 0; i < candidates.length; i++) {
      if (Array.isArray(candidates[i])) { return candidates[i]; }
    }
    if (d.context_pack && Array.isArray(d.context_pack.items)) { return d.context_pack.items; }
    return [];
  }
  function itemText(it) {
    if (typeof it === "string") { return it; }
    return it.text || it.content || it.memory || it.summary || it.value || JSON.stringify(it);
  }
  function itemMeta(it) {
    if (typeof it === "string") { return ""; }
    var bits = [];
    ["layer", "kind", "record_type", "source", "session_id"].forEach(function (k) {
      if (it[k]) { bits.push(k + "=" + it[k]); }
    });
    if (typeof it.score === "number") { bits.push("score=" + it.score.toFixed(3)); }
    if (it.id || it.memory_id) { bits.push(String(it.id || it.memory_id)); }
    return bits.join(" · ");
  }

  function ask() {
    var q = $("query").value.trim();
    if (!q) { say($("askMsg"), "Type a question first.", "info"); return; }
    var body = { query: q, scope: scope() };
    var budget = parseInt($("budget").value, 10);
    if (budget > 0) { body.max_budget_tokens = budget; }
    var topk = parseInt($("topk").value, 10);
    if (topk > 0) { body.limit = topk; }
    $("ask").disabled = true;
    say($("askMsg"), "");
    $("pack").innerHTML = '<div class="empty">Retrieving…</div>';
    var started = Date.now();
    fetch("/v1/retrieve", { method: "POST", headers: jsonAuth(), body: JSON.stringify(body) })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        $("ask").disabled = false;
        conn("live", "connected");
        var items = packItems(d);
        var ms = Date.now() - started;
        $("askStats").textContent = items.length + " item" + (items.length === 1 ? "" : "s") +
          (d.tokens ? " · " + d.tokens + " tokens" : "") + " · " + ms + " ms";
        if (!items.length) {
          $("pack").innerHTML = '<div class="empty">Nothing matched. If the store is not empty, ' +
            "the usual cause is an embedding provider that is still deterministic — hash vectors " +
            "do not match on meaning.</div>";
          return;
        }
        $("pack").innerHTML = items.map(function (it) {
          var meta = itemMeta(it);
          return '<div class="packitem">' +
            (meta ? '<div class="meta">' + esc(meta) + "</div>" : "") +
            "<p>" + esc(itemText(it)) + "</p></div>";
        }).join("");
      })
      .catch(function (e) {
        $("ask").disabled = false;
        $("pack").innerHTML = '<div class="empty">Nothing retrieved.</div>';
        say($("askMsg"), typeof e === "number" ? failure(e) : "Could not reach the gateway.", "err");
      });
  }

  /* ---------- add ---------- */
  function add() {
    var text = $("note").value.trim();
    if (!text) { say($("addMsg"), "Type something to ingest.", "info"); return; }
    var body = {
      scope: scope(),
      messages: [{ role: "user", content: text }],
      finalize: $("finalize").checked
    };
    $("add").disabled = true;
    fetch("/v1/ingest", { method: "POST", headers: jsonAuth(), body: JSON.stringify(body) })
      .then(function (r) {
        return r.json().catch(function () { return {}; }).then(function (b) {
          return { ok: r.ok, status: r.status, body: b };
        });
      })
      .then(function (res) {
        $("add").disabled = false;
        if (!res.ok) { say($("addMsg"), reason(res.body, res.status), "err"); return; }
        say($("addMsg"), $("finalize").checked
          ? "Ingested and extracted. Ask a question about it on the Ask tab."
          : "Accepted. Extraction runs in the background, so a retrieve issued right now may not "
            + "see it yet.", "ok");
        $("note").value = "";
      })
      .catch(function () {
        $("add").disabled = false;
        say($("addMsg"), "Could not reach the gateway.", "err");
      });
  }

  /* ---------- browse ---------- */
  function loadBrowse() {
    say($("browseMsg"), "");
    fetch("/v1/users?" + scopeQuery(), { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        var rows = d.users || d.results || [];
        $("users").innerHTML = rows.length
          ? rows.map(function (u) {
              var name = typeof u === "string" ? u : (u.user_id || u.agent_id || u.run_id || u.id);
              var n = typeof u === "object" && u.count != null ? u.count : "";
              return "<div><span>" + esc(name) + "</span><b>" + esc(n) + "</b></div>";
            }).join("")
          : '<div class="empty">No subjects hold memories in this scope yet.</div>';
      })
      .catch(function () { $("users").innerHTML = ""; });

    $("memories").innerHTML = '<div class="empty">Loading…</div>';
    fetch("/v1/memories?" + scopeQuery() + "&limit=50", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        conn("live", "connected");
        var rows = d.memories || d.results || d.records || [];
        if (!rows.length) {
          $("memories").innerHTML = '<div class="empty">No memories in this scope.</div>';
          return;
        }
        $("memories").innerHTML = "<table><thead><tr><th>Memory</th><th>Kind</th><th>Updated</th>" +
          "</tr></thead><tbody>" + rows.map(function (m) {
            var id = m.id || m.memory_id || "";
            var when = m.updated_at_ms || m.created_at_ms;
            return '<tr class="rowlink" data-id="' + esc(id) + '"><td>' +
              esc(String(m.memory || m.text || m.content || "").slice(0, 160)) + "</td><td>" +
              esc(m.record_type || m.kind || "—") + "</td><td>" +
              esc(when ? new Date(when).toLocaleString() : "—") + "</td></tr>";
          }).join("") + "</tbody></table>";
      })
      .catch(function (e) {
        $("memories").innerHTML = '<div class="empty">Not loaded.</div>';
        say($("browseMsg"), typeof e === "number" ? failure(e) : "Could not reach the gateway.",
            "err");
      });
  }

  $("memories").addEventListener("click", function (ev) {
    var row = ev.target.closest ? ev.target.closest("tr.rowlink") : null;
    if (!row || !row.dataset.id) { return; }
    $("detail").textContent = "Loading…";
    var id = encodeURIComponent(row.dataset.id);
    Promise.all([
      fetch("/v1/memory/" + id, { headers: auth() }).then(function (r) { return r.ok ? r.json() : null; }),
      fetch("/v1/memory/" + id + "/history", { headers: auth() }).then(function (r) { return r.ok ? r.json() : null; })
    ]).then(function (parts) {
      $("detail").textContent = JSON.stringify({ memory: parts[0], history: parts[1] }, null, 2);
    }).catch(function () {
      $("detail").textContent = "Could not read that memory.";
    });
  });

  /* ---------- upload ---------- */
  /* A queue of {file, state, sent, detail}. The File object is kept so a failure can be retried
     without asking for the file again -- the bytes came from this browser and were never on the
     server, so nothing else could resend them. */
  var queue = [], uploading = false, nextId = 1;

  function fmtBytes(n) {
    if (!n) { return "0 B"; }
    var units = ["B", "KB", "MB", "GB"], i = Math.floor(Math.log(n) / Math.log(1024));
    return (n / Math.pow(1024, i)).toFixed(i ? 1 : 0) + " " + units[i];
  }

  function renderQueue() {
    $("retryNote").hidden = !queue.some(function (item) { return item.state === "failed"; });
    $("uploadQueue").innerHTML = queue.map(function (item) {
      var pct = item.file.size ? Math.round((item.sent / item.file.size) * 100) : 0;
      if (item.state === "done") { pct = 100; }
      var label = { queued: "waiting", uploading: pct + "%", extracting: "extracting",
                    done: "stored", failed: "failed" }[item.state] || item.state;
      return '<div class="upl ' + item.state + '" data-item="' + item.id + '">' +
        '<div class="upl-head"><span class="upl-name">' + esc(item.file.name) + "</span>" +
        '<span class="upl-size">' + fmtBytes(item.file.size) + " · " + esc(label) + "</span></div>" +
        '<div class="upl-bar"><i style="width:' + pct + '%"></i></div>' +
        (item.detail ? '<div class="upl-detail">' + esc(item.detail) + "</div>" : "") +
        (item.state === "failed"
          ? '<div class="upl-actions"><button type="button" class="ghost" data-retry="' +
            item.id + '">Retry</button></div>'
          : "") +
        /* A large upload with no way out is a page you have to reload to escape. Cancelling
           leaves the item as a failure with a Retry, which is what it is. */
        (item.state === "uploading" || item.state === "extracting"
          ? '<div class="upl-actions"><button type="button" class="danger" data-cancel="' +
            item.id + '">Cancel</button></div>'
          : "") +
        (item.state === "queued"
          ? '<div class="upl-actions"><button type="button" class="link" data-drop="' +
            item.id + '">remove from queue</button></div>'
          : "") + "</div>";
    }).join("");
  }

  function sendOne(item) {
    return new Promise(function (resolve) {
      var request = new XMLHttpRequest();
      item.request = request;
      request.open("POST", "/v1/ingest_file", true);
      var headers = Object.assign({
        "Content-Type": item.file.type || "application/octet-stream",
        "X-Filename": item.file.name,
        "X-Resource-Kind": item.kind,
        /* X-Scope is a JSON scope OBJECT, not a label -- the gateway parses it. */
        "X-Scope": JSON.stringify(item.scope)
      }, auth());
      if (item.wait) { headers["X-Wait"] = "1"; }
      if (item.sharing) { headers["X-Sharing-Scope"] = item.sharing; }
      Object.keys(headers).forEach(function (name) {
        request.setRequestHeader(name, headers[name]);
      });

      /* fetch() reports nothing until the request finishes, so a large upload was a disabled
         button and a sentence. This is the only upload-progress event the platform has. */
      request.upload.onprogress = function (ev) {
        if (!ev.lengthComputable) { return; }
        item.sent = ev.loaded;
        /* Bytes are on the wire; what happens after the last byte is the server extracting, and
           saying so is the difference between "stuck at 100%" and "nearly there". */
        item.state = ev.loaded >= ev.total ? "extracting" : "uploading";
        renderQueue();
      };
      request.onload = function () {
        var body = {};
        try { body = JSON.parse(request.responseText || "{}"); } catch (e) { body = {}; }
        if (request.status >= 200 && request.status < 300) {
          item.state = "done";
          item.sent = item.file.size;
          item.detail = "Stored as a " + item.kind +
            (item.wait ? "." : ". Extraction is running in the background.");
        } else {
          item.state = "failed";
          item.detail = reason(body, request.status);
        }
        renderQueue();
        resolve();
      };
      request.onerror = function () {
        item.state = "failed";
        item.detail = "Could not reach the gateway.";
        renderQueue();
        resolve();
      };
      request.onabort = function () {
        item.state = "failed";
        item.detail = "Cancelled before it finished.";
        renderQueue();
        resolve();
      };
      item.state = "uploading";
      item.sent = 0;
      renderQueue();
      request.send(item.file);
    });
  }

  /* One at a time: a failure then stops at the file it failed on rather than being one line in a
     pile of parallel errors, and the progress bar means something. */
  function drain() {
    if (uploading) { return; }
    var next = queue.filter(function (item) { return item.state === "queued"; })[0];
    if (!next) {
      uploading = false;
      $("upload").disabled = false;
      var failed = queue.filter(function (i) { return i.state === "failed"; }).length;
      var done = queue.filter(function (i) { return i.state === "done"; }).length;
      if (done || failed) {
        say($("uploadMsg"), done + " stored" + (failed ? ", " + failed + " failed" : "") +
          (done ? ". They should appear on the catalog page." : ""), failed ? "err" : "ok");
      }
      return;
    }
    uploading = true;
    $("upload").disabled = true;
    sendOne(next).then(function () { uploading = false; drain(); });
  }

  function enqueue(files) {
    Array.prototype.forEach.call(files, function (file) {
      queue.push({
        id: nextId++, file: file, state: "queued", sent: 0, detail: "",
        kind: $("kind").value, sharing: $("sharing").value, wait: $("wait").checked,
        scope: scope()
      });
    });
    renderQueue();
    drain();
  }

  function upload() {
    var input = $("file");
    if (!input.files || !input.files.length) {
      say($("uploadMsg"), "Choose a file first.", "info");
      return;
    }
    say($("uploadMsg"), "");
    enqueue(input.files);
    input.value = "";
  }

  function itemById(id) {
    return queue.filter(function (i) { return String(i.id) === String(id); })[0];
  }

  $("uploadQueue").addEventListener("click", function (ev) {
    var target = ev.target.closest ? ev.target : null;
    if (!target) { return; }

    var retry = target.closest("[data-retry]");
    if (retry) {
      var item = itemById(retry.dataset.retry);
      if (!item) { return; }
      item.state = "queued";
      item.detail = "";
      item.sent = 0;
      renderQueue();
      drain();
      return;
    }

    var cancel = target.closest("[data-cancel]");
    if (cancel) {
      var running = itemById(cancel.dataset.cancel);
      if (running && running.request) { running.request.abort(); }
      return;
    }

    var drop = target.closest("[data-drop]");
    if (drop) {
      queue = queue.filter(function (i) { return String(i.id) !== drop.dataset.drop; });
      renderQueue();
    }
  });
  $("clearDone").addEventListener("click", function () {
    queue = queue.filter(function (i) { return i.state !== "done"; });
    renderQueue();
  });
  $("upload").addEventListener("click", upload);
  $("ask").addEventListener("click", ask);
  $("query").addEventListener("keydown", function (ev) { if (ev.key === "Enter") { ask(); } });
  $("add").addEventListener("click", add);
  $("refresh").addEventListener("click", loadBrowse);
  $("key").addEventListener("change", function () {
    try { sessionStorage.setItem("matrixark_admin_key", $("key").value); } catch (e) { /* ignore */ }
  });
  try {
    var saved = sessionStorage.getItem("matrixark_admin_key");
    if (saved) { $("key").value = saved; }
  } catch (e) { /* ignore */ }
  conn("live", "ready");
}());
</script>
"""

# ================================================================================================
API_BODY = """
  <header>
    <h1>API</h1>
    <span class="conn"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>
%(nav)s
  <p class="lede">Every route this deployment serves, with the scope it needs and a request that
    runs against this host. Served from the gateway itself, so what you read here is what this
    process actually answers.</p>

  <section>
    <h2>Filter</h2>
    <input id="filter" type="search" placeholder="path, method, scope or description" spellcheck="false" autocomplete="off">
    <div class="hint">Reading this page needs nothing. Each route enforces its own access — the
      scope column is what a key must carry.</div>
  </section>

  <div id="routes"><div class="empty">Loading…</div></div>
"""

API_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  var ROUTES = [];
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function conn(state, text) {
    $("dot").className = "dot " + state;
    $("conn").textContent = text;
  }

  /* The key is left as a shell variable rather than pasted in, so a copied command is never a
     copied credential -- and the host is this one, so the command runs where you are reading it. */
  function curlFor(route) {
    var lines = ["curl -sX " + route.method + " " + location.origin + route.path +
                 (route.query ? "?" + route.query : "")];
    if (route.scope) {
      lines.push("  -H \"Authorization: Bearer $MATRIXARK_API_KEY\"");
    }
    if (route.raw_body) {
      lines.push("  -H 'Content-Type: application/octet-stream'");
      lines.push("  -H 'X-Filename: playbook.md'");
      lines.push("  --data-binary @playbook.md");
    } else if (route.body) {
      lines.push("  -H 'Content-Type: application/json'");
      lines.push("  -d '" + JSON.stringify(route.body) + "'");
    }
    return lines.join(" \\\n") + "\n";
  }

  function routeHtml(route, index) {
    return '<div class="route" data-index="' + index + '" data-search="' +
      esc((route.method + " " + route.path + " " + (route.scope || "") + " " +
           route.summary).toLowerCase()) + '">' +
      '<div class="route-head"><span class="verb ' + esc(route.method) + '">' +
      esc(route.method) + '</span><span class="route-path">' + esc(route.path) + "</span>" +
      '<span class="route-scope">' + esc(route.scope || "no auth") + "</span></div>" +
      "<p>" + esc(route.summary) +
      (route.needs ? " Needs " + esc(route.needs) + " configured." : "") + "</p>" +
      '<button type="button" class="link curl" data-curl="' + index + '">show curl</button>' +
      "<pre>" + esc(curlFor(route)) + "</pre></div>";
  }

  function render() {
    var query = $("filter").value.trim().toLowerCase();
    var groups = {};
    ROUTES.forEach(function (route, index) {
      (groups[route.group] = groups[route.group] || []).push([route, index]);
    });
    var order = Object.keys(groups);
    var html = order.map(function (group) {
      var rows = groups[group].filter(function (pair) {
        return !query || (pair[0].method + " " + pair[0].path + " " + (pair[0].scope || "") + " " +
          pair[0].summary).toLowerCase().indexOf(query) >= 0;
      });
      if (!rows.length) { return ""; }
      return "<section><h2>" + esc(group) + " <span class='aux'>" + rows.length + " route" +
        (rows.length === 1 ? "" : "s") + "</span></h2>" +
        rows.map(function (pair) { return routeHtml(pair[0], pair[1]); }).join("") + "</section>";
    }).join("");
    $("routes").innerHTML = html ||
      '<section><div class="empty">Nothing matches that.</div></section>';
  }

  $("routes").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-curl]") : null;
    if (!button) { return; }
    var route = button.closest(".route");
    var open = route.classList.toggle("open");
    button.textContent = open ? "hide curl" : "show curl";
    if (open && navigator.clipboard) {
      navigator.clipboard.writeText(curlFor(ROUTES[parseInt(button.dataset.curl, 10)]));
      button.textContent = "hide curl — copied";
    }
  });
  $("filter").addEventListener("input", render);

  fetch("/v1/admin/routes")
    .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
    .then(function (d) { conn("live", "connected"); ROUTES = d.routes || []; render(); })
    .catch(function () {
      conn("down", "gateway unreachable");
      $("routes").innerHTML = '<section><div class="msg err">Could not read the route list.' +
        "</div></section>";
    });
}());
</script>
"""


TAIL = """
</div>
%(js)s
</body>
</html>
"""


def emit(filename, title, body, js, active):
    css = BASE_CSS + NAV_CSS + EXTRA_CSS
    html = (HEAD % {"title": title, "css": css}
            + (body % {"nav": nav(active)})
            + (TAIL % {"js": js.strip()}))
    path = os.path.join(PORTAL, filename)
    io.open(path, "w", encoding="utf-8", newline="\n").write(html)
    print("wrote %s (%d bytes)" % (path, len(html)))


emit("overview_portal.html", "MatrixArk", OVERVIEW_BODY, OVERVIEW_JS, "/v1/admin")
emit("api_portal.html", "MatrixArk — API", API_BODY, API_JS, "/v1/admin/api")
emit("explore_portal.html", "MatrixArk — Explore", EXPLORE_BODY, EXPLORE_JS, "/v1/admin/explore")
emit("setup_portal.html", "MatrixArk — Setup & Metrics", SETUP_BODY, SETUP_JS, "/v1/admin/setup")
emit("catalog_portal.html", "MatrixArk — Skills & Resources", CATALOG_BODY, CATALOG_JS,
     "/v1/admin/catalog")


# ---- add the nav to the two existing pages ------------------------------------------------------
def inject(filename, anchor, active):
    """Add the shared nav, or replace the one already there.

    Replace rather than skip: the nav lists every page, so adding a page has to update the nav on
    all the others. Skipping when one was already present meant the pages that existed first kept
    a nav that did not mention the pages added later.
    """
    path = os.path.join(PORTAL, filename)
    text = io.open(path, encoding="utf-8").read()
    if "portalnav" in text:
        replaced, count = re.subn(r'  <nav class="portalnav">.*?</nav>', lambda _m: nav(active),
                                  text, count=1, flags=re.S)
        if not count:
            print("nav present but unrecognised in %s" % filename)
            sys.exit(1)
        io.open(path, "w", encoding="utf-8", newline="\n").write(replaced)
        print("nav refreshed in %s" % filename)
        return
    if anchor not in text:
        print("ANCHOR MISSING in %s" % filename)
        sys.exit(1)
    text = text.replace("</style>", NAV_CSS + "</style>", 1)
    text = text.replace(anchor, anchor + "\n" + nav(active), 1)
    io.open(path, "w", encoding="utf-8", newline="\n").write(text)
    print("nav added to %s" % filename)


inject("ingestion_portal.html",
       '''    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>''', "/v1/admin/ingestion")
inject("api_key_portal.html", "<main class=\"wrap\">", "/v1/admin/portal")
