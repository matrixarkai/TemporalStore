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
  .livestrip{display:flex;gap:14px;align-items:center;margin-left:auto;padding-bottom:7px;
             flex-wrap:wrap}
  .subhead{font-size:13px;font-weight:650;margin:18px 0 0;letter-spacing:.01em}
  .status-chip{display:inline-block;font-family:"IBM Plex Mono",monospace;font-size:11px;
      padding:0 5px;border-radius:4px;background:var(--card);margin-right:4px;
      font-variant-numeric:tabular-nums}
  .status-chip.bad{background:var(--danger-bg);color:var(--danger)}
  .live-seg{display:inline-flex;gap:5px;align-items:baseline;font-size:12.5px;color:var(--muted);
            text-decoration:none;border-bottom:0;padding:0;font-weight:400;
            font-variant-numeric:tabular-nums}
  .live-seg:hover{color:var(--accent)}
  .live-seg b{font-weight:650;color:var(--ink)}
  .live-seg.warn b{color:var(--warn)}
  .live-seg.busy b{color:var(--accent)}
  .live-dot{width:7px;height:7px;border-radius:50%;background:var(--ok);flex:0 0 auto}
  .live-dot.stale{background:var(--warn)}
  .live-dot.down{background:var(--crit)}
  @media (max-width:820px){ .livestrip{margin-left:0;width:100%;padding-top:6px} }
"""

EXTRA_CSS = """
  /* setup + catalog additions */
  .field{margin-bottom:2px}
  .field .env{font-size:11.5px;color:var(--faint);font-family:"IBM Plex Mono",monospace}
  .badge{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
         padding:2px 7px;border-radius:20px;margin-left:7px;white-space:nowrap}
  .badge.restart{background:var(--warn-soft);color:var(--warn)}
  .badge.essential{background:var(--crit-soft);color:var(--crit)}
  /* Louder than .restart on purpose: that one is advice about a kind of setting, this one is a
     deployment running something other than what this page shows. */
  .badge.pending{background:var(--crit-soft);color:var(--crit)}
  .badge.portal{background:var(--accent-soft);color:var(--accent)}
  .badge.environment{background:var(--line);color:var(--muted)}
  .badge.override{background:var(--panel-2);color:var(--muted);border:1px solid var(--line)}
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
  .tabs button[data-badge]:not([data-badge=""])::after{content:attr(data-badge);
      margin-left:6px;background:var(--accent);color:var(--bg);border-radius:9px;
      padding:0 6px;font-size:11px;font-weight:650;vertical-align:1px}
  .pane[hidden]{display:none}
  .polrow{display:grid;grid-template-columns:1fr auto auto;gap:10px;align-items:center;
          padding:9px 0;border-bottom:1px solid var(--line)}
  .polrow:last-child{border-bottom:0}
  .polrow .n{font-family:"IBM Plex Mono",monospace;font-size:12.5px;overflow-wrap:anywhere}
  .polrow .d{grid-column:1/-1;font-size:12.5px;color:var(--faint);line-height:1.5;
             margin-top:-4px;max-width:78ch}
  .polrow input[type=text],.polrow select{width:120px;margin:0}
  .polrow.locked{opacity:.72}
  .src{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
       border-radius:4px;padding:2px 6px;background:var(--line);color:var(--muted)}
  .src.user{background:var(--accent-soft);color:var(--accent)}
  .src.tenant{background:var(--warn-soft);color:var(--warn)}
  .opgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:7px;
          margin-bottom:6px}
  .opbtn{text-align:left;background:var(--panel-2);color:var(--ink);border:1px solid var(--line);
         border-radius:7px;padding:8px 10px;cursor:pointer;font-size:13px;font-weight:550;
         font-family:"IBM Plex Mono",monospace}
  .opbtn:hover{border-color:var(--accent)}
  .opbtn[aria-pressed=true]{border-color:var(--accent);background:var(--accent-soft);
                            color:var(--accent)}
  .opbtn.danger[aria-pressed=true]{border-color:var(--crit);background:var(--crit-soft);
                                   color:var(--crit)}
  .opgroup{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
           color:var(--faint);margin:12px 0 6px}
  .opform{border:1px solid var(--line);border-radius:9px;padding:14px 16px;margin-top:12px;
          background:var(--panel-2)}
  .opform h3{margin:0 0 4px;font-size:15px;font-weight:650}
  .opform .route{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--muted);
                 margin-bottom:8px}
  .opform .route .scope{color:var(--faint)}
  .mblock{border:1px solid var(--line);border-radius:9px;padding:13px 15px;margin-bottom:11px;
          background:var(--panel-2)}
  .mblock h3{margin:0 0 3px;font-size:14.5px;font-weight:650;display:flex;gap:9px;
             align-items:baseline;flex-wrap:wrap}
  .mblock h3 .cur{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--muted);
                  font-weight:400}
  .mblock .mnote{font-size:13px;color:var(--muted);line-height:1.5;margin:9px 0 0;max-width:70ch}
  .mrow{display:flex;gap:9px;align-items:center;flex-wrap:wrap;margin-top:10px}
  .mrow select,.mrow input[type=text]{flex:1 1 240px;width:auto;margin:0}
  .mrow button{flex:0 0 auto}
  .mstore{font-size:12.5px;color:var(--faint);margin-top:9px;line-height:1.5}
  .mmeas{width:100%;border-collapse:collapse;font-size:12.5px;margin-top:11px}
  .mmeas th{text-align:left;font-weight:600;color:var(--muted);padding:4px 10px 4px 0;
            white-space:nowrap;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
  .mmeas td{padding:5px 10px 5px 0;border-top:1px solid var(--line);
            font-variant-numeric:tabular-nums;white-space:nowrap}
  .mmeas td.name{font-family:"IBM Plex Mono",monospace;font-size:11.5px;white-space:normal;
                 overflow-wrap:anywhere;width:99%}
  .mmeas tr.chosen td{background:var(--accent-soft)}
  .mmeas tr.chosen td.name{font-weight:650}
  .mmeas .best{color:var(--ok);font-weight:650}
  .mstore b{color:var(--muted);font-weight:600}
  .mwarn{border:1px solid var(--crit);background:var(--crit-soft);border-radius:8px;
         padding:11px 13px;margin-top:11px;font-size:13px;line-height:1.55;max-width:78ch}
  .mwarn b{display:block;margin-bottom:4px;font-size:13.5px}
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
  .route-live{font-size:11.5px;color:var(--muted);font-variant-numeric:tabular-nums}
  .route-live.bad{color:var(--danger)}
  .route-live.quiet{opacity:.6}
  .route-scope{margin-left:auto;font-size:11.5px;font-family:"IBM Plex Mono",monospace;
               color:var(--faint);white-space:nowrap}
  .route p{margin:6px 0 0;font-size:13.5px;color:var(--muted);line-height:1.55;max-width:78ch}
  .route .curl{margin-top:9px}
  .route pre{margin-top:8px;display:none}
  .route.open pre{display:block}
  .visually-hidden{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;
                   clip:rect(0 0 0 0);white-space:nowrap;border:0}
"""

# The shared live strip's script, placed on every page: the generated ones through TAIL, the two
# hand-maintained ones through `_with_nav_js`. It is deliberately self-contained -- no helper from
# any page's own script -- because it has to run identically on pages that share nothing else.
NAV_JS = r'''<script>
/* One copy helper for every page. It resolves true when the text reached the clipboard and false
   when it did not: no clipboard API at all (an http:// origin, which a self-hosted portal often
   is), a denied permission, a document that is not focused.

   Callers must report what they are handed. Announcing "copied" without looking -- which every
   caller here used to do -- is worse than saying nothing, because the reader stops trying and
   walks away with an empty clipboard.

   Its own IIFE, ahead of the strip's: the strip returns immediately on a page that has none, and
   a copier defined after that line would not exist on those pages. */
(function () {
  "use strict";
  /* Why a request was refused, in the words the edge used. A scope refusal answers
     {"error": "insufficient_scope", "required": ["admin:api_key"]}, and showing the code alone
     leaves the operator guessing at the one thing the answer already contained -- and the scope
     name is exactly the string to tick when issuing the key.

     `detail` still wins where there is one: it names the setting and the reason. The scope is
     appended to that rather than replacing it. */
  window.__matrixarkWhy = function (body, fallback) {
    body = body || {};
    var text = body.detail || body.error || fallback;
    if (body.required && body.required.length) {
      text += " — this needs " + [].concat(body.required).join(" or ");
    }
    return text;
  };

  window.__matrixarkCopyText = function (text) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text).then(
          function () { return true; }, function () { return false; });
      }
    } catch (e) { /* falls through to the failure below */ }
    return Promise.resolve(false);
  };
}());

/* The shared live strip. One stream per page: a page that runs its own (Setup, Overview) claims
   window.__matrixarkLive and calls window.__matrixarkLiveFrame instead of opening a second. */
(function () {
  "use strict";
  var strip = document.getElementById("liveStrip");
  if (!strip) { return; }
  var seg = {
    enc: document.getElementById("liveEnc"),
    imp: document.getElementById("liveImp"),
    req: document.getElementById("liveReq"),
    warn: document.getElementById("liveWarn"),
    node: document.getElementById("liveNode"),
    waiting: document.getElementById("liveWaiting")
  };
  var dot = document.getElementById("liveDot");
  /* Any page can watch the frames this page is already receiving. The alternative -- each panel
     opening its own stream -- costs the gateway a task per panel to show one deployment's state. */
  var watchers = [];
  window.__matrixarkOnFrame = function (callback) {
    if (typeof callback === "function") { watchers.push(callback); }
  };
  /* This script is emitted AFTER the page's own -- deliberately, so a page running its own stream
     can claim it before this one decides whether to open a second. The cost of that ordering was
     that a page registering at its top level called a function that did not exist yet, and two
     did: the catalog stopped re-listing when the store grew, and nothing looked wrong.

     So pages push to a queue. Whatever is waiting is drained here, and the queue is then replaced
     by an object whose `push` IS the registration, so a page loading later goes through the same
     call and nothing has to know which script ran first. */
  (window.__matrixarkFrameQueue || []).forEach(window.__matrixarkOnFrame);
  window.__matrixarkFrameQueue = { push: window.__matrixarkOnFrame };

  function n(value) {
    return Number(value || 0).toLocaleString();
  }
  function show(el, html, cls) {
    if (!el) { return; }
    /* Reset the class on the way out too: a segment hidden while it was warning would come back
       still coloured for a state that has passed. */
    el.className = "live-seg" + (html != null && cls ? " " + cls : "");
    if (html == null) { el.hidden = true; return; }
    el.hidden = false;
    el.innerHTML = html;
  }

  function plural(count, word) {
    return "<b>" + n(count) + "</b> " + word + (Number(count) === 1 ? "" : "s");
  }

  /* Rendered only from a frame that arrived. Zeros drawn before the first frame would describe a
     quiet healthy deployment for one that is not answering. */
  function render(frame) {
    strip.hidden = false;
    var e = frame.embedding || null;
    if (e && e.total) {
      show(seg.enc, e.pending
        ? "<b>" + n(e.pending) + "</b> waiting to embed"
        : "<b>" + n(e.total) + "</b> embedded",
        e.pending ? "busy" : "");
    } else if (e) {
      show(seg.enc, "<b>0</b> stored");
    } else {
      /* Absent, not zero: the backend could not be asked. "0 waiting" here would be the
         reassuring answer to a question nobody managed to ask. */
      show(seg.enc, "encoding <b>unknown</b>", "warn");
    }

    var imports = frame.imports || {};
    var active = (imports.active || [])[0];
    if (active) {
      var done = (active.done || 0) + (active.failed || 0);
      show(seg.imp, "importing <b>" + n(done) + "</b> of " + n(active.total || 0), "busy");
    } else if (imports.retryable) {
      show(seg.imp, "<b>" + n(imports.retryable) + "</b> to retry", "warn");
    } else {
      show(seg.imp, null);
    }

    var t = frame.traffic || {};
    show(seg.req, plural(t.total_requests, "request") +
      (t.total_errors ? ", <b>" + n(t.total_errors) + "</b> failed" : ""),
      t.total_errors ? "warn" : "");

    show(seg.warn, frame.warnings ? plural(frame.warnings, "warning") : null, "warn");

    /* Written and not in effect: the deployment is running something other than what the Setup
       page describes. Absent when there is none, the way the warning segment behaves -- a strip
       that always says "0 awaiting restart" is a place people stop looking. */
    show(seg.waiting,
         frame.settings_waiting
           ? plural(frame.settings_waiting, "setting") + " awaiting restart"
           : null,
         "warn");

    /* Only when it is NOT ok, the way the warning segment already behaves. A strip that always
       says "datanode: ok" is one people stop reading, and absent means nothing has looked yet --
       which is not the same as reachable, so that shows nothing either.

       Matched against the two states rather than escaped and interpolated: this block has no esc()
       (it defines n, show and plural and nothing else), so calling one would have thrown inside
       render, which the caller wraps in a catch that ignores -- failing in total silence. A closed
       set of expected values is also stricter than escaping: a server sending something
       unexpected renders nothing rather than rendering it safely. */
    show(seg.node,
      frame.datanode === "unreachable" ? "datanode <b>unreachable</b>"
        : (frame.datanode === "erroring" ? "datanode <b>erroring</b>" : null),
      "warn");

    watchers.forEach(function (callback) {
      /* One panel throwing must not stop the others, or the strip, from updating. */
      try { callback(frame); } catch (err) { /* a watcher's problem, not the strip's */ }
    });
  }

  window.__matrixarkLiveFrame = function (frame) {
    if (frame) { render(frame); dot.className = "live-dot"; }
  };
  window.__matrixarkLiveState = function (state) {
    dot.className = "live-dot" + (state === "live" ? "" : (state === "down" ? " down" : " stale"));
  };

  /* A page with its own stream feeds the strip; only pages without one connect here. */
  if (window.__matrixarkLive) { return; }
  window.__matrixarkLive = "strip";

  function key() {
    var el = document.getElementById("key");
    if (el && el.value.trim()) { return el.value.trim(); }
    try { return sessionStorage.getItem("matrixark_admin_key") || ""; } catch (e) { return ""; }
  }

  var controller = null, backoff = 1000, buffer = "";

  function open() {
    var k = key();
    if (!k) { setTimeout(open, 2000); return; }   /* inert without a key, like every page */
    controller = new AbortController();
    fetch("/v1/admin/events", {
      headers: { Authorization: "Bearer " + k },
      signal: controller.signal
    }).then(function (r) {
      if (!r.ok || !r.body) { throw new Error("stream " + r.status); }
      dot.className = "live-dot";
      backoff = 1000;
      var reader = r.body.getReader(), decoder = new TextDecoder();
      function pump() {
        return reader.read().then(function (chunk) {
          if (chunk.done) { throw new Error("stream ended"); }
          buffer += decoder.decode(chunk.value, { stream: true });
          var parts = buffer.split("\n\n");
          buffer = parts.pop();
          parts.forEach(function (block) {
            block.split("\n").forEach(function (line) {
              if (line.indexOf("data:") !== 0) { return; }
              try { render(JSON.parse(line.slice(5).trim())); } catch (err) { /* ignore */ }
            });
          });
          return pump();
        });
      }
      return pump();
    }).catch(function () {
      dot.className = "live-dot stale";
      setTimeout(open, backoff);
      backoff = Math.min(backoff * 2, 30000);  /* a gateway that is down must not be hammered */
    });
  }

  /* A hidden tab holds a connection open for nobody. Drop it, reconnect on return. */
  document.addEventListener("visibilitychange", function () {
    if (document.hidden && controller) { controller.abort(); controller = null; }
    else if (!document.hidden && !controller) { open(); }
  });
  window.addEventListener("pagehide", function () { if (controller) { controller.abort(); } });
  open();
}());

/* A fragment can name something that is folded away.
 *
 * The key portal answers "where does the first one come from" in a <details>, shut by default
 * because it is a question somebody has once. The pages that ask for a key link straight at it --
 * and a link to a closed <details> arrives with the answer still folded up, which is the dead end
 * the block was written to remove, reached by a different road. Browsers disagree about expanding
 * it; this does not depend on which one is reading.
 */
(function () {
  "use strict";

  function reveal() {
    /* Every harness that runs this file stubs a window, and most have no location on it --
       the same shape a page gets before navigation has settled. Nothing to reveal, not a crash. */
    var hash = (window.location && window.location.hash) || "";
    var name;
    try { name = decodeURIComponent(hash.replace(/^#/, "")); }
    catch (error) { name = hash.replace(/^#/, ""); }
    if (!name) { return false; }
    var target = document.getElementById(name);
    if (!target) { return false; }
    var opened = false;
    /* From the target itself: the fragment usually names the <details>, not something inside it. */
    for (var node = target; node; node = node.parentNode) {
      if (node.tagName === "DETAILS" && !node.open) { node.open = true; opened = true; }
    }
    /* The browser scrolled before this ran, to a target that was folded up. */
    if (opened && target.scrollIntoView) { target.scrollIntoView(); }
    return opened;
  }

  reveal();
  if (window.addEventListener) { window.addEventListener("hashchange", reveal); }
}());
</script>
'''

NAV_LINKS = [
    ("/v1/admin", "Overview"),
    ("/v1/admin/setup", "Setup &amp; metrics"),
    ("/v1/admin/catalog", "Skills &amp; resources"),
    ("/v1/admin/explore", "Explore"),
    ("/v1/admin/ingestion", "Ingestion"),
    ("/v1/admin/portal", "API keys"),
    ("/v1/admin/api", "API"),
]


# Rendered inside the nav element so `inject()`'s nav-replacing regex carries it to the two
# hand-maintained pages too. Empty until a frame arrives: a strip showing zeros before it has heard
# anything would report a healthy idle deployment for one that is unreachable.
LIVE_STRIP = """
    <div class="livestrip" id="liveStrip" hidden>
      <a class="live-seg" href="/v1/admin/setup#encoding" id="liveEnc" hidden></a>
      <a class="live-seg" href="/v1/admin/ingestion" id="liveImp" hidden></a>
      <a class="live-seg" href="/v1/admin/setup#traffic" id="liveReq" hidden></a>
      <a class="live-seg warn" href="/v1/admin/setup" id="liveWarn" hidden></a><span class="live-seg" id="liveNode" hidden></span><a class="live-seg warn" href="/v1/admin/setup" id="liveWaiting" hidden></a>
      <span class="live-dot" id="liveDot" title="live"></span>
    </div>"""


def nav(active):
    parts = []
    for href, label in NAV_LINKS:
        current = ' aria-current="page"' if href == active else ""
        parts.append('    <a href="%s"%s>%s</a>' % (href, current, label))
    return ('  <nav class="portalnav">\n' + "\n".join(parts) + LIVE_STRIP + "\n  </nav>")


TABS_JS = r"""
<script>
/* Tabs, for every portal page whose body declares a tablist.
 *
 * Shared rather than per page. The explore page had the only copy and this is the second caller;
 * two copies of a behaviour that looks identical is how they stop being identical.
 *
 * Click alone is not enough for role="tablist". Someone arriving on the selected tab expects the
 * arrow keys to move between them -- that is what the role announces -- and roving tabindex is
 * what makes those arrows the way through rather than one more thing to tab past.
 */
(function () {
  "use strict";

  window.wireTabs = function (onShow) {
    var tabs = Array.prototype.slice.call(document.querySelectorAll(".tabs button"));
    if (!tabs.length) { return; }

    function show(button) {
      tabs.forEach(function (b) {
        var selected = b === button;
        b.setAttribute("aria-selected", String(selected));
        b.tabIndex = selected ? 0 : -1;
      });
      Array.prototype.forEach.call(document.querySelectorAll(".pane"), function (pane) {
        pane.hidden = pane.id !== "pane-" + button.dataset.pane;
      });
      if (typeof onShow === "function") { onShow(button.dataset.pane); }
    }

    tabs.forEach(function (button, index) {
      button.addEventListener("click", function () { show(button); });
      button.addEventListener("keydown", function (event) {
        var next = null;
        if (event.key === "ArrowRight") { next = tabs[(index + 1) % tabs.length]; }
        else if (event.key === "ArrowLeft") { next = tabs[(index - 1 + tabs.length) % tabs.length]; }
        else if (event.key === "Home") { next = tabs[0]; }
        else if (event.key === "End") { next = tabs[tabs.length - 1]; }
        if (!next) { return; }
        event.preventDefault();
        show(next);
        next.focus();
      });
    });

    /* A fragment can name something inside a pane that is not showing.
     *
     * The live strip does exactly that from every page: "/v1/admin/setup#encoding" is a div inside
     * the Settings pane, "#traffic" one inside Checks, and both panes load hidden. The browser
     * scrolls to a fragment once, at load, and a hidden element is nowhere to scroll to -- so the
     * badge arrived at the page, the default tab came up, and the state it was reporting on was not
     * on screen. Nothing said so; the link worked in the only sense a link can be said to work.
     *
     * Which pane holds the target is the page's business, not this helper's, so it is read off the
     * element rather than listed here: any fragment naming anything inside any pane opens it. */
    function fromHash(hash) {
      var name;
      try { name = decodeURIComponent((hash || "").replace(/^#/, "")); }
      catch (error) { name = (hash || "").replace(/^#/, ""); }
      if (!name) { return false; }
      var target = document.getElementById(name);
      var pane = target && target.closest ? target.closest(".pane") : null;
      if (!pane) { return false; }
      var button = document.getElementById("tab-" + pane.id.replace(/^pane-/, ""));
      if (!button) { return false; }
      show(button);
      /* The pane was hidden when the browser did its own scrolling, so it did not scroll. */
      if (target.scrollIntoView) { target.scrollIntoView(); }
      return true;
    }

    /* Clicking "#traffic" while already on this page changes no document, so nothing reloads and
       the only signal is this event. That is the strip's own page -- the case most likely to be
       reached and the one where doing nothing looks most like a dead control. */
    if (window.addEventListener) {
      window.addEventListener("hashchange", function () {
        fromHash(window.location && window.location.hash);
      });
    }

    var already = tabs.filter(function (b) {
      return b.getAttribute("aria-selected") === "true";
    });
    if (!fromHash(window.location && window.location.hash)) {
      show(already[0] || tabs[0]);
    }
  };

  /* Show a pane from code. Needed whenever one pane's control writes into another's: without
     it the change lands somewhere the person cannot see, and any instruction about what to do
     next refers to a part of the page that is not on screen. */
  window.showTab = function (pane) {
    var button = document.getElementById("tab-" + pane);
    if (button) { button.click(); }
  };

  /* Put a count on a tab. A pane that is hidden cannot report anything itself, and the case that
     matters is a form with unsaved changes in it. */
  window.markTab = function (pane, badge) {
    var button = document.getElementById("tab-" + pane);
    if (!button) { return; }
    button.setAttribute("data-badge", badge ? String(badge) : "");
  };
}());
</script>
"""


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


  <div class="tabs" role="tablist" aria-label="Setup sections">
    <button type="button" role="tab" id="tab-access" aria-controls="pane-access"
            data-pane="access" aria-selected="true">Access</button>
    <button type="button" role="tab" id="tab-settings" aria-controls="pane-settings"
            data-pane="settings" aria-selected="false" tabindex="-1">Settings</button>
    <button type="button" role="tab" id="tab-health" aria-controls="pane-health"
            data-pane="health" aria-selected="false" tabindex="-1">Checks</button>
    <button type="button" role="tab" id="tab-deployment" aria-controls="pane-deployment"
            data-pane="deployment" aria-selected="false" tabindex="-1">Deployment</button>
  </div>

  <section class="pane" role="tabpanel" aria-labelledby="tab-access" id="pane-access">
  <section>
    <h2>Access <span class="aux"><label class="check"><input type="checkbox" id="remember"> remember for this browser tab</label></span></h2>
    <label for="key">Admin API key</label>
    <input id="key" type="password" placeholder="Key carrying an admin scope" autocomplete="off" spellcheck="false">
    <p class="hint">No key yet? <a href="/v1/admin/portal#firstkey">Where the first one comes from</a>
      — no page can mint it; it takes one command where the gateway runs.</p>
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

  <section>
    <h2>Models <span class="aux"><button class="link" id="probeModels" type="button">ask the
      endpoints what they serve</button></span></h2>
    <p class="hint" style="margin-top:0">Both of these are free text on the wire, and a name that is
      merely misspelt does not fail loudly: extraction falls back to the local rules and embedding to
      hash vectors, both answering 200. Pick from the catalogue, or ask the configured endpoint for
      its own list. Choosing here fills the field below — nothing is written until you save.</p>
    <div id="models"><div class="empty">Enter an admin key to choose models.</div></div>
  </section>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-settings" id="pane-settings" hidden>

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
    <h2>Retrieval settings <span class="aux" id="polAux"></span></h2>
    <p class="hint" style="margin-top:0">Tune retrieval for everyone in this tenant, or for one
      person. Each row says where its current value came from — that user, the tenant, the
      environment, or the built-in default — so a setting that looks wrong can be traced to the
      layer that set it. Changes apply to the next request; no restart.</p>
    <div class="grid2">
      <div>
        <label for="polLevel">Applies to</label>
        <select id="polLevel">
          <option value="tenant">Everyone in this tenant</option>
          <option value="user">One user</option>
        </select>
      </div>
      <div>
        <label for="polUser">User</label>
        <input id="polUser" type="text" placeholder="alice" spellcheck="false" autocomplete="off">
      </div>
      <div>
        <label for="polFilter">Filter</label>
        <input id="polFilter" type="search" placeholder="name or description" spellcheck="false">
      </div>
    </div>
    <div class="actions">
      <button id="polLoad" class="ghost" type="button">Load settings</button>
      <label class="check"><input type="checkbox" id="polOnlySettable" checked> only what this
        level can set</label>
    </div>
    <div id="polMsg" role="status" aria-live="polite"></div>
    <div id="polWhy" class="hint" hidden></div>
    <div id="policy"><div class="empty">Name a user and load their settings.</div></div>
  </section>

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
    <h2>Encoding <span class="aux" id="encodeAux"></span></h2>
    <p class="hint" style="margin-top:0">Ingest can defer encoding: chunking is synchronous and the
      vector is filled in behind it. Until a chunk is encoded it exists and cannot be matched on
      meaning, so a retrieve over that window returns less than it should and says nothing — which
      reads as bad retrieval rather than unfinished retrieval.</p>
    <div id="encoding"><div class="empty">Enter an admin key to read the encoding state.</div></div>
  </section>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-health" id="pane-health" hidden>

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
    <h3 class="subhead">Recent failures</h3>
    <p class="hint" style="margin-top:0">The last few requests this worker answered with an error,
      newest first. Route, method and status only &mdash; no key, no tenant, nothing about who
      asked. Counters tell you seven requests failed; this tells you whether that was a minute ago
      or last Tuesday.</p>
    <div id="failures"><div class="empty">Nothing yet.</div></div>
  </section>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-deployment" id="pane-deployment" hidden>

  <section>
    <h2>Where this deployment lives</h2>
    <p class="hint" style="margin-top:0">Where this deployment lives and how it is reached. Read
      only, deliberately: repointing a storage directory or a cluster address from a browser form
      does not reconfigure a running deployment, it strands its data. Change these where the process
      is started.</p>
    <div id="inventory"><div class="empty">Enter an admin key to see the deployment.</div></div>
  </section>

  <section>
    <h2>Launch a deployment <span class="aux"><button class="link" id="copyEnv" type="button">copy env file</button></span></h2>
    <p class="hint" style="margin-top:0">The storage directories, the metaserver address and the
      topology are not editable on the Settings tab, and that is deliberate — repointing a running deployment's
      storage from a browser does not reconfigure it, it strands its data. They are chosen when a
      deployment is <em>launched</em>, which is what this composes.</p>
    <div id="liveShape" class="hint"></div>
    <div class="grid2">
      <label class="field"><span>Shape</span>
        <select id="depShape"></select>
        <span class="hint" id="depShapeWhen"></span>
      </label>
      <label class="field"><span>Storage</span>
        <select id="depStorage"></select>
        <span class="hint" id="depStorageNote"></span>
      </label>
      <label class="field"><span>Nodes</span>
        <input id="depNodes" type="number" min="1" max="99" step="1" value="1">
        <span class="hint">Raft needs an odd count so a majority exists.</span>
      </label>
      <label class="field"><span>Data root</span>
        <input id="depRoot" type="text" placeholder="/var/lib/temporalstore">
        <span class="hint">Where pages, blobs, index and cache go.</span>
      </label>
      <label class="field" id="depSharedField" hidden><span>Shared directory</span>
        <input id="depShared" type="text" placeholder="/srv/temporalstore/shared">
        <span class="hint">Must be the same storage seen by every node, not a per-node path of
          the same name.</span>
      </label>
      <label class="field"><span>Key variables</span>
        <input id="depKeys" type="text" placeholder="DEEPSEEK_API_KEY, OPENAI_API_KEY">
        <span class="hint">Names only. Values are entered on the Settings tab and never appear in a plan.</span>
      </label>
    </div>
    <div id="depMsg" role="status" aria-live="polite"></div>
    <div id="depVerdict"></div>
    <pre id="depEnv"></pre>
    <h3>Launch <span class="aux"><button class="link" id="copyUserData" type="button">copy
      user-data</button></span></h3>
    <p class="hint" style="margin-top:0">Nothing here runs from this page. The gateway holds no AWS
      credentials, and creating billable infrastructure is not something a web page should do on a
      click — so this produces exactly what to run, and the command that destroys it again.
      Key <em>names</em> appear in the script; key <em>values</em> never do, because user-data is
      readable from the instance metadata service by anything on the box.</p>
    <pre id="depUserData"></pre>
    <pre id="depCommands"></pre>
  </section>

  <section>
    <h2>Grafana <span class="aux"><button class="link" id="copyScrape" type="button">copy scrape config</button></span></h2>
    <p class="hint" style="margin-top:0">Everything on this page is also exported at
      <span class="mono">/v1/metrics</span> in Prometheus text format — aggregate counters only, no
      keys and no tenant identifiers, so it is safe to scrape without credentials.</p>
    <p class="hint">Three steps: point Prometheus at the scrape config below, import the dashboards
      into Grafana, and load the alert rules. The dashboards are served by this gateway rather than
      named as a file path, so the panels match the metrics this build actually emits — a panel
      querying a series nobody emits is a blank one, and a blank panel reads as “no traffic”.</p>
    <p class="hint">Check the <b>scraped from</b> column before importing. The gateway and the
      engine publish on different endpoints, and an engine dashboard pointed at the gateway shows
      twelve empty panels — which looks like an idle cluster, not a misdirected query.</p>
    <div id="monitoring"><div class="empty">Enter an admin key to list the dashboards.</div></div>
    <p class="hint">Grafana → Dashboards → New → Import → Upload JSON. The alert rules are
      Prometheus rule files, not Grafana imports.</p>
    <div id="grafanaMsg" role="status" aria-live="polite"></div>
    <table>
      <tr><td class="mono">matrixark_gateway_embedding_semantic</td><td>0 means retrieval is running on hash
        vectors. Alert on it — nothing else distinguishes that deployment from a healthy one.</td></tr>
      <tr><td class="mono">matrixark_gateway_extraction_model_active</td><td>0 means ingest stores only what the
        local rules extract, with no model call.</td></tr>
      <tr><td class="mono">matrixark_gateway_config_warnings</td><td>How many configuration warnings are live
        right now. Steady state is 0.</td></tr>
      <tr><td class="mono">matrixark_gateway_request_duration_seconds</td><td>Edge latency histogram, by route.</td></tr>
      <tr><td class="mono">matrixark_gateway_requests_total</td><td>Edge requests by route, method and status.</td></tr>
      <tr><td class="mono">matrixark_ingestion_documents_retryable</td><td>Documents that failed an
        import for a reason worth retrying. Non-zero means work is sitting there until somebody
        presses retry — nothing else raises its hand.</td></tr>
      <tr><td class="mono">matrixark_gateway_config_changed_timestamp_seconds</td><td>When the
        configuration was last written from the portal. Half of “it started behaving differently on
        Tuesday” is answered by knowing whether anyone changed it on Tuesday.</td></tr>
    </table>
    <pre id="scrape"></pre>
  </section>
  </section>
"""

SETUP_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };

  window.wireTabs();

  /* Claimed before the strip's own script runs, so it feeds from this connection instead of
     opening a second one to the same endpoint. */
  window.__matrixarkLive = "page";

/* ---------- the live stream ---------- */
/* Read over fetch() rather than with EventSource, which cannot set an Authorization header --
   the alternative is the key in a query string, and a credential in a URL ends up in every access
   log and proxy trace between here and the gateway. Same wire format either way; only the
   reconnect is ours to write. */
function liveStream(options) {
  var onFrame = options.onFrame || function () {};
  var onState = options.onState || function () {};
  var headers = options.headers || function () { return {}; };
  var controller = null, stopped = false, backoff = 1000, buffer = "";

  function schedule() {
    if (stopped) { return; }
    onState("retrying", Math.round(backoff / 1000));
    setTimeout(open, backoff);
    backoff = Math.min(backoff * 2, 30000);  /* a gateway that is down should not be hammered */
  }

  function handle(block) {
    var payload = null;
    block.split("\n").forEach(function (line) {
      if (line.indexOf("data: ") === 0) { payload = line.slice(6); }
    });
    if (!payload) { return; }
    try { onFrame(JSON.parse(payload)); } catch (e) { /* a partial frame; the next one is whole */ }
  }

  function open() {
    if (stopped) { return; }
    controller = new AbortController();
    onState("connecting");
    fetch("/v1/admin/events", { headers: headers(), signal: controller.signal })
      .then(function (response) {
        if (!response.ok) { return Promise.reject(response.status); }
        if (!response.body) { return Promise.reject("nostream"); }
        onState("live");
        backoff = 1000;
        var reader = response.body.getReader();
        var decoder = new TextDecoder();
        function pump() {
          return reader.read().then(function (chunk) {
            if (chunk.done) { schedule(); return; }
            buffer += decoder.decode(chunk.value, { stream: true });
            var blocks = buffer.split("\n\n");
            buffer = blocks.pop();
            blocks.forEach(handle);
            return pump();
          });
        }
        return pump();
      })
      .catch(function (err) {
        if (stopped) { return; }
        if (err === 401 || err === 403) { onState("denied"); return; }
        schedule();
      });
  }

  open();
  window.addEventListener("pagehide", function () {
    stopped = true;
    if (controller) { controller.abort(); }
  });
  return {
    restart: function () {
      if (controller) { controller.abort(); }
      backoff = 1000;
      stopped = false;
      open();
    },
    stop: function () { stopped = true; if (controller) { controller.abort(); } }
  };
}

  var loaded = null;      /* last GET /v1/admin/config payload */
  var fields = {};        /* setting key -> field descriptor from the server */
  var edits = {};         /* setting key -> value typed/reset since the last load */
  var lastConfigAt = null;
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
    /* Values in the file that this build has no setting for. They were set deliberately by
       somebody who then upgraded, and they have quietly meant nothing since. The page cannot
       remove them -- a write of an unknown key is refused, correctly -- so it says where they
       are instead. */
    var forgotten = ((d.settings || {}).unknown_stored) || [];
    $("warnings").innerHTML = (d.warnings || []).map(function (w) {
      return '<div class="note">' + esc(w) + "</div>";
    }).join("") + (forgotten.length
      ? '<div class="note">' + esc(forgotten.length) +
        (forgotten.length === 1 ? " stored value is" : " stored values are") +
        " not a setting this build has, so " +
        (forgotten.length === 1 ? "it does" : "they do") + " nothing: " +
        esc(forgotten.join(", ")) + ". Stored by an older build, in " +
        esc((d.settings || {}).config_file || "the configuration file") + "." +
        "</div>"
      : "");
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

  /* A byte count is stored as bytes and read by a person. 1073741824 and 268435456 differ by a
     factor of four and look the same at a glance, which is the sort of thing that gets a cache
     sized wrong. The stored value is untouched -- this only adds a reading of it.

     Applies to any setting whose variable ends in _BYTES, so it covers the two quota settings
     that predate the engine group as well. */
  function byteHint(f) {
    if (!f.env || !/_BYTES$/.test(f.env)) { return ""; }
    var raw = (f.value === "" || f.value == null) ? f.default : f.value;
    var size = Number(raw);
    if (!isFinite(size) || size <= 0) { return ""; }
    var units = ["B", "KiB", "MiB", "GiB", "TiB"], step = 0;
    while (size >= 1024 && step < units.length - 1) { size /= 1024; step += 1; }
    var shown = (step === 0 || size >= 10) ? String(Math.round(size))
      : String(Math.round(size * 10) / 10);
    return ' <span class="env">= ' + shown + " " + units[step] + "</span>";
  }

  function fieldHtml(f) {
    var badges = "";
    if (f.essential) { badges += '<span class="badge essential">required</span>'; }
    /* Written since this process started, and read only at startup, so the deployment is
       running the old value right now. This replaces the "needs restart" note rather than
       joining it: that note is about the kind of setting and is true before anybody touches
       anything, and showing both buries the half that is about this deployment today. */
    if (f.pending_restart) {
      badges += '<span class="badge pending" title="This value is saved but not in effect: it ' +
        'is read once at startup, and this process is still running the previous one. Restart ' +
        'the gateway to apply it.">changed — restart to apply</span>';
    } else if (f.applies === "restart") {
      badges += '<span class="badge restart">needs restart</span>';
    }
    /* The launcher set this variable before the process started. A change here takes effect now
       and is dropped at the next restart, because boot values keep precedence over stored ones.
       Without this the field looks exactly like any other live setting. */
    if (f.boot_pinned) {
      badges += '<span class="badge restart" title="Your launcher sets this variable, and a ' +
        'launcher value wins at startup. A change here applies immediately and will be replaced ' +
        'by the launcher value the next time this process restarts. Change it where the launcher ' +
        'sets it to make it stick.">launcher sets this</span>';
    }
    /* What is set here is the fallback. Without this the field reads as the final answer, and a
       customer whose tenant or user override says otherwise has no way to know from this screen. */
    if ((f.overridable_by || []).length) {
      badges += '<span class="badge override" title="What you set here is the fallback. A ' +
        (f.overridable_by || []).join(" or ") +
        ' setting wins where one exists — set those under Retrieval settings.">' +
        esc((f.overridable_by || []).join("/")) + " can override</span>";
    }
    if (f.source && f.source !== "default") {
      badges += '<span class="badge ' + esc(f.source) + '">' + esc(f.source) + "</span>";
    }
    var resettable = f.kind !== "secret" &&
      ((f.value || "") !== (f.default == null ? "" : String(f.default)));
    var reset = resettable
      ? '<button type="button" class="link reset" data-reset="' + esc(f.key) + '">reset</button>' : "";
    var help = esc(f.help) + (f.env ? ' <span class="env">' + esc(f.env) + "</span>" : "") +
      byteHint(f);
    var longCls = f.help.length > 190 ? " long" : "";
    return '<div class="field" data-field="' + esc(f.key) + '" data-essential="' +
      (f.essential ? "1" : "0") + '" data-search="' +
      esc((f.label + " " + f.env + " " + f.help + " " + f.key).toLowerCase()) + '">' +
      '<label class="row" for="' + fieldId(f.key) + '">' + esc(f.label) + badges + reset + "</label>" +
      controlHtml(f) + '<div class="helptext' + longCls + '">' + help + "</div></div>";
  }

  function unsetSecrets() {
    /* The snapshot never carries a secret VALUE -- it reports `kind: "secret"` and a
       `configured` boolean -- so this asks the only question there is to ask. */
    return Object.keys(fields)
      .filter(function (k) { return fields[k].kind === "secret" && !fields[k].configured; })
      .sort();
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

  /* ---------- encoding ---------- */
  /* Polled on its own cadence: fast while there is a backlog, because watching it shrink is the
     point, and slowly once there is not. Deliberately NOT on /v1/metrics -- answering it walks the
     record log, and a scrape every fifteen seconds doing that is a self-inflicted load. */
  var encodeTimer = null, encodeMs = 0;
  /* Whether the stream is delivering. The stream already carries the encoding backlog and calls
     renderEncoding with it, so polling for the same number while it is live is a second read of
     one answer -- and that endpoint walks the record log. */
  var streamLive = false;

  function encodePace(draining) {
    var want = draining ? 5000 : 60000;
    if (want === encodeMs && encodeTimer) { return; }
    if (encodeTimer) { clearInterval(encodeTimer); }
    encodeMs = want;
    encodeTimer = setInterval(function () {
      if (document.hidden || !$("key").value.trim()) { return; }
      /* The fallback only. While the stream is live it delivers this, sooner than this timer
         would ask for it -- 4s against 5 while draining, 30s against 60 when idle.

         Gated on the connection, not on when a frame last arrived: an unchanged frame is no
         longer republished, so a healthy stream on a quiet deployment sends nothing for minutes.
         A clock cannot tell that apart from a dead connection, and would start polling precisely
         when there is least reason to. */
      if (streamLive) { return; }
      loadEncoding();
    }, want);
  }

  function renderEncoding(d) {
    var total = d.total || 0, encoded = d.encoded || 0, pending = d.pending || 0;
    var pct = total ? Math.round((encoded / total) * 1000) / 10 : 100;
    var encoder = d.encoder || {};
    var rows = "";

    if (!encoder.semantic) {
      /* Zero pending means something else entirely here: nothing is waiting because nothing will
         ever be encoded. Reporting "all done" would be true and useless. */
      rows += '<div class="note">' + esc(encoder.note ||
        "No encoder is configured, so nothing is waiting and nothing ever will be.") + "</div>";
    }
    if (d.mixed_dimensions) {
      rows += '<div class="note">This store holds vectors of more than one width. Vectors of ' +
        "different dimensions cannot be compared, so some memories can never match a query — " +
        "which is what happens when the embedding model changes without a backfill, and looks " +
        "exactly like ordinary poor recall.</div>";
    }

    var barCls = pending ? "" : "done";
    rows += '<div class="bar"><i class="' + barCls + '" style="width:' + pct + '%"></i></div>' +
      '<div class="job-stats">' +
      "<span><b>" + encoded.toLocaleString() + "</b> encoded</span>" +
      "<span><b>" + pending.toLocaleString() + "</b> waiting</span>" +
      "<span><b>" + pct + "%</b> of " + total.toLocaleString() + "</span>" +
      (d.deferred_tasks ? "<span><b>" + d.deferred_tasks + "</b> deferred tasks</span>" : "") +
      "</div>";

    var detail = [];
    (d.models || []).forEach(function (m) {
      detail.push(esc(m.model) + " · " + m.count.toLocaleString());
    });
    (d.dimensions || []).forEach(function (dim) {
      detail.push(dim.dim + "-dim · " + dim.count.toLocaleString());
    });
    if (detail.length) {
      rows += '<div class="hint">' + detail.join(" &nbsp;·&nbsp; ") + "</div>";
    }
    $("encoding").innerHTML = rows;
    $("encodeAux").textContent = pending
      ? pending.toLocaleString() + " waiting"
      : (total ? "all encoded" : "nothing stored yet");
    encodePace(pending > 0);
  }

  function loadEncoding() {
    fetch("/v1/admin/embeddings", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(renderEncoding)
      .catch(function (e) {
        /* A backend that cannot answer must not read as "nothing is pending". */
        $("encoding").innerHTML = '<div class="msg err">' +
          esc(typeof e === "number" ? failure(e) : "Could not reach the gateway.") +
          " The encoding state is unknown, not empty.</div>";
        $("encodeAux").textContent = "unknown";
        encodePace(false);
      });
  }

  /* ---------- model picker ---------- */
  /* Kept as a chooser over the SAME fields the form below writes: one save path, one entry in the
     change log. A second write path for models would be a second thing to keep correct. */
  var modelData = {};
  /* What is selected in the dropdown but not yet applied to the form. The risk of an encoder
     change has to be visible while it is being chosen; showing it only after the field is filled
     in tells someone about a decision they have already made. */
  var pendingPick = {};

  var MODEL_TARGETS = [
    { target: "extraction", key: "extraction.model", title: "Extraction model",
      note: "The model that reads a document and decides what is worth remembering. When it is " +
            "unset or unreachable, ingest keeps working — it stores only what the local rules " +
            "pull out, and says nothing." },
    { target: "embedding", key: "embedding.model", title: "Embedding model",
      note: "The encoder that turns text into the vectors retrieval searches. Changing it is the " +
            "one setting on this page that can quietly invalidate data you already have." }
  ];

  function loadModels(probe) {
    if (!$("key").value.trim()) { return Promise.resolve(); }
    return Promise.all(MODEL_TARGETS.map(function (t) {
      return fetch("/v1/admin/models?target=" + t.target + "&probe=" + (probe ? "1" : "0"),
                   { headers: auth() })
        .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
        .then(function (d) { modelData[t.target] = d; })
        .catch(function (status) {
          modelData[t.target] = { error: failure(typeof status === "number" ? status : 0) };
        });
    })).then(renderModels);
  }

  /* The catalogue, whatever the endpoint reported, and the value in use -- deduplicated, because a
     model appearing twice in a list reads as two different models. */
  function modelOptions(target) {
    var d = modelData[target] || {};
    var seen = {}, out = [];
    function push(name, note) {
      name = String(name || "").trim();
      if (!name || seen[name]) { return; }
      seen[name] = 1;
      out.push({ model: name, note: note || "" });
    }
    if (d.current) { push(d.current, "currently configured"); }
    ((d.discovered || {}).models || []).forEach(function (m) {
      push(m, "offered by the configured endpoint");
    });
    (d.catalogue || []).forEach(function (c) {
      /* The width collision belongs in the option text, not in a footnote: it is the reason a
         switch that looks harmless is not. Two encoders of the same width raise no error. */
      var same = (c.same_width_as || []).length
        ? " — same width as " + (c.same_width_as || []).join(", ") + ", different space"
        : "";
      /* Lead with the measurement where there is one. "hit@1 76.2%" is the number a customer is
         choosing on; the sentence is the explanation of it. */
      var measured = c.hit_at_1 != null
        ? "hit@1 " + c.hit_at_1 + "%, " + c.texts_per_s + " texts/s, " + c.dim + " dims" + same
        : (c.dim ? c.dim + " dimensions" + same : "");
      push(c.model, measured || c.note);
    });
    return out;
  }

  /* Same rule the backend uses: a repository prefix is not part of the model's identity. Without
     it the table highlights nothing whenever the deployment names the model in the shorter form,
     which reads as "you are on something exotic" while sitting on a listed model. */
  function sameModel(left, right) {
    left = String(left || "").trim().replace(/\/+$/, "");
    right = String(right || "").trim().replace(/\/+$/, "");
    if (!left || !right) { return false; }
    if (left === right) { return true; }
    return left.split("/").pop() === right.split("/").pop();
  }

  /* The backend reports models and widths as {model, count} / {dim, count} rows, not bare names. */
  function storedModelNames(s) {
    return (s.models || []).map(function (m) {
      return typeof m === "string" ? m : String((m || {}).model || "");
    }).filter(function (m) { return m; });
  }

  function storedModelLine(d) {
    var s = d.in_store || {};
    if (!s.known) {
      return '<div class="mstore">' + esc(s.detail || "The store was not asked.") + "</div>";
    }
    if (!s.total) {
      return '<div class="mstore">Nothing is stored yet, so there is nothing a change could ' +
        "strand. This is the moment to settle on an encoder.</div>";
    }
    var models = storedModelNames(s);
    var widths = (s.dimensions || []).map(function (x) {
      return typeof x === "number" ? String(x) : String((x || {}).dim || "");
    }).filter(function (x) { return x; });
    return '<div class="mstore"><b>' + esc(s.total) + "</b> stored vector" +
      (s.total === 1 ? "" : "s") +
      (models.length ? ", made with <b>" + models.map(esc).join("</b>, <b>") + "</b>" :
        ", made with an encoder this build did not record") +
      (widths.length ? " · " + esc(widths.join(", ")) + " dimensions" : "") +
      (s.mixed_dimensions ? " · <b>widths already differ</b>, so some of this store is already " +
        "unsearchable against the rest" : "") + "</div>";
  }

  /* Show the warning only when the choice in the box would actually strand something. Warning on
     every render trains people to click past it, and then it is not there when it matters. */
  function embeddingRisk(d, chosen) {
    var s = d.in_store || {};
    if (!chosen) { return ""; }
    if (s.known && !s.total) { return ""; }
    var models = storedModelNames(s);
    /* Compared with the same rule the backend uses. A repository prefix is not part of the model's
       identity, and warning that a store will be stranded by re-spelling the encoder it already
       holds is how the warning gets clicked past on the change that would strand it. */
    if (s.known && models.length === 1 && sameModel(models[0], chosen)) { return ""; }
    if (s.known && models.length > 1 &&
        models.every(function (m) { return sameModel(m, chosen); })) { return ""; }
    if (!s.known || !models.length) {
      if (sameModel(chosen, d.current || "")) { return ""; }
    }
    return '<div class="mwarn"><b>Changing this strands what is already stored</b>' +
      esc(d.change_warning || "") + "</div>";
  }

  function renderModels() {
    $("models").innerHTML = MODEL_TARGETS.map(function (t) {
      var d = modelData[t.target] || {};
      if (d.error) {
        return '<div class="mblock"><h3>' + esc(t.title) + '</h3><div class="mnote">' +
          esc(d.error) + "</div></div>";
      }
      var chosen = Object.prototype.hasOwnProperty.call(pendingPick, t.target)
        ? pendingPick[t.target]
        : ((Object.prototype.hasOwnProperty.call(edits, t.key) && edits[t.key] !== null)
            ? edits[t.key] : (d.current || ""));
      var options = modelOptions(t.target);
      var known = options.some(function (o) { return sameModel(o.model, chosen); });
      var disc = d.discovered || {};
      var discLine = disc.available
        ? "The endpoint lists " + disc.count + " model" + (disc.count === 1 ? "" : "s") + "."
        : (disc.reason === "not_probed"
            ? "The endpoint has not been asked what it serves."
            : esc(disc.detail || "The endpoint could not be asked."));
      return '<div class="mblock" data-target="' + esc(t.target) + '"><h3>' + esc(t.title) +
        '<span class="cur">' + (d.current ? esc(d.current) : "not set") + "</span></h3>" +
        '<div class="mnote">' + esc(t.note) + "</div>" +
        '<div class="mrow"><select data-pick="' + esc(t.target) + '">' +
        options.map(function (o) {
          return '<option value="' + esc(o.model) + '"' +
            (o.model === chosen ? " selected" : "") + ">" + esc(o.model) +
            (o.note ? " — " + esc(o.note) : "") + "</option>";
        }).join("") +
        '<option value="__other__"' + (known || !chosen ? "" : " selected") +
        ">something else — type it</option></select>" +
        '<input type="text" data-other="' + esc(t.target) + '" placeholder="model name" value="' +
        (known ? "" : esc(chosen)) + '"' + (known || !chosen ? " hidden" : "") +
        ' spellcheck="false">' +
        '<button type="button" class="ghost" data-use="' + esc(t.target) + '">Use this</button>' +
        "</div>" +
        '<div class="mnote" style="margin-top:7px">' + discLine + "</div>" +
        (t.target === "embedding"
          ? measuredTable(d, chosen) + storedModelLine(d) + embeddingRisk(d, chosen)
          : "") +
        "</div>";
    }).join("");
  }

  /* The measurement, as a table rather than as prose in six option labels. Sorted by the number
     a customer is actually choosing on, with the best in each column marked -- otherwise every row
     reads as equally reasonable and the default wins by being first. */
  function measuredTable(d, chosen) {
    var rows = (d.catalogue || []).filter(function (c) { return c.hit_at_1 != null; });
    if (!rows.length) { return ""; }
    rows = rows.slice().sort(function (a, b) { return b.hit_at_1 - a.hit_at_1; });
    var best = {
      hit_at_1: Math.max.apply(null, rows.map(function (c) { return c.hit_at_1; })),
      texts_per_s: Math.max.apply(null, rows.map(function (c) { return c.texts_per_s; })),
      vectors_mb_per_doc: Math.min.apply(null, rows.map(function (c) {
        return c.vectors_mb_per_doc;
      }))
    };
    function cell(value, key, suffix) {
      return "<td" + (value === best[key] ? ' class="best"' : "") + ">" +
        esc(value) + (suffix || "") + "</td>";
    }
    return '<table class="mmeas"><thead><tr><th>Encoder</th><th>hit@1</th><th>hit@5</th>' +
      "<th>speed</th><th>storage</th><th>dims</th></tr></thead><tbody>" +
      rows.map(function (c) {
        return "<tr" + (sameModel(c.model, chosen) ? ' class="chosen"' : "") + '><td class="name">' +
          esc(c.model) + (c.recommended ? ' <span class="badge portal">recommended</span>' : "") +
          (c.needs_prefix ? ' <span class="tag">needs query/passage prefixes</span>' : "") +
          "</td>" +
          cell(c.hit_at_1, "hit_at_1", "%") +
          "<td>" + esc(c.hit_at_5) + "%</td>" +
          cell(c.texts_per_s, "texts_per_s", "/s") +
          cell(c.vectors_mb_per_doc, "vectors_mb_per_doc", " MB/doc") +
          "<td>" + esc(c.dim) + "</td></tr>";
      }).join("") + "</tbody></table>" +
      '<div class="hint" style="margin-top:6px">Measured on this deployment\'s own corpus. ' +
      "Storage is per document at that model\'s window — a wider window needs fewer vectors, " +
      "which is why the slowest model here is also the smallest.</div>";
  }

  function pickedModel(target) {
    var select = $("models").querySelector('[data-pick="' + target + '"]');
    var other = $("models").querySelector('[data-other="' + target + '"]');
    if (!select) { return ""; }
    return select.value === "__other__" ? (other ? other.value.trim() : "") : select.value;
  }

  $("models").addEventListener("change", function (ev) {
    var dataset = ev.target && ev.target.dataset ? ev.target.dataset : {};
    var target = dataset.pick || dataset.other;
    if (!target) { return; }
    if (dataset.pick && ev.target.value === "__other__") {
      /* Do not re-render: it would replace the text box the customer is about to type into. */
      var other = $("models").querySelector('[data-other="' + target + '"]');
      if (other) { other.hidden = false; other.focus(); }
      return;
    }
    pendingPick[target] = pickedModel(target);
    renderModels();
  });

  $("models").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-use]") : null;
    if (!button) { return; }
    ev.preventDefault();
    var target = button.dataset.use;
    var entry = MODEL_TARGETS.filter(function (t) { return t.target === target; })[0];
    var value = pickedModel(target);
    if (!entry || !value) {
      say($("saveMsg"), "Type a model name first.", "warn");
      return;
    }
    var el = $("groups").querySelector('[data-key="' + entry.key + '"]');
    if (!el) {
      say($("saveMsg"), "That field is not loaded — enter an admin key first.", "warn");
      return;
    }
    el.value = value;
    edits[entry.key] = value;
    pendingPick[target] = value;
    markDirty();
    renderModels();
    var group = el.closest("details");
    if (group) { group.open = true; }
    /* This control is on the Access tab and the field it just set is on the Settings tab. Without
       moving there, the scroll below is a no-op on a hidden pane and the "save it" message points
       at a save bar the person cannot see. */
    window.showTab("settings");
    el.scrollIntoView({ block: "center" });
    /* Deliberately not saved here. A model change is worth seeing next to whatever else is
       pending -- a key that has not been filled in yet, most of all. */
    say($("saveMsg"), entry.title + " set to " + value + " — not saved yet. Save configuration " +
      "below to apply it.", "info");
  });

  $("probeModels").addEventListener("click", function () {
    say($("saveMsg"), "Asking the endpoints what they serve…", "info");
    loadModels(true).then(function () { say($("saveMsg"), "", "info"); });
  });

  /* ---------- per-user settings ---------- */
  var policyView = null;

  function polLevel() { return $("polLevel").value; }

  /* At tenant level every knob is settable; at user level only the ones that decide how one
     request's results are selected. The backend is the authority on which -- the page asks rather
     than keeping its own copy of the list. */
  function settableHere(name) {
    var d = policyView;
    if (!d) { return false; }
    if (polLevel() === "tenant") {
      return (d.settable_per_tenant || []).indexOf(name) >= 0;
    }
    return (d.settable_per_user || []).indexOf(name) >= 0;
  }

  function loadPolicy() {
    var user = $("polUser").value.trim();
    if (!$("key").value.trim()) { say($("polMsg"), "Enter an admin key first.", "info"); return; }
    if (polLevel() === "user" && !user) {
      say($("polMsg"), "Name a user, or switch to the whole tenant.", "info");
      return;
    }
    say($("polMsg"), "");
    fetch("/v1/admin/policy?user_id=" + encodeURIComponent(user), { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) { policyView = d; renderPolicy(); })
      .catch(function (e) {
        policyView = null;
        $("policy").innerHTML = '<div class="empty">' +
          esc(typeof e === "number" ? failure(e) : "Could not reach the gateway.") + "</div>";
      });
  }

  function policyControl(name, knob) {
    var id = "pol_" + name;
    if (!settableHere(name)) {
      /* Shown, not hidden: a customer looking for a setting needs to find it and learn where it
         lives, rather than conclude it does not exist. */
      return '<span class="hint" style="margin:0">set it for the whole tenant</span>';
    }
    if (knob.kind === "bool") {
      return "<select id='" + id + "' data-knob='" + esc(name) + "'>" +
        "<option value='0'" + (knob.value ? "" : " selected") + ">off</option>" +
        "<option value='1'" + (knob.value ? " selected" : "") + ">on</option></select>";
    }
    if (knob.kind === "choice") {
      return "<select id='" + id + "' data-knob='" + esc(name) + "'>" +
        (knob.choices || []).map(function (c) {
          return "<option value='" + esc(c) + "'" + (c === knob.value ? " selected" : "") + ">" +
            esc(c) + "</option>";
        }).join("") + "</select>";
    }
    return "<input type='text' id='" + id + "' data-knob='" + esc(name) + "' value='" +
      esc(knob.value) + "' spellcheck='false'>";
  }

  function renderPolicy() {
    var d = policyView;
    if (!d) { return; }
    $("polWhy").hidden = false;
    $("polWhy").textContent = d.why_some_are_tenant_only;
    $("polAux").textContent = d.policy_file
      ? "saved to " + d.policy_file
      : "no policy file configured — changes are lost on restart";
    var query = $("polFilter").value.trim().toLowerCase();
    var onlySettable = $("polOnlySettable").checked;
    var names = Object.keys(d.knobs).sort();
    var rows = names.filter(function (name) {
      var knob = d.knobs[name];
      if (onlySettable && !settableHere(name)) { return false; }
      if (!query) { return true; }
      return (name + " " + knob.description).toLowerCase().indexOf(query) >= 0;
    }).map(function (name) {
      var knob = d.knobs[name];
      return '<div class="polrow' + (settableHere(name) ? "" : " locked") + '">' +
        '<span class="n">' + esc(name) + "</span>" +
        '<span class="src ' + esc(knob.source) + '">' + esc(knob.source) + "</span>" +
        policyControl(name, knob) +
        '<span class="d">' + esc(knob.description) + "</span></div>";
    }).join("");
    /* The name comes from the field, not from the last loaded view: switching level does not
       reload, so the view still remembers whoever was loaded before -- and this is the label on
       the button that commits the change. */
    var typed = $("polUser").value.trim();
    var who = polLevel() === "tenant"
      ? "everyone in " + esc(d.tenant || "this tenant")
      : esc(typed || d.user || "this user");
    $("policy").innerHTML = (rows || '<div class="empty">Nothing matches.</div>') +
      '<div class="actions"><button id="polSave" type="button">Save for ' + who + "</button></div>";
  }

  function savePolicy() {
    var d = policyView;
    if (!d) { return; }
    var settings = {};
    Array.prototype.forEach.call($("policy").querySelectorAll("[data-knob]"), function (el) {
      var name = el.dataset.knob, knob = d.knobs[name];
      var raw = el.value;
      var value = knob.kind === "bool" ? raw === "1"
        : (knob.kind === "int" ? parseInt(raw, 10) : raw);
      if (knob.kind === "int" && isNaN(value)) { return; }
      /* Only what actually changed: sending the whole set would write a user override for every
         knob, and then a later tenant change would never reach them. */
      if (String(value) !== String(knob.value)) { settings[name] = value; }
    });
    if (!Object.keys(settings).length) {
      say($("polMsg"), "Nothing changed.", "info");
      return;
    }
    fetch("/v1/admin/policy", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({ level: polLevel(), user_id: $("polUser").value.trim(),
                             settings: settings })
    })
      .then(function (r) { return r.json().then(function (b) { return { ok: r.ok, body: b }; }); })
      .then(function (res) {
        if (!res.ok) {
          say($("polMsg"), window.__matrixarkWhy(res.body, "Save failed."), "err");
          return;
        }
        policyView = res.body;
        renderPolicy();
        var applied = (res.body.applied || []).length;
        var refused = res.body.refused || [];
        var note = applied + " setting" + (applied === 1 ? "" : "s") + " applied" +
          (res.body.persisted ? " and saved." : ".");
        if (refused.length) {
          note += " Refused for one user, because these decide what is written into the store " +
            "the whole tenant shares — set them for the tenant instead: " +
            refused.join(", ") + ".";
        }
        if (res.body.already_written_note) { note += " " + res.body.already_written_note; }
        if (!res.body.persisted && res.body.persist_note) { note += " " + res.body.persist_note; }
        say($("polMsg"), note, refused.length || !res.body.persisted ? "warn" : "ok");
      })
      .catch(function () { say($("polMsg"), "Could not reach the gateway.", "err"); });
  }

  function polLevelHint() {
    /* The field stays usable at both levels: at tenant level it picks whose effective values are
       shown, which is worth seeing while deciding a change for everyone. */
    $("polUser").placeholder = polLevel() === "tenant"
      ? "alice — optional, to see one person's effective values"
      : "alice";
  }

  $("polLevel").addEventListener("change", function () {
    /* The set of settable knobs changes with the level, so re-render rather than leave controls
       from the other one on screen. */
    polLevelHint();
    if (policyView) { renderPolicy(); }
  });
  $("polUser").addEventListener("input", function () {
    /* Keeps the save button naming the person it will actually save for. */
    if (policyView && polLevel() === "user") { renderPolicy(); }
  });
  polLevelHint();
  $("polLoad").addEventListener("click", loadPolicy);
  $("polUser").addEventListener("keydown", function (ev) {
    if (ev.key === "Enter") { ev.preventDefault(); loadPolicy(); }
  });
  $("polFilter").addEventListener("input", renderPolicy);
  $("polOnlySettable").addEventListener("change", renderPolicy);
  $("policy").addEventListener("click", function (ev) {
    if (ev.target && ev.target.id === "polSave") { savePolicy(); }
  });

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
    /* The save bar lives inside a pane now, so it is invisible from any other tab. Without this
       a field can be edited, a tab switched, and nothing on screen says the change is unsaved. */
    window.markTab("settings", keys.length);
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
    /* Returned so a caller can act on the reloaded settings. The import needs to say what this
       deployment is still missing, and that is only knowable once the answer has come back. */
    return fetch("/v1/admin/config", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        conn("live", "connected");
        loaded = d; edits = {};
        renderGroups(d.settings);
        renderSummary(d);
        renderPresets((d.settings || {}).presets);
        renderHistory(d.settings);
        renderInventory(d.settings);
        renderMonitoring(d);
        loadDeployment();
        loadEncoding();
        loadModels(false);
        startLive();
      })
      .catch(function (e) {
        if (e === 401 || e === 403) {
          conn("live", "connected");
          $("groups").innerHTML = '<section><div class="empty">Enter an admin-scoped key on the Access tab to ' +
            "load and change this deployment’s configuration.</div></section>";
          $("presets").innerHTML = '<div class="empty">Enter an admin key to see the presets.</div>';
          $("models").innerHTML = '<div class="empty">Enter an admin key to choose models.</div>';
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
          say($("saveMsg"), window.__matrixarkWhy(res.body, "Save failed."), "err");
          return;
        }
        var restart = res.body.restart_required || [];
        var one = restart.length === 1;
        /* "live now" is true of the worker that served this request. A live setting is read from
           the environment per call, and only this process's environment was written, so with
           several workers the rest keep the old value until they restart. */
        var workers = res.body.workers || 1;
        var elsewhere = workers > 1
          ? " This worker has it now; the other " + (workers - 1) +
            (workers === 2 ? " picks it up" : " pick it up") + " when they restart."
          : "";
        say($("saveMsg"), restart.length
          ? "Saved. " + restart.length + " setting" + (one ? "" : "s") + " (" +
            restart.join(", ") + ") " + (one ? "is" : "are") + " read once at startup — restart " +
            "the gateway for " + (one ? "it" : "them") + " to take effect. Everything else is " +
            "live now." + elsewhere
          : "Saved and live now." + elsewhere, restart.length || workers > 1 ? "info" : "ok");
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

  /* ---------- deployment chooser ---------- */
  var depCatalogue = null;
  var depPlanDoc = null;

  function depShape() {
    if (!depCatalogue) { return null; }
    var want = $("depShape").value;
    var found = depCatalogue.shapes.filter(function (s) { return s.id === want; });
    return found.length ? found[0] : depCatalogue.shapes[0];
  }

  function renderDeploymentChoices() {
    if (!depCatalogue) { return; }
    $("depShape").innerHTML = depCatalogue.shapes.map(function (s) {
      return "<option value='" + esc(s.id) + "'>" + esc(s.label) + " — " + esc(s.summary) +
        "</option>";
    }).join("");
    $("depShape").value = depCatalogue.default_shape;
    renderShapeDetail();
  }

  function renderShapeDetail() {
    var shape = depShape();
    if (!shape) { return; }
    $("depShapeWhen").textContent = shape.when;
    $("depNodes").value = shape.nodes;
    $("depStorage").innerHTML = shape.storage.map(function (s) {
      /* An option that cannot be honoured is still listed rather than hidden: the customer needs
         to know the choice exists and why this build cannot take it. */
      var suffix = s.available === false ? " (not in this build)" : "";
      return "<option value='" + esc(s.id) + "'>" + esc(s.label) + suffix + "</option>";
    }).join("");
    if (!$("depRoot").value) {
      $("depRoot").value = shape.storage[0].default_root || "";
    }
    $("depSharedField").hidden = shape.id !== "shared";
    renderStorageNote();
  }

  function renderStorageNote() {
    var shape = depShape();
    if (!shape) { return; }
    var want = $("depStorage").value;
    var found = shape.storage.filter(function (s) { return s.id === want; });
    $("depStorageNote").textContent = found.length ? found[0].note : "";
    $("depSharedField").hidden = !(shape.id === "shared" && want === "path");
  }

  function previewPlan() {
    if (!depCatalogue) { return; }
    var keys = $("depKeys").value.split(",").map(function (k) { return k.trim(); })
      .filter(function (k) { return k.length; });
    fetch("/v1/admin/deployment/plan", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({
        shape: $("depShape").value,
        storage: $("depStorage").value,
        nodes: parseInt($("depNodes").value, 10) || 0,
        root: $("depRoot").value,
        shared_dir: $("depShared").value,
        key_envs: keys
      })
    })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (plan) {
        depPlanDoc = plan;
        /* The requested backend and the resolved one are shown together, because they differ
           silently: a shared request with no directory, or MatrixObject on a build without it,
           both fall through to auto-detection and start a deployment nobody chose. */
        var rows = [];
        rows.push("<tr><td>Resolves to</td><td><span class='mono'>" +
          esc(plan.resolved_backend) + "</span><div class='hint' style='margin:2px 0 0'>" +
          esc(plan.backend_reason) + "</div></td></tr>");
        (plan.blocking || []).forEach(function (b) {
          rows.push("<tr><td>Blocked</td><td>" + esc(b) + "</td></tr>");
        });
        (plan.warnings || []).forEach(function (w) {
          rows.push("<tr><td>Warning</td><td>" + esc(w) + "</td></tr>");
        });
        (plan.notes || []).forEach(function (n) {
          rows.push("<tr><td>Note</td><td>" + esc(n) + "</td></tr>");
        });
        $("depVerdict").innerHTML = "<table><tbody>" + rows.join("") + "</tbody></table>";
        $("depEnv").textContent = plan.env_file || "";
        $("depUserData").textContent = plan.cloud_init || "";
        $("depCommands").textContent = plan.commands
          ? (plan.commands.launch + "\n\n" + plan.commands.teardown)
          : "";
        say($("depMsg"), plan.ok
          ? "This plan is launchable."
          : "This plan cannot produce the deployment described. See Blocked above.",
          plan.ok ? "ok" : "err");
      })
      .catch(function (e) {
        say($("depMsg"), typeof e === "number" ? failure(e) : "Could not reach the gateway.",
            "err");
      });
  }

  function loadDeployment() {
    fetch("/v1/admin/deployment", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        depCatalogue = d.catalogue;
        renderDeploymentChoices();
        $("liveShape").textContent = d.live
          ? ("Running now: the " + d.live.backend + " backend (" + d.live.replication +
             "). " + d.live_detail)
          : d.live_detail;
        previewPlan();
      })
      .catch(function (e) {
        if (e !== 401 && e !== 403) {
          say($("depMsg"), "Could not load the deployment shapes.", "err");
        }
      });
  }

  ["depShape", "depStorage", "depNodes", "depRoot", "depShared", "depKeys"].forEach(function (id) {
    $(id).addEventListener("change", function () {
      if (id === "depShape") { renderShapeDetail(); }
      if (id === "depStorage") { renderStorageNote(); }
      previewPlan();
    });
  });

  $("copyUserData").addEventListener("click", function () {
    var text = $("depUserData").textContent;
    window.__matrixarkCopyText(text).then(function (ok) {
      $("copyUserData").textContent = ok ? "copied" : "copy failed";
      setTimeout(function () { $("copyUserData").textContent = "copy user-data"; }, 1400);
    });
  });

  $("copyEnv").addEventListener("click", function () {
    var text = $("depEnv").textContent;
    window.__matrixarkCopyText(text).then(function (ok) {
      $("copyEnv").textContent = ok ? "copied" : "copy failed";
      setTimeout(function () { $("copyEnv").textContent = "copy env file"; }, 1400);
    });
  });

  /* Set once the config lands. Until then only the gateway job is knowable, and printing a
     placeholder engine host would be worse than printing nothing. */
  var monitoringTargets = null;

  function scrapeConfig() {
    var targets = monitoringTargets || {
      gateway: { job: "matrixark_gateway", metrics_path: "/v1/metrics", host: "" }
    };
    return "scrape_configs:\n" + Object.keys(targets).map(function (name) {
      var t = targets[name];
      /* The engine host comes from this deployment's own datanode setting, so the config that is
         copied out resolves instead of carrying a placeholder to be edited by hand. */
      var host = t.host || (name === "gateway" ? location.host : "");
      return (t.note ? "  # " + t.note + "\n" : "") +
        "  - job_name: " + t.job + "\n" +
        "    metrics_path: " + t.metrics_path + "\n" +
        "    static_configs:\n" +
        "      - targets: [" + (host ? "'" + host + "'" : "") + "]" +
        (host ? "" : "  # add each " + name + " process here") + "\n";
    }).join("\n");
  }

  /* One row per asset the gateway actually ships, listed by the server rather than hard-coded
     here, so adding an asset to the registry needs no matching edit on this page -- and so an
     asset a build omits is never offered as a download that 404s. */
  function renderMonitoring(d) {
    var monitoring = (d || {}).monitoring || {};
    var assets = monitoring.assets || [];
    monitoringTargets = monitoring.targets || null;
    $("scrape").textContent = scrapeConfig();
    if (!assets.length) {
      $("monitoring").innerHTML = '<div class="empty">This deployment does not bundle the ' +
        "monitoring assets.</div>";
      return;
    }
    var targets = monitoringTargets || {};
    $("monitoring").innerHTML = "<table><thead><tr><th>Download</th><th>Covers</th>" +
      "<th>Scraped from</th></tr></thead><tbody>" + assets.map(function (a) {
        var target = targets[a.scrape] || {};
        return "<tr><td><button class='link' type='button' data-asset='" + esc(a.asset) +
          "' data-filename='" + esc(a.filename) + "'>" + esc(a.label) + "</button>" +
          "<div class='hint' style='margin:2px 0 0'>" +
          (a.kind === "rules" ? "Prometheus rules" : "Grafana dashboard") + "</div></td>" +
          "<td>" + esc(a.covers) + "</td>" +
          "<td>" + esc(target.label || a.scrape) + "<div class='hint mono' style='margin:2px 0 0'>" +
          esc(target.metrics_path || "") + "</div></td></tr>";
      }).join("") + "</tbody></table>";
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
      window.__matrixarkCopyText(text).then(function (ok) {
        $("copyCfgCurl").textContent = ok ? "copied" : "copy failed";
        setTimeout(function () { $("copyCfgCurl").textContent = "copy as curl"; }, 1400);
      });
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
          say($("importMsg"), window.__matrixarkWhy(res.b, failure(res.s)), "err");
          return;
        }
        var n = Object.keys(settings).length;
        var restart = res.b.restart_required || [];
        var applied = "Applied " + n + " setting" + (n === 1 ? "" : "s") + "." +
          (restart.length
            ? " " + restart.length + (restart.length === 1 ? " of them needs" : " of them need") +
              " a gateway restart."
            : "");
        say($("importMsg"), applied, restart.length ? "info" : "ok");
        $("importText").value = "";
        load().then(function () {
          /* An export never carries secret values, because blanking them at the target would
             clear a working key. The person who made the file was told that; the person applying
             it here was not, and an absent key looks exactly like a present one until an ingest
             quietly falls back. */
          var missing = unsetSecrets();
          if (!missing.length) { return; }
          say($("importMsg"), applied + " " + missing.length +
            (missing.length === 1 ? " secret is" : " secrets are") + " not set on this " +
            "deployment: " + missing.join(", ") + ". An export never carries them.", "info");
        });
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
  /* Served from the process, so what a customer imports is what this build emits. */
  function downloadAsset(name, filename) {
    fetch("/v1/admin/monitoring/" + name, { headers: auth() })
      .then(function (r) { return r.ok ? r.text() : Promise.reject(r.status); })
      .then(function (text) {
        var url = URL.createObjectURL(new Blob([text], { type: "application/octet-stream" }));
        var a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(function () { URL.revokeObjectURL(url); }, 2000);
        say($("grafanaMsg"), filename + " downloaded.", "ok");
      })
      .catch(function (e) {
        say($("grafanaMsg"), e === 404
          ? "This deployment does not bundle the monitoring assets."
          : (typeof e === "number" ? failure(e) : "Could not reach the gateway."), "err");
      });
  }

  /* Delegated, because the rows do not exist until the config lands. */
  $("monitoring").addEventListener("click", function (ev) {
    var node = ev.target;
    while (node && node !== $("monitoring") && !(node.getAttribute && node.getAttribute("data-asset"))) {
      node = node.parentNode;
    }
    if (node && node.getAttribute && node.getAttribute("data-asset")) {
      downloadAsset(node.getAttribute("data-asset"), node.getAttribute("data-filename"));
    }
  });

  $("copyScrape").addEventListener("click", function () {
    var text = scrapeConfig();
    window.__matrixarkCopyText(text).then(function (ok) {
      $("copyScrape").textContent = ok ? "copied" : "copy failed";
      setTimeout(function () { $("copyScrape").textContent = "copy scrape config"; }, 1400);
    });
  });
  /* Traffic and encoding arrive on the stream; the checkbox now pauses the stream rather than a
     timer, which is the same promise to the reader and one connection instead of two pollers. */
  $("auto").addEventListener("change", function () {
    if (!$("auto").checked) {
      if (live) { live.stop(); live = null; }
      conn("live", "paused");
    } else {
      startLive();
    }
  });

  var live = null;

  function startLive() {
    if (!$("key").value.trim() || !$("auto").checked) { return; }
    if (live) { live.restart(); return; }
    live = liveStream({
      headers: auth,
      onState: function (state, seconds) {
        streamLive = (state === "live");
        if (state === "live") { conn("live", "live"); }
        else if (state === "denied") { conn("live", "connected"); }
        else if (state === "retrying") { conn("down", "reconnecting in " + seconds + "s"); }
        if (window.__matrixarkLiveState) {
          window.__matrixarkLiveState(state === "live" ? "live"
            : (state === "retrying" ? "down" : "stale"));
        }
      },
      onFrame: function (frame) {
        /* Somebody else changed this configuration.

           This page is the one holding an editable form, so reloading is not automatically the
           kind thing to do: it would discard whatever the person here has typed and not saved.
           Two operators on the same deployment is exactly when that happens.

           With nothing unsaved, take their change -- the form is showing stale values and there
           is nothing to lose. With unsaved edits, say so and leave the edits alone. Saving from
           here would overwrite what they just set, and the person deserves to know that before
           they press the button rather than after. */
        var configAt = frame.config_changed_at;
        if (configAt != null) {
          if (lastConfigAt !== null && configAt !== lastConfigAt) {
            lastConfigAt = configAt;
            if (Object.keys(pendingPatch()).length) {
              say($("saveMsg"),
                "Someone changed this configuration elsewhere. Your unsaved edits are still here, "
                + "and saving now will overwrite theirs. Discard changes to see what they set.",
                "warn");
            } else if (!document.hidden && $("key").value.trim()) {
              load();
            }
          } else {
            lastConfigAt = configAt;
          }
        }
        renderLiveTraffic(frame.traffic);
        renderFailures((frame.traffic || {}).recent_failures);
        if (frame.embedding) { renderEncoding(frame.embedding); }
        if (window.__matrixarkLiveFrame) { window.__matrixarkLiveFrame(frame); }
      }
    });
  }

  /* Mean AND p95: a mean is the one latency figure that cannot describe the requests people
     complain about -- nineteen at 3 ms and one at nine seconds average to 453 ms, which
     describes neither. The p95 is a bucket edge, so it is shown with a tilde rather than
     claiming a precision the histogram does not have. */
  /* The same table the scrape used to draw, from the frame instead of a /v1/metrics parse. */
  function renderLiveTraffic(traffic) {
    var routes = (traffic || {}).routes || {};
    var names = Object.keys(routes).sort(function (a, b) {
      return routes[b].requests - routes[a].requests;
    });
    if (!names.length) {
      $("traffic").innerHTML = '<div class="empty">No requests recorded yet on this worker.</div>';
      return;
    }
    $("traffic").innerHTML = "<table><thead><tr><th>Route</th><th>Requests</th><th>Answers</th>" +
      "<th>Errors</th><th>Mean</th><th>95% within</th></tr></thead><tbody>" + names.map(function (name) {
        var row = routes[name];
        return "<tr><td><span class='mono'>" + esc(name) + "</span></td><td class='num'>" +
          row.requests + "</td><td>" + statusChips(row.statuses) + "</td><td class='num'>" +
          (row.errors || 0) + "</td><td class='num'>" +
          (row.avg_ms ? row.avg_ms + " ms" : "—") + "</td><td class='num'>" +
          (row.p95_ms == null ? "—" : "~" + row.p95_ms + " ms") + "</td></tr>";
      }).join("") + "</tbody></table>";
  }

  /* What a route actually answered, in order. One "errors" count cannot tell a customer holding the
     wrong key (401) from a backend that fell over (500), and those want different things done. */
  function statusChips(statuses) {
    var codes = Object.keys(statuses || {}).sort(function (a, b) { return Number(a) - Number(b); });
    if (!codes.length) { return "<span class='hint'>—</span>"; }
    return codes.map(function (code) {
      return "<span class='status-chip" + (Number(code) >= 400 ? " bad" : "") + "'>" +
        esc(code) + "×" + esc(String(statuses[code])) + "</span>";
    }).join("");
  }

  /* Seconds, not a clock time: "14s ago" is read at a glance, a timestamp has to be subtracted from
     now in your head first. */
  function agoText(at) {
    var seconds = Math.max(0, Math.round(Date.now() / 1000 - Number(at || 0)));
    if (seconds < 60) { return seconds + "s ago"; }
    if (seconds < 3600) { return Math.round(seconds / 60) + "m ago"; }
    return Math.round(seconds / 3600) + "h ago";
  }

  function renderFailures(list) {
    var rows = list || [];
    if (!rows.length) {
      /* Said plainly: an empty panel and one that never loaded look identical otherwise. */
      $("failures").innerHTML = '<div class="empty">No failures recorded on this worker.</div>';
      return;
    }
    $("failures").innerHTML =
      "<table><thead><tr><th>When</th><th>Status</th><th>Method</th><th>Route</th></tr></thead>" +
      "<tbody>" + rows.map(function (row) {
        return "<tr><td>" + esc(agoText(row.at)) + "</td><td><span class='status-chip bad'>" +
          esc(String(row.status)) + "</span></td><td class='mono'>" + esc(row.method || "") +
          "</td><td class='mono'>" + esc(row.route || "") + "</td></tr>";
      }).join("") + "</tbody></table>";
  }
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
  loadTraffic();  /* one read so the table is populated before the first frame arrives */
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

  <div class="tabs" role="tablist" aria-label="Catalogue">
    <button type="button" role="tab" id="tab-skills" aria-controls="pane-skills"
            data-pane="skills" aria-selected="true">Skills<span class="aux"
            id="skillsCount"></span></button>
    <button type="button" role="tab" id="tab-resources" aria-controls="pane-resources"
            data-pane="resources" aria-selected="false" tabindex="-1">Resources<span class="aux"
            id="resourcesCount"></span></button>
  </div>
  <div id="crossList"></div>

  <section class="pane" role="tabpanel" aria-labelledby="tab-skills" id="pane-skills">
    <div id="skills"><div class="empty">Not loaded.</div></div>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-resources" id="pane-resources"
           hidden>
    <div id="resources"><div class="empty">Not loaded.</div></div>
  </section>

  <section>
    <h2>Stored text <span class="aux" id="contentTitle"></span></h2>
    <p class="hint" style="margin-top:0">Select a row in either list to read what was
      actually stored — the chunks retrieval will draw from, not the source file on disk.</p>
    <pre id="content">Nothing selected.</pre>
  </section>
"""

CATALOG_JS = r"""
<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };

  /* Wired once: these buttons live in the static markup, so unlike the API page they are
     not replaced on every render and their listeners survive. */
  if (window.wireTabs) { window.wireTabs(); }

  document.getElementById("crossList").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-goto]") : null;
    if (!button) { return; }
    if (window.showTab) { window.showTab(button.dataset.goto); }
  });

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
            : window.__matrixarkWhy(res.body, "Could not update the skill."), "err");
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
      /* Counts under the CURRENT query, not the size of each catalogue: they describe what
         clicking would show. */
      $("skillsCount").textContent = " " + skills.length;
      $("resourcesCount").textContent = " " + resources.length;
      /* One filter covers both lists, and only one is visible. With Skills open, a query matching
         only Resources would empty the list and read as "nothing found" while the match sat one
         tab away -- so when the open tab has none and the other has some, say where they are. */
      var openPane = ($("tab-resources").getAttribute("aria-selected") === "true")
        ? "resources" : "skills";
      var here = openPane === "skills" ? skills.length : resources.length;
      var other = openPane === "skills" ? resources.length : skills.length;
      var otherName = openPane === "skills" ? "Resources" : "Skills";
      if (text && !here && other) {
        $("crossList").innerHTML = '<div class="msg info">' + other + " match" +
          (other === 1 ? "" : "es") + " in " + otherName +
          '. <button type="button" class="link" data-goto="' +
          (openPane === "skills" ? "resources" : "skills") + '">Show ' + otherName +
          "</button></div>";
      } else {
        $("crossList").innerHTML = "";
      }
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

  /* Follow the store rather than a clock. Only after the customer has listed once: before that the
     page has no scope to list in, and refreshing something nobody asked for would spend their
     backend on a guess. */
  var listedOnce = false;
  var lastStoredTotal = null;
  var lastAutoAt = 0;

  var originalLoad = load;
  load = function (keepMessage) {
    listedOnce = true;
    return originalLoad(keepMessage);
  };

  /* Through the queue. This script runs before the nav block defines the register function,
     so calling it directly registered nothing at all -- the watcher below never ran. */
  (window.__matrixarkFrameQueue = window.__matrixarkFrameQueue || []).push(
    function (frame) {
      var total = (frame.embedding || {}).total;
      if (total == null) { return; }
      if (lastStoredTotal === null) { lastStoredTotal = total; return; }
      if (total === lastStoredTotal) { return; }
      var grew = total > lastStoredTotal;
      lastStoredTotal = total;
      if (!listedOnce || document.hidden) { return; }
      /* An import lands many records; re-listing on each frame of it would be a request per
         couple of seconds for a list nobody is reading yet. */
      var now = Date.now();
      if (now - lastAutoAt < 4000) { return; }
      lastAutoAt = now;
      load(true);
      say($("listMsg"), grew
        ? "The store changed — this list has been refreshed."
        : "Items were removed from the store — this list has been refreshed.", "info");
    });

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
    <p class="hint">No key yet? <a href="/v1/admin/portal#firstkey">Where the first one comes from</a>
      — no page can mint it; it takes one command where the gateway runs.</p>
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

  <section id="encodingSection" hidden>
    <h2>Encoding <span class="aux" id="encodingAux"></span></h2>
    <div id="encodingBody"></div>
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

  /* Claimed before the strip's own script runs, so it feeds from this connection instead of
     opening a second one to the same endpoint. */
  window.__matrixarkLive = "page";

/* ---------- the live stream ---------- */
/* Read over fetch() rather than with EventSource, which cannot set an Authorization header --
   the alternative is the key in a query string, and a credential in a URL ends up in every access
   log and proxy trace between here and the gateway. Same wire format either way; only the
   reconnect is ours to write. */
function liveStream(options) {
  var onFrame = options.onFrame || function () {};
  var onState = options.onState || function () {};
  var headers = options.headers || function () { return {}; };
  var controller = null, stopped = false, backoff = 1000, buffer = "";

  function schedule() {
    if (stopped) { return; }
    onState("retrying", Math.round(backoff / 1000));
    setTimeout(open, backoff);
    backoff = Math.min(backoff * 2, 30000);  /* a gateway that is down should not be hammered */
  }

  function handle(block) {
    var payload = null;
    block.split("\n").forEach(function (line) {
      if (line.indexOf("data: ") === 0) { payload = line.slice(6); }
    });
    if (!payload) { return; }
    try { onFrame(JSON.parse(payload)); } catch (e) { /* a partial frame; the next one is whole */ }
  }

  function open() {
    if (stopped) { return; }
    controller = new AbortController();
    onState("connecting");
    fetch("/v1/admin/events", { headers: headers(), signal: controller.signal })
      .then(function (response) {
        if (!response.ok) { return Promise.reject(response.status); }
        if (!response.body) { return Promise.reject("nostream"); }
        onState("live");
        backoff = 1000;
        var reader = response.body.getReader();
        var decoder = new TextDecoder();
        function pump() {
          return reader.read().then(function (chunk) {
            if (chunk.done) { schedule(); return; }
            buffer += decoder.decode(chunk.value, { stream: true });
            var blocks = buffer.split("\n\n");
            buffer = blocks.pop();
            blocks.forEach(handle);
            return pump();
          });
        }
        return pump();
      })
      .catch(function (err) {
        if (stopped) { return; }
        if (err === 401 || err === 403) { onState("denied"); return; }
        schedule();
      });
  }

  open();
  window.addEventListener("pagehide", function () {
    stopped = true;
    if (controller) { controller.abort(); }
  });
  return {
    restart: function () {
      if (controller) { controller.abort(); }
      backoff = 1000;
      stopped = false;
      open();
    },
    stop: function () { stopped = true; if (controller) { controller.abort(); } }
  };
}

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
  /* The encoding backlog belongs next to the import strip: both are "the deployment is still
     working on what you gave it", and both are invisible from anywhere else. */
  function renderEncoding(d) {
    var pending = (d && d.pending) || 0;
    var total = (d && d.total) || 0;
    var encoder = (d && d.encoder) || {};
    var section = $("encodingSection");
    if (!section) { return; }
    if (!total || (!pending && encoder.semantic)) { section.hidden = true; return; }
    section.hidden = false;
    var pct = total ? Math.round((((d.encoded || 0) / total) * 100)) : 0;
    $("encodingAux").textContent = pending
      ? pending.toLocaleString() + " waiting"
      : "all encoded";
    $("encodingBody").innerHTML =
      '<div class="importrow"><div class="importhead"><span class="mono">embedding</span>' +
      '<span class="importmeta">' + pct + "% of " + total.toLocaleString() + "</span></div>" +
      '<div class="bar"><i style="width:' + pct + '%"></i></div>' +
      (encoder.semantic
        ? '<div class="hint" style="margin-top:7px">Until a chunk is encoded it cannot be matched ' +
          'on meaning. <a href="/v1/admin/setup">Setup</a> shows the detail.</div>'
        : '<div class="note" style="margin-top:7px">' + esc(encoder.note || "") + "</div>") +
      "</div>";
  }

  function loadEncoding() {
    fetch("/v1/admin/embeddings", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(renderEncoding)
      .catch(function () { /* the checklist already reports an unreachable backend */ });
  }

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
      /* Where the answer came from, next to the answer. "ok" on a configured encoder and "ok" on
         a counted record are different claims: the first survives the encoder being unreachable,
         and that is precisely the failure this page exists to surface. */
      var src = c.source_label
        ? "<span class='hint' style='margin:2px 0 0'>" + esc(c.source_label) + "</span>"
        : "";
      return '<div class="checkrow"><span class="mark ' + esc(c.status) + '">' +
        (MARK[c.status] || "") + '</span><span class="body"><b>' + esc(c.title) +
        "</b><span>" + esc(c.detail) + "</span>" + src + how + "</span>" + go + "</div>";
    }).join("") || '<div class="empty">Nothing to check.</div>';

    var pct = d.total ? Math.round((d.done / d.total) * 100) : 0;
    $("progressBar").style.width = pct + "%";
    $("progressText").textContent = d.done + " of " + d.total + " ready";

    renderImports(d.imports);
    /* Keep up with an import while one is running, and go back to idle pacing when it ends. */
    pace(!!((d.imports || {}).active || []).length);

    renderCounts(d);
  }

  function renderCounts(d) {
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
  var lastConfigChangedAt = null;
  var lastFrame = null;

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
        .catch(function () { return ""; }),
      /* Readiness answers 503 when the backend cannot serve, so a non-2xx here is the answer rather
         than a failure to collect -- the status is recorded next to the body. */
      fetch("/v1/readyz").then(function (r) {
        return r.json().then(function (body) { return { status: r.status, body: body }; });
      }).catch(function () { return null; })
    ]).then(function (parts) {
      var traffic = (lastFrame || {}).traffic || {};
      return JSON.stringify({
        collected_at: new Date().toISOString(),
        origin: location.origin,
        overview: lastReport,
        config: compactConfig(parts[0]),
        metrics: parts[1],
        readiness: parts[2],
        /* The counters in `metrics` say how many requests failed. This says WHEN, which is the
           difference between an incident and a footnote -- and it exists only on the live frame, so
           a bundle assembled from endpoints alone cannot carry it. */
        recent_failures: traffic.recent_failures || null,
        datanode: (lastFrame || {}).datanode || null
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
      .then(function (d) {
        lastReport = d;
        render(d);
        startLive();
      })
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
  /* The strips come from the stream, so the checklist only has to be re-read occasionally: it
     changes when somebody changes a setting, not several times a second. */
  var CHECKLIST_MS = 60000;
  var overviewTimer = setInterval(function () {
    if (document.hidden) { return; }
    if ($("autoOverview").checked && $("key").value.trim()) { load(); }
  }, CHECKLIST_MS);

  function pace(_active) { /* the stream sets the pace now */ }

  var live = null;

  function startLive() {
    if (!$("key").value.trim()) { return; }
    if (live) { live.restart(); return; }
    live = liveStream({
      headers: auth,
      onState: function (state, seconds) {
        if (state === "live") { conn("live", "live"); }
        else if (state === "denied") { conn("live", "connected"); }
        else if (state === "retrying") { conn("down", "reconnecting in " + seconds + "s"); }
        if (window.__matrixarkLiveState) {
          window.__matrixarkLiveState(state === "live" ? "live"
            : (state === "retrying" ? "down" : "stale"));
        }
      },
      onFrame: function (frame) {
        /* A configuration write in another tab, or by another operator, used to take up to a full
           minute to show here -- the checklist rows that exist to say what is misconfigured were the
           ones going stale. The frame carries when the stored configuration was last written, so a
           change is picked up on the next tick instead.

           The first frame only records the value: on load the checklist has just been read, and
           treating the first sighting as a change would re-read it immediately for nothing.

           The slow timer stays. This list counts records and requests as well as reading settings,
           and those move without anyone writing configuration. */
        var seen = frame.config_changed_at;
        if (seen != null) {
          if (lastConfigChangedAt !== null && seen !== lastConfigChangedAt) {
            lastConfigChangedAt = seen;
            if (!document.hidden && $("key").value.trim()) { load(); }
          } else {
            lastConfigChangedAt = seen;
          }
        }
        /* Kept for the diagnostics bundle. The failure timeline exists only on the frame, and
           it answers "when did this start" -- which is the question a bundle is sent to
           answer. */
        lastFrame = frame;
        if (window.__matrixarkLiveFrame) { window.__matrixarkLiveFrame(frame); }
        renderImports(frame.imports);
        if (frame.embedding) { renderEncoding(frame.embedding); }
        if (lastReport) {
          lastReport.imports = frame.imports;
          lastReport.traffic = frame.traffic;
          renderCounts(lastReport);
        }
      }
    });
  }
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden && $("autoOverview").checked && $("key").value.trim()) { load(); }
  });
  $("autoOverview").addEventListener("change", function () {
    if ($("autoOverview").checked) { load(); }
  });
  window.addEventListener("pagehide", function () { clearInterval(overviewTimer); });

  $("copyDiag").addEventListener("click", function () {
    withBundle(function (text) {
      window.__matrixarkCopyText(text).then(function (ok) {
        say($("diagMsg"),
            ok ? "Copied " + text.length + " characters to the clipboard."
               : "Could not copy. Use Download instead, or select the bundle and copy it by hand.",
            ok ? "ok" : "err");
      });
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
    <button type="button" role="tab" id="tab-api" aria-controls="pane-api" data-pane="api" aria-selected="false">mem0 API</button>
    <button type="button" role="tab" id="tab-batch" aria-controls="pane-batch" data-pane="batch" aria-selected="false">Batch ingest</button>
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
    <div id="askServed" class="hint" hidden></div>
    <div id="askWarnings"></div>
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

  <section class="pane" role="tabpanel" aria-labelledby="tab-api" id="pane-api" hidden>
    <h2>Run a mem0 operation</h2>
    <p class="hint" style="margin-top:0">Every method of the mem0 API, listed by the name you call
      it by rather than by the URL that serves it. The request is shown before it is sent and the
      raw answer after, so this doubles as the worked example for writing the call yourself. The
      scope comes from the fields above.</p>
    <div id="ops"></div>
    <div id="opForm"><div class="empty">Choose an operation.</div></div>
    <div id="opMsg" role="status" aria-live="polite"></div>
    <div id="opResult"></div>
  </section>

  <section class="pane" role="tabpanel" aria-labelledby="tab-batch" id="pane-batch" hidden>
    <h2>Batch ingest</h2>
    <p class="hint" style="margin-top:0">Many memories in one submission. Each line is one memory:
      plain text, or a JSON object to set a per-line user, agent, session or metadata. The batch
      runs on the SERVER as a tracked job — closing this tab does not stop it, and its failures are
      retryable from <a href="/v1/admin/ingestion">Ingestion</a> exactly like a directory import.</p>
    <label for="batchText">Records</label>
    <textarea id="batchText" style="min-height:170px" spellcheck="false"
      placeholder='The team agreed to keep the coupon stacking rule for ACME until Q4.
{"text": "Renewal moved to March.", "user_id": "alice", "metadata": {"source": "crm"}}'></textarea>
    <div class="grid2">
      <div>
        <label for="batchFile">…or load a file</label>
        <input id="batchFile" type="file" accept=".txt,.jsonl,.ndjson,.json">
        <div class="hint">Read in this browser and put in the box above, so what will be sent is
          in front of you before you send it.</div>
      </div>
      <div>
        <label for="batchKeyEnv">Server-side key variable</label>
        <input id="batchKeyEnv" type="text" value="MATRIXARK_API_KEY" spellcheck="false">
        <div class="hint">The job runs on the server and authenticates with the key in this
          environment variable. The key in your browser is not handed to the worker.</div>
      </div>
    </div>
    <div class="actions">
      <button id="batchPreview" class="ghost" type="button">Check what would be sent</button>
      <button id="batchRun" type="button">Start batch</button>
      <span class="hint" style="margin:0" id="batchCount">Nothing entered yet.</span>
    </div>
    <div id="batchMsg" role="status" aria-live="polite"></div>
    <div id="batchResult"></div>
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
  window.wireTabs(function (pane) { if (pane === "browse") { loadBrowse(); } });

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

  /* The backend explains a thin answer; this is where a person sees it. Without this the pack
     arrives shorter than it should be with nothing saying why, which reads as bad retrieval -- and
     the fix for bad retrieval is not the fix for a changed encoder. */
  var ASSEMBLY_LABELS = {
    python_local_adapter: "the Python adapter",
    native_rust_proxy: "the native Rust pack",
    native_backend: "the native backend",
    rust_proxy_native_context_pack: "the native Rust pack"
  };

  function renderAskWarnings(d) {
    var conflicts = d.embedding_conflicts || {};
    var blocks = [];

    /* Which implementation answered, and whether it consulted the encoder. Two implementations rank
       this same store differently, and the results alone do not say which one ran. */
    var served = d.served_by || {};
    if (served.assembly) {
      var who = ASSEMBLY_LABELS[served.assembly] || served.assembly;
      var how = served.ranking
        ? " · ranked by " + esc(String(served.ranking).replace(/_/g, " "))
        : "";
      $("askServed").innerHTML = "Answered by " + esc(who) + how;
      $("askServed").hidden = false;
    } else {
      $("askServed").hidden = true;
    }
    if (served.ranking_uses_vectors === false) {
      /* The sentence that matters: an encoder is configured, the encoding panel shows vectors
         stored, and this answer was ordered without reading one of them. */
      /* Kept as one literal. Split across a concatenation the sentence stops being greppable in
         the built page, which is how a test asserting the customer can read it passed on a page
         where they could not. */
      blocks.push('<div class="note"><b>These results were ranked without using the embedding model.</b> ' +
        esc(ASSEMBLY_LABELS[served.assembly] || served.assembly) +
        " orders candidates by how many of your query's words appear in the text, not by meaning. " +
        "Stored vectors are not consulted on this path, so a paraphrase that shares no words with " +
        "the original will not match however well the encoder is configured.</div>");
    }
    if (conflicts.encoder_change) {
      blocks.push('<div class="mwarn"><b>' + esc(conflicts.encoder_change) + " stored memor" +
        (conflicts.encoder_change === 1 ? "y was" : "ies were") + " not searched</b>" +
        "Their vectors were made by a different embedding model than the one in use now" +
        (conflicts.active_embedding_model
          ? " (" + esc(conflicts.active_embedding_model) + ")" : "") +
        ". Vectors from two models cannot be compared, so those memories cannot match any query " +
        "until the store is re-embedded — or until you switch back to the model that wrote them. " +
        'Nothing else reports this: the two are often the same width, so no error is raised.</div>');
    }
    if (conflicts.vector_width) {
      blocks.push('<div class="note"><b>' + esc(conflicts.vector_width) + " stored memor" +
        (conflicts.vector_width === 1 ? "y was" : "ies were") + " not searched.</b> Their vectors " +
        "are a different width from this query's, which is what happens when the encoder was " +
        "unavailable and fallback vectors were written in its place.</div>");
    }
    (d.warnings || []).forEach(function (w) {
      /* The two above say the same thing with more room; do not say it twice. */
      if (/embedded by a different model|different width from this query/.test(String(w))) {
        return;
      }
      blocks.push('<div class="note">' + esc(w) + "</div>");
    });
    $("askWarnings").innerHTML = blocks.join("");
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
        renderAskWarnings(d);
        if (!items.length) {
          /* Two causes look identical from here, and naming only one sends the reader to check a
             setting that is already right. The backend says which it was when it knows. */
          var conflicts = d.embedding_conflicts || {};
          $("pack").innerHTML = '<div class="empty">Nothing matched. ' + (conflicts.encoder_change
            ? "The stored vectors were made by a different embedding model than the one in use " +
              "now, so they cannot be compared with this query — see above."
            : "If the store is not empty, the usual cause is an embedding provider that is still " +
              "deterministic — hash vectors do not match on meaning.") + "</div>";
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


  /* ---------- mem0 console ---------- */
  /* Generated from the gateway's own MEM0_OPERATIONS, so an operation that gains an argument gains
     a field here without anyone remembering to add one. */
  var MEM0_OPS = [
  {
    "id": "add",
    "label": "add()",
    "group": "Write",
    "method": "POST",
    "path": "/v1/ingest",
    "scope": "context:ingest",
    "destructive": false,
    "needs_scope": true,
    "summary": "Write a turn. Acknowledged at 202 with extraction running behind it, which is why a search issued immediately afterwards can legitimately not see it yet.",
    "fields": [
      {
        "name": "content",
        "in": "message",
        "kind": "textarea",
        "required": true,
        "label": "Message",
        "placeholder": "The team agreed to keep the coupon stacking rule for ACME until Q4.",
        "help": "Sent as one user turn."
      },
      {
        "name": "finalize",
        "in": "body",
        "kind": "bool",
        "default": true,
        "label": "Extract before answering",
        "help": "Off is how production behaves: the write is acknowledged and extraction runs in the background."
      },
      {
        "name": "metadata",
        "in": "body",
        "kind": "json",
        "label": "Metadata",
        "placeholder": "{\"source\": \"portal\"}",
        "help": "Stored alongside the memory and returned with it."
      }
    ]
  },
  {
    "id": "search",
    "label": "search()",
    "group": "Read",
    "method": "POST",
    "path": "/v1/retrieve",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": true,
    "summary": "The retrieval path an agent uses: ranked, then packed to fit a token budget.",
    "fields": [
      {
        "name": "query",
        "in": "body",
        "kind": "text",
        "required": true,
        "label": "Query",
        "placeholder": "when do we ship?"
      },
      {
        "name": "max_budget_tokens",
        "in": "body",
        "kind": "number",
        "default": 2048,
        "label": "Token budget",
        "help": "The pack is built to fit this. A budget far below what the answer needs is indistinguishable from poor retrieval."
      }
    ]
  },
  {
    "id": "get_all",
    "label": "get_all()",
    "group": "Read",
    "method": "POST",
    "path": "/v1/memories",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": true,
    "summary": "Every live memory in the scope, unranked. This is the listing, not the search.",
    "fields": [
      {
        "name": "limit",
        "in": "body",
        "kind": "number",
        "default": 50,
        "label": "Limit"
      }
    ]
  },
  {
    "id": "get",
    "label": "get()",
    "group": "Read",
    "method": "GET",
    "path": "/v1/memory/{id}",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": false,
    "summary": "One memory's stored record, by id.",
    "fields": [
      {
        "name": "id",
        "in": "path",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      }
    ]
  },
  {
    "id": "history",
    "label": "history()",
    "group": "Read",
    "method": "GET",
    "path": "/v1/memory/{id}/history",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": false,
    "summary": "How that memory changed: each supersede, with what it said before.",
    "fields": [
      {
        "name": "id",
        "in": "path",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      }
    ]
  },
  {
    "id": "get_by_key",
    "label": "by identity key",
    "group": "Read",
    "method": "GET",
    "path": "/v1/memory/by-key",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": false,
    "summary": "The one live value for an identity key in a scope \u2014 the shape a profile field wants, where the answer is a value and not a ranked list.",
    "fields": [
      {
        "name": "identity_key",
        "in": "query",
        "kind": "text",
        "required": true,
        "label": "Identity key",
        "placeholder": "ship_date"
      },
      {
        "name": "user_id",
        "in": "query",
        "kind": "text",
        "label": "User",
        "from_scope": "user"
      }
    ]
  },
  {
    "id": "users",
    "label": "users()",
    "group": "Read",
    "method": "POST",
    "path": "/v1/users",
    "scope": "context:retrieve",
    "destructive": false,
    "needs_scope": true,
    "summary": "Which users, agents and runs hold memories in this tenant.",
    "fields": []
  },
  {
    "id": "update",
    "label": "update()",
    "group": "Write",
    "method": "POST",
    "path": "/v1/update",
    "scope": "context:ingest",
    "destructive": false,
    "needs_scope": false,
    "summary": "Supersede a memory: the amended text is ingested and the old id tombstoned, so history keeps both.",
    "fields": [
      {
        "name": "memory_id",
        "in": "body",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      },
      {
        "name": "text",
        "in": "body",
        "kind": "textarea",
        "required": true,
        "label": "New text",
        "placeholder": "We ship on Friday."
      }
    ]
  },
  {
    "id": "feedback",
    "label": "feedback()",
    "group": "Write",
    "method": "POST",
    "path": "/v1/memory/feedback",
    "scope": "context:ingest",
    "destructive": false,
    "needs_scope": false,
    "summary": "Rate a retrieved memory. A write about a memory, so it gates like a write.",
    "fields": [
      {
        "name": "memory_id",
        "in": "body",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      },
      {
        "name": "rating",
        "in": "body",
        "kind": "number",
        "default": 1,
        "label": "Rating",
        "help": "1 useful, -1 not."
      }
    ]
  },
  {
    "id": "session_commit",
    "label": "commit a session",
    "group": "Write",
    "method": "POST",
    "path": "/v1/session/commit",
    "scope": "context:ingest",
    "destructive": false,
    "needs_scope": true,
    "summary": "Close a session and roll its turns into summaries. Until this runs the turns are stored but not summarised.",
    "fields": []
  },
  {
    "id": "forget",
    "label": "forget()",
    "group": "Forget",
    "method": "POST",
    "path": "/v1/forget",
    "scope": "context:forget",
    "destructive": true,
    "needs_scope": false,
    "summary": "Stop returning one memory. The record stays, so history still explains it.",
    "fields": [
      {
        "name": "memory_id",
        "in": "body",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      }
    ]
  },
  {
    "id": "delete",
    "label": "delete()",
    "group": "Forget",
    "method": "POST",
    "path": "/v1/delete",
    "scope": "context:forget",
    "destructive": true,
    "needs_scope": false,
    "summary": "Delete one memory outright.",
    "fields": [
      {
        "name": "memory_id",
        "in": "body",
        "kind": "text",
        "required": true,
        "label": "Memory id",
        "placeholder": "mem_7f21"
      }
    ]
  },
  {
    "id": "delete_all",
    "label": "delete_all()",
    "group": "Forget",
    "method": "POST",
    "path": "/v1/reset",
    "scope": "context:forget",
    "destructive": true,
    "needs_scope": true,
    "summary": "Drop everything in the scope shown above. There is no undo, and an empty user field means the default scope, not none of them.",
    "fields": []
  }
];

  var currentOp = null;

  function opGroups() {
    var order = [], byGroup = {};
    MEM0_OPS.forEach(function (op) {
      if (!byGroup[op.group]) { byGroup[op.group] = []; order.push(op.group); }
      byGroup[op.group].push(op);
    });
    return order.map(function (name) { return { name: name, ops: byGroup[name] }; });
  }

  function renderOps() {
    $("ops").innerHTML = opGroups().map(function (group) {
      return '<div class="opgroup">' + esc(group.name) + '</div><div class="opgrid">' +
        group.ops.map(function (op) {
          return '<button type="button" class="opbtn' + (op.destructive ? " danger" : "") +
            '" data-op="' + esc(op.id) + '" aria-pressed="' +
            (currentOp && currentOp.id === op.id ? "true" : "false") + '">' +
            esc(op.label) + "</button>";
        }).join("") + "</div>";
    }).join("");
  }

  function fieldControl(op, f) {
    var id = "op_" + op.id + "_" + f.name;
    var value = f["default"] == null ? "" : String(f["default"]);
    if (f.kind === "bool") {
      return '<label class="check"><input type="checkbox" id="' + id + '" data-f="' +
        esc(f.name) + '"' + (f["default"] ? " checked" : "") + "> " + esc(f.label) + "</label>" +
        (f.help ? '<div class="hint">' + esc(f.help) + "</div>" : "");
    }
    var control;
    if (f.kind === "textarea" || f.kind === "json") {
      control = '<textarea id="' + id + '" data-f="' + esc(f.name) + '" spellcheck="false"' +
        (f.placeholder ? ' placeholder="' + esc(f.placeholder) + '"' : "") + ">" +
        esc(f.kind === "json" ? "" : value) + "</textarea>";
    } else {
      control = '<input type="text" id="' + id + '" data-f="' + esc(f.name) +
        '" spellcheck="false" value="' + esc(value) + '"' +
        (f.placeholder ? ' placeholder="' + esc(f.placeholder) + '"' : "") + ">";
    }
    return '<label for="' + id + '">' + esc(f.label) + (f.required ? " *" : "") + "</label>" +
      control + (f.help ? '<div class="hint">' + esc(f.help) + "</div>" : "");
  }

  function renderOpForm() {
    var op = currentOp;
    if (!op) {
      $("opForm").innerHTML = '<div class="empty">Choose an operation.</div>';
      renderOps();
      return;
    }
    var scopeLine = op.needs_scope
      ? " · scope from the fields above"
      : " · not scoped — it addresses one memory by id";
    $("opForm").innerHTML = '<div class="opform"><h3>' + esc(op.label) + "</h3>" +
      '<div class="route">' + esc(op.method) + " " + esc(op.path) +
      '<span class="scope"> · needs ' + esc(op.scope) + esc(scopeLine) + "</span></div>" +
      '<p class="hint" style="margin-top:0">' + esc(op.summary) + "</p>" +
      op.fields.map(function (f) { return fieldControl(op, f); }).join("") +
      (op.destructive
        ? '<div class="mwarn"><b>This cannot be undone</b>Type <span class="mono">' + esc(op.id) +
          '</span> to confirm, then run.</div><label for="opConfirm">Confirm</label>' +
          '<input id="opConfirm" type="text" spellcheck="false" autocomplete="off">'
        : "") +
      '<div class="actions"><button id="opRun" type="button">Run</button>' +
      '<button id="opCurl" class="ghost" type="button">Copy as curl</button></div>' +
      '<pre id="opPreview"></pre></div>';
    renderOps();
    updatePreview();
  }

  function opFieldValue(op, f) {
    var el = document.getElementById("op_" + op.id + "_" + f.name);
    if (!el) { return null; }
    if (f.kind === "bool") { return el.checked; }
    var raw = el.value.trim();
    if (!raw) { return null; }
    if (f.kind === "number") {
      var n = Number(raw);
      return isNaN(n) ? null : n;
    }
    if (f.kind === "json") {
      try { return JSON.parse(raw); } catch (e) { return { __bad__: raw }; }
    }
    return raw;
  }

  /* One builder for the preview, the curl and the send. Three code paths would let the preview
     drift from what is actually sent -- and the preview is the thing a customer copies into their
     own client, so a preview that lies is worse than none. */
  function buildRequest(op) {
    var body = {}, query = [], path = op.path, messages = [], problems = [];
    if (op.needs_scope) { body.scope = scope(); }
    op.fields.forEach(function (f) {
      var value = opFieldValue(op, f);
      if (value && value.__bad__ !== undefined) {
        problems.push(f.label + " is not valid JSON.");
        return;
      }
      if (f.required && (value === null || value === "")) {
        problems.push(f.label + " is required.");
        return;
      }
      if (value === null) { return; }
      if (f["in"] === "message") { messages.push({ role: "user", content: value }); }
      else if (f["in"] === "body") { body[f.name] = value; }
      else if (f["in"] === "scope") { (body.scope = body.scope || {})[f.name] = value; }
      else if (f["in"] === "query") {
        query.push(encodeURIComponent(f.name) + "=" + encodeURIComponent(value));
      } else if (f["in"] === "path") {
        path = path.replace("{" + f.name + "}", encodeURIComponent(value));
      }
    });
    if (messages.length) { body.messages = messages; }
    if (path.indexOf("{") >= 0) { problems.push("The id is required."); }
    return {
      method: op.method,
      url: path + (query.length ? "?" + query.join("&") : ""),
      problems: problems,
      body: op.method === "GET" ? null : body
    };
  }

  function updatePreview() {
    if (!currentOp) { return; }
    var pre = $("opPreview");
    if (!pre) { return; }
    var request = buildRequest(currentOp);
    pre.textContent = request.method + " " + request.url +
      (request.body ? "\n\n" + JSON.stringify(request.body, null, 2) : "");
  }

  $("ops").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-op]") : null;
    if (!button) { return; }
    currentOp = MEM0_OPS.filter(function (o) { return o.id === button.dataset.op; })[0] || null;
    $("opResult").innerHTML = "";
    say($("opMsg"), "");
    renderOpForm();
  });

  $("opForm").addEventListener("input", updatePreview);
  $("opForm").addEventListener("change", updatePreview);

  $("opForm").addEventListener("click", function (ev) {
    if (!currentOp) { return; }
    if (ev.target.id === "opCurl") {
      var request = buildRequest(currentOp);
      var lines = ["curl -X " + request.method + " " + location.origin + request.url,
                   "  -H 'Authorization: Bearer $MATRIXARK_API_KEY'"];
      if (request.body) {
        lines.push("  -H 'Content-Type: application/json'");
        lines.push("  -d '" + JSON.stringify(request.body) + "'");
      }
      /* The key is named, never pasted: this goes on a clipboard and often into a ticket. */
      var text = lines.join(" \\\n");
      window.__matrixarkCopyText(text).then(function (ok) {
        say($("opMsg"),
            ok ? "Copied. It reads $MATRIXARK_API_KEY from your environment rather than "
                 + "carrying your key."
               : "Could not copy. The command is shown above; select it and copy it by hand.",
            ok ? "ok" : "err");
      });
      return;
    }
    if (ev.target.id === "opRun") { runOp(); }
  });

  function runOp() {
    var op = currentOp;
    if (!$("key").value.trim()) { say($("opMsg"), "Enter an API key first.", "info"); return; }
    var request = buildRequest(op);
    if (request.problems.length) {
      say($("opMsg"), request.problems.join(" "), "warn");
      return;
    }
    if (op.destructive) {
      var confirmEl = $("opConfirm");
      if (!confirmEl || confirmEl.value.trim() !== op.id) {
        say($("opMsg"), "Type " + op.id + " in the confirm box to run this.", "warn");
        return;
      }
    }
    say($("opMsg"), "Running " + op.label + "…", "info");
    var started = Date.now();
    var init = { method: request.method, headers: auth() };
    if (request.body) {
      init.headers = Object.assign({ "Content-Type": "application/json" }, init.headers);
      init.body = JSON.stringify(request.body);
    }
    fetch(request.url, init)
      .then(function (r) {
        return r.text().then(function (text) {
          return { status: r.status, ok: r.ok, text: text };
        });
      })
      .then(function (res) {
        var ms = Date.now() - started;
        var pretty = res.text, parsed = null;
        try { parsed = JSON.parse(res.text); pretty = JSON.stringify(parsed, null, 2); }
        catch (e) { /* leave it raw */ }
        say($("opMsg"), res.ok
          ? op.label + " answered " + res.status + " in " + ms + " ms."
          : op.label + " answered " + res.status + " — " + reason(parsed, res.status),
          res.ok ? "ok" : "err");
        $("opResult").innerHTML = "<pre>" + esc(pretty) + "</pre>";
        /* Clear the confirmation after a destructive op succeeds, or the next click runs it again
           against a box that still says the magic word. */
        if (res.ok && op.destructive && $("opConfirm")) { $("opConfirm").value = ""; }
      })
      .catch(function () { say($("opMsg"), "Could not reach the gateway.", "err"); });
  }

  renderOps();

  /* ---------- batch ingest ---------- */
  /* Parsed here so the count and the unusable lines are visible before anything is submitted, and
     parsed AGAIN on the server, which is the copy that decides. This one is a courtesy. */
  function parseBatch(text) {
    var records = [], problems = [];
    text.split(/\r?\n/).forEach(function (line, index) {
      var trimmed = line.trim();
      if (!trimmed) { return; }
      if (trimmed.charAt(0) === "{") {
        try {
          var parsed = JSON.parse(trimmed);
          if (!parsed.text && !parsed.content) {
            problems.push("Line " + (index + 1) + " is JSON with no text in it.");
            return;
          }
          records.push(parsed);
        } catch (e) {
          problems.push("Line " + (index + 1) + " starts like JSON but does not parse.");
        }
        return;
      }
      records.push({ text: trimmed });
    });
    return { records: records, problems: problems };
  }

  function batchCount() {
    var parsed = parseBatch($("batchText").value);
    $("batchCount").textContent = parsed.records.length
      ? parsed.records.length + " record" + (parsed.records.length === 1 ? "" : "s") +
        (parsed.problems.length
          ? ", " + parsed.problems.length + " line(s) unusable and skipped" : "")
      : "Nothing entered yet.";
    return parsed;
  }

  $("batchText").addEventListener("input", batchCount);

  $("batchFile").addEventListener("change", function () {
    var file = $("batchFile").files && $("batchFile").files[0];
    if (!file) { return; }
    var reader = new FileReader();
    reader.onload = function () {
      $("batchText").value = String(reader.result || "");
      batchCount();
      say($("batchMsg"), "Loaded " + file.name + ". Nothing has been sent yet.", "info");
    };
    reader.onerror = function () { say($("batchMsg"), "Could not read that file.", "err"); };
    reader.readAsText(file);
  });

  function submitBatch(preview) {
    if (!$("key").value.trim()) { say($("batchMsg"), "Enter an API key first.", "info"); return; }
    var parsed = batchCount();
    if (!parsed.records.length) {
      say($("batchMsg"), "There is nothing to send." +
        (parsed.problems.length ? " " + parsed.problems[0] : ""), "warn");
      return;
    }
    var sc = scope();
    fetch("/v1/admin/ingestion/records", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, auth()),
      body: JSON.stringify({
        records: parsed.records,
        preview: !!preview,
        user_id: sc.user_id || "default",
        agent_id: sc.agent_id || "",
        session_id: sc.session_id || "",
        api_key_env: $("batchKeyEnv").value.trim() || "MATRIXARK_API_KEY"
      })
    })
      .then(function (r) {
        return r.json().then(function (b) { return { ok: r.ok, status: r.status, body: b }; });
      })
      .then(function (res) {
        if (!res.ok) {
          say($("batchMsg"), reason(res.body, res.status), "err");
          return;
        }
        if (preview) {
          say($("batchMsg"), "Nothing was sent. " + res.body.total + " record" +
            (res.body.total === 1 ? "" : "s") + " would be ingested as " +
            (res.body.user_id || "default") + ".", "info");
          $("batchResult").innerHTML = "<pre>" +
            esc(JSON.stringify(res.body.sample || [], null, 2)) + "</pre>";
          return;
        }
        say($("batchMsg"), "Started job " + res.body.job_id + " over " + res.body.total +
          " records. It runs on the server — this tab can be closed.", "ok");
        watchBatch(res.body.job_id, res.body.total);
      })
      .catch(function () { say($("batchMsg"), "Could not reach the gateway.", "err"); });
  }

  /* Watched from the shared stream rather than a timer of its own: the job's progress is already
     being pushed to this page for the strip. */
  var watchedJob = null;

  function watchBatch(jobId, total) {
    watchedJob = { id: jobId, total: total, seen: false };
    renderBatchJob({ job_id: jobId, total: total, done: 0, failed: 0, state: "queued" });
  }

  function renderBatchJob(job) {
    var done = (job.done || 0) + (job.failed || 0);
    var total = job.total || 0;
    var pct = total ? Math.round((done / total) * 1000) / 10 : 0;
    var eta = job.eta_s ? " · about " + Math.round(job.eta_s) + "s left" : "";
    $("batchResult").innerHTML =
      '<div class="upl"><div class="upl-head"><span class="upl-name">job ' + esc(job.job_id) +
      '</span><span class="upl-size">' + esc(job.state || "running") + eta + "</span></div>" +
      '<div class="upl-bar"><i style="width:' + pct + '%"></i></div>' +
      '<div class="hint" style="margin-top:6px">' + done + " of " + total + " records" +
      (job.failed ? " · <b>" + job.failed + "</b> failed" : "") +
      (job.current ? " · " + esc(String(job.current).slice(0, 80)) : "") + "</div></div>";
  }

  function finishBatchJob() {
    /* One request when it leaves the running set, rather than polling to find out how it ended.
       The frame says a job is no longer active; it does not say whether it succeeded. */
    fetch("/v1/admin/ingestion/jobs", { headers: auth() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function (d) {
        var job = (d.jobs || []).filter(function (j) {
          return j.job_id === (watchedJob || {}).id;
        })[0];
        watchedJob = null;
        if (!job) { return; }
        renderBatchJob(job);
        var failed = job.failed || 0;
        say($("batchMsg"), failed
          ? job.done + " stored, " + failed + " failed" +
            (job.retryable_failures
              ? " — " + job.retryable_failures + " worth retrying on Ingestion." : ".")
          : "All " + job.done + " records stored.", failed ? "warn" : "ok");
        if (failed) {
          $("batchResult").innerHTML += '<p class="hint">Retry the failures on ' +
            '<a href="/v1/admin/ingestion">Ingestion</a>.</p>';
        }
      })
      .catch(function () { watchedJob = null; });
  }

  /* Through the queue. This script runs before the nav block defines the register function,
     so calling it directly registered nothing at all -- the watcher below never ran. */
  (window.__matrixarkFrameQueue = window.__matrixarkFrameQueue || []).push(
    function (frame) {
      if (!watchedJob) { return; }
      var active = ((frame.imports || {}).active || []).filter(function (j) {
        return j.job_id === watchedJob.id;
      })[0];
      if (active) { watchedJob.seen = true; renderBatchJob(active); return; }
      /* Absent from one frame is not proof it finished: a job can be submitted and not yet appear.
         Only conclude it ended once it has actually been seen running. */
      if (watchedJob.seen) { finishBatchJob(); }
    });

  $("batchPreview").addEventListener("click", function () { submitBatch(true); });
  $("batchRun").addEventListener("click", function () { submitBatch(false); });

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

  <!-- Empty until the catalogue arrives. It carries role="tablist" in the static body because
       the builder injects the shared tab switcher only into pages that declare one, and a strip
       built after a fetch would not be there to see. -->
  <div class="tabs" role="tablist" aria-label="API groups" id="routeTabs"></div>
  <div id="crossGroup"></div>
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
      '<span class="route-live" data-metric="' + esc(route.metric || "") + '"' +
      (route.metric_shared_with
        ? ' title="Counted together with ' + esc(route.metric_shared_with.join(", ")) + '"'
        : "") + "></span>" +
      '<span class="route-scope">' + esc(route.scope || "no auth") + "</span></div>" +
      "<p>" + esc(route.summary) +
      (route.needs ? " Needs " + esc(route.needs) + " configured." : "") + "</p>" +
      '<button type="button" class="link curl" data-curl="' + index + '">show curl</button>' +
      "<pre>" + esc(curlFor(route)) + "</pre></div>";
  }

  /* The last traffic seen, so a re-render (filtering, mostly) can repaint rather than blank the
     counters until the next frame two seconds later. */
  var lastTraffic = null;

  /* This page defines esc() and not n(). Reaching for the strip's helper compiled fine, threw a
     ReferenceError on the first frame, and the strip catches each watcher's error so that one
     panel cannot break the rest -- so it failed in total silence. */
  function num(value) {
    return Number(value || 0).toLocaleString();
  }

  function paintTraffic(traffic) {
    lastTraffic = traffic || lastTraffic;
    var routes = (lastTraffic || {}).routes || {};
    Array.prototype.forEach.call(document.querySelectorAll(".route-live"), function (cell) {
      var label = cell.getAttribute("data-metric");
      if (!label) { cell.textContent = ""; return; }
      var seen = routes[label];
      if (!seen) {
        /* The frame arrived and this route is not in it, which means nothing has been recorded
           against it -- that is a fact, not a gap. Before any frame arrives this function has not
           run at all, so the cell stays empty rather than claiming a zero nobody reported. */
        cell.className = "route-live quiet";
        cell.textContent = "no requests yet";
        return;
      }
      var bits = [num(seen.requests) + (Number(seen.requests) === 1 ? " request" : " requests")];
      if (seen.errors) {
        bits.push(num(seen.errors) + (Number(seen.errors) === 1 ? " error" : " errors"));
      }
      if (seen.avg_ms) { bits.push(seen.avg_ms + " ms avg"); }
      cell.className = "route-live" + (seen.errors ? " bad" : "");
      cell.textContent = bits.join(" \u00b7 ");
    });
  }

  /* Through the queue: this script runs before the nav block defines the register function. */
  (window.__matrixarkFrameQueue = window.__matrixarkFrameQueue || []).push(
    function (frame) { paintTraffic((frame || {}).traffic); });

  /* Which group is showing. Kept across re-renders: the list is rebuilt on every keystroke in
     the filter, and losing the open tab each time would make the strip unusable. */
  var activeGroup = "";

  function paneId(group) {
    /* A pane id the switcher can find. Group names have spaces ("Portal pages"), and the switcher
       matches on `pane-` + the button's data-pane, so both sides use the same slug. */
    return String(group).toLowerCase().replace(/[^a-z0-9]+/g, "-");
  }

  function matches(route, query) {
    return !query || (route.method + " " + route.path + " " + (route.scope || "") + " " +
      route.summary).toLowerCase().indexOf(query) >= 0;
  }

  function render() {
    var query = $("filter").value.trim().toLowerCase();
    var groups = {};
    ROUTES.forEach(function (route, index) {
      (groups[route.group] = groups[route.group] || []).push([route, index]);
    });
    var order = Object.keys(groups);
    if (!order.length) {
      $("routeTabs").innerHTML = "";
      $("crossGroup").innerHTML = "";
      $("routes").innerHTML = '<section><div class="empty">No routes to show.</div></section>';
      return;
    }
    if (order.indexOf(activeGroup) < 0) { activeGroup = order[0]; }

    /* Matches per group under the CURRENT query, so the counts on the tabs describe what a click
       would actually show rather than the size of the group. */
    var hits = {};
    var total = 0;
    order.forEach(function (group) {
      hits[group] = groups[group].filter(function (pair) { return matches(pair[0], query); });
      total += hits[group].length;
    });

    $("routeTabs").innerHTML = order.map(function (group) {
      var n = hits[group].length;
      return '<button type="button" role="tab" id="tab-' + esc(paneId(group)) +
        '" aria-controls="pane-' + esc(paneId(group)) + '" data-pane="' + esc(paneId(group)) +
        '" aria-selected="' + (group === activeGroup ? "true" : "false") + '"' +
        (group === activeGroup ? "" : ' tabindex="-1"') + '>' + esc(group) +
        "<span class='aux'> " + n + "</span></button>";
    }).join("");

    $("routes").innerHTML = order.map(function (group) {
      var rows = hits[group];
      var body = rows.length
        ? rows.map(function (pair) { return routeHtml(pair[0], pair[1]); }).join("")
        : '<div class="empty">' + (query ? "Nothing in this group matches that."
                                         : "No routes in this group.") + "</div>";
      return '<section class="pane" role="tabpanel" aria-labelledby="tab-' + esc(paneId(group)) +
        '" id="pane-' + esc(paneId(group)) + '"' + (group === activeGroup ? "" : " hidden") + ">" +
        body + "</section>";
    }).join("");

    /* The filter searches every group. Restricting it to the open tab would hide matches in the
       other five and read as "nothing matches that", which is the worst answer to give somebody
       searching a reference -- so when the open tab has none and others do, say where they are. */
    var elsewhere = total - hits[activeGroup].length;
    if (query && !hits[activeGroup].length && elsewhere > 0) {
      var first = order.filter(function (g) { return hits[g].length; })[0];
      $("crossGroup").innerHTML = '<div class="msg info">' + elsewhere + " match" +
        (elsewhere === 1 ? "" : "es") + " in other groups. " +
        '<button type="button" class="link" data-goto="' + esc(paneId(first)) + '">Show ' +
        esc(first) + "</button></div>";
    } else {
      $("crossGroup").innerHTML = "";
    }

    /* Called after every render: the buttons are new elements each time, so the listeners the
       switcher attached to the old ones went with them. */
    if (window.wireTabs) {
      window.wireTabs(function (pane) {
        /* The switcher calls this once while wiring, for the tab that is already selected. Without
           this guard that call re-renders, which re-wires, which calls it again -- the page died
           of a stack overflow on load. Only an actual change to another group redraws. */
        var next = null;
        for (var i = 0; i < order.length; i++) {
          if (paneId(order[i]) === pane) { next = order[i]; break; }
        }
        if (next === null || next === activeGroup) { return; }
        activeGroup = next;
        render();
      });
    }
  }

  $("crossGroup").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-goto]") : null;
    if (!button) { return; }
    for (var i = 0; i < ROUTES.length; i++) {
      if (paneId(ROUTES[i].group) === button.dataset.goto) { activeGroup = ROUTES[i].group; break; }
    }
    render();
  });

  /* A re-render replaces every cell, so the counters have to go back on. Without this, filtering
     blanks them until the next frame. */
  var renderRoutes = render;
  render = function () { renderRoutes(); if (lastTraffic) { paintTraffic(null); } };

  $("routes").addEventListener("click", function (ev) {
    var button = ev.target.closest ? ev.target.closest("[data-curl]") : null;
    if (!button) { return; }
    var route = button.closest(".route");
    var open = route.classList.toggle("open");
    button.textContent = open ? "hide curl" : "show curl";
    if (open) {
      window.__matrixarkCopyText(curlFor(ROUTES[parseInt(button.dataset.curl, 10)]))
        .then(function (ok) {
          /* Only while the route is still open: the reader may have closed it again, and
             relabelling a closed toggle "copied" describes something not on screen. */
          if (!route.classList.contains("open")) { return; }
          button.textContent = ok ? "hide curl — copied" : "hide curl — copy failed";
        });
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
%(navjs)s
</body>
</html>
"""


def emit(filename, title, body, js, active):
    css = BASE_CSS + NAV_CSS + EXTRA_CSS
    rendered = body % {"nav": nav(active)}
    # Only pages that declare a tablist carry the helper, so a page without tabs is byte for byte
    # what it was and a page that grows tabs picks it up without anyone remembering to wire it.
    page_js = js.strip()
    if 'role="tablist"' in rendered:
        page_js = TABS_JS.strip() + "\n" + page_js
    html = (HEAD % {"title": title, "css": css}
            + rendered
            + (TAIL % {"js": page_js, "navjs": NAV_JS.strip()}))
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
TABS_JS_MARKER = "/* Tabs, for every portal page whose body declares a tablist."
NAV_JS_MARKER = "/* The shared live strip."


def _with_nav_js(text):
    """Put the shared strip script in, replacing any copy already there.

    Replaced rather than appended: the builder runs repeatedly over these two files, and a script
    that stacks would open one stream per build.
    """
    if NAV_JS_MARKER in text:
        marker = text.index(NAV_JS_MARKER)
        start = text.rindex("<script>", 0, marker)
        end = text.index("</script>", start) + len("</script>")
        return text[:start] + NAV_JS.strip() + text[end:]
    if "</body>" not in text:
        print("no </body> to place the live strip script before")
        sys.exit(1)
    return text.replace("</body>", NAV_JS.strip() + "\n</body>", 1)


def _with_tabs_js(text):
    """Put the shared tab helper into a hand-maintained page that declares a tablist.

    Removed and re-placed rather than replaced where it sits. The builder runs over these
    files repeatedly, and an earlier build put the block in the wrong place -- after the
    page's own script, so `window.wireTabs()` ran before anything defined it. Replacing in
    place would carry that position forward for ever; taking it out and putting it back means
    the current rule about where it goes always wins.

    A page with no tablist is left exactly as it was.
    """
    if 'role="tablist"' not in text:
        return text
    if TABS_JS_MARKER in text:
        start = text.index("<script>", text.index(TABS_JS_MARKER) - 200)
        end = text.index("</script>", start) + len("</script>")
        text = text[:start] + text[end:].lstrip(chr(10))
    # Scripts run in document order, so the helper goes before the page's own.
    if "<script>" not in text:
        print("no <script> to place the tab helper before")
        sys.exit(1)
    at = text.index("<script>")
    return text[:at] + TABS_JS.strip() + chr(10) + text[at:]

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
        rendered = _with_tabs_js(_with_nav_js(replaced))
        io.open(path, "w", encoding="utf-8", newline="\n").write(rendered)
        print("nav refreshed in %s" % filename)
        return
    if anchor not in text:
        print("ANCHOR MISSING in %s" % filename)
        sys.exit(1)
    text = text.replace("</style>", NAV_CSS + "</style>", 1)
    text = text.replace(anchor, anchor + "\n" + nav(active), 1)
    text = _with_tabs_js(_with_nav_js(text))
    io.open(path, "w", encoding="utf-8", newline="\n").write(text)
    print("nav added to %s" % filename)


inject("ingestion_portal.html",
       '''    <span class="conn" role="status" aria-live="polite"><span id="dot" class="dot"></span><span id="conn">connecting…</span></span>
  </header>''', "/v1/admin/ingestion")
inject("api_key_portal.html", "<main class=\"wrap\">", "/v1/admin/portal")
