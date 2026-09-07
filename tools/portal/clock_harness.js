// Drives the SHIPPED live-stream parser with a real SSE block, then asks the SHIPPED agoText and
// clockNoteHtml what they say. Setting the clock variables directly would test my arithmetic, not
// the code that reads `ts` off a frame -- which is the part that did not exist.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

function slice(startMarker) {
  const start = page.indexOf(startMarker);
  if (start < 0) throw new Error("not found: " + startMarker);
  let depth = 0, i = page.indexOf("{", start);
  for (; i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + startMarker);
}

// The clock block is plain statements, not a function: take it up to `function liveStream`.
const clockStart = page.indexOf("var liveServerMs = 0;");
const clockBlock = page.slice(clockStart, page.indexOf("function liveStream(options)"));

const parts = [
  clockBlock,
  slice("function liveStream(options)"),
  "var CLOCK_SKEW_NOTICE_MS = " +
    /var CLOCK_SKEW_NOTICE_MS = (\d+);/.exec(page)[1] + ";",
  slice("function clockNoteHtml()"),
  slice("  function agoText(at)"),
  slice("  function renderFailures(list)"),
];

// A browser whose clock is `input.browserOffsetMs` away from real time.
const REAL_NOW = 1_760_000_000_000;
const browserNow = () => REAL_NOW + (input.browserOffsetMs || 0);

const frames = input.frames || [];
const sse = frames.map((f) => "event: status\ndata: " + JSON.stringify(f) + "\n\n").join("");

const nodes = {};
const scope = {
  Date: { now: browserNow },
  fetch: () => Promise.resolve({
    ok: true,
    body: {
      getReader: () => {
        let sent = false;
        return {
          read: () => Promise.resolve(sent
            ? { done: true }
            : ((sent = true), { done: false, value: sse })),
        };
      },
    },
  }),
  TextDecoder: function () { this.decode = (v) => (v === undefined ? "" : String(v)); },
  AbortController: function () { this.abort = () => {}; this.signal = null; },
  document: { hidden: false, addEventListener: () => {} },
  $: (id) => (nodes[id] = nodes[id] || { innerHTML: "" }),
  window: { addEventListener: () => {} },
  // A no-op: the shipped code reconnects when the reader finishes, and a real timer here turns
  // that into a hot loop. The reconnect is not what this harness is measuring.
  setTimeout: () => 0,
  esc: (s) => String(s == null ? "" : s),
};

const names = Object.keys(scope);
const api = new Function(...names, parts.join("\n") + `
  return { liveStream: liveStream, agoText: agoText, clockNoteHtml: clockNoteHtml,
           serverNowMs: serverNowMs, clockSkewMs: clockSkewMs,
           sinceLastFrameMs: sinceLastFrameMs, renderFailures: renderFailures };
`)(...names.map((k) => scope[k]));

const before = { ago: api.agoText(input.at), skew: api.clockSkewMs() };
api.liveStream({ onFrame: () => {}, onState: () => {}, headers: () => ({}) });

setTimeout(() => {
  process.stdout.write(JSON.stringify({
    before,
    after: {
      ago: api.agoText(input.at),
      skewMs: api.clockSkewMs(),
      serverNowMs: api.serverNowMs(),
      note: api.clockNoteHtml(),
      panel: (api.renderFailures(input.failures || []),
              (nodes.failures || { innerHTML: "" }).innerHTML),
      emptyPanel: (api.renderFailures([]),
                   (nodes.failures || { innerHTML: "" }).innerHTML),
    },
  }));
}, 10);
