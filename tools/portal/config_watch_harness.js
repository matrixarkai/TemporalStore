/* Does the ingestion panel notice the configuration changing under it?
 *
 * That panel renders what an import will actually use -- embedding provider and model, the encoder
 * endpoint, whether a failed encoder call errors or falls back, the extraction provider and key --
 * and all of it is set on a different page. It read that once, on load, and went on describing the
 * old deployment for as long as the tab stayed open, under a header saying "checked 14:32:01":
 * true about when it looked, false about what it is showing.
 *
 * Whether a watcher fires is behaviour, and the ways it goes wrong are all silent: registering
 * against a function the nav has not defined yet, firing on the first frame, firing on every frame,
 * firing for a hidden tab. So the page's own registration is executed and the callback it hands
 * over is driven with frames.
 *
 * Usage: node config_watch_harness.js <ingestion_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* The registration statement as the page ships it, including the `var lastConfigAt` it closes
   over. Sliced rather than reimplemented: the point is to run what is there. */
const start = page.indexOf("  var lastConfigAt = null;");
const end = page.indexOf("});", page.indexOf("(window.__matrixarkFrameQueue", start)) + 3;
ok("the page registers a config watcher", start !== -1 && end > start);
if (start === -1) { console.log("FAILED " + failures); process.exit(1); }
const src = page.slice(start, end);

ok("it registers through the queue, not by calling the register function",
   src.includes("window.__matrixarkFrameQueue = window.__matrixarkFrameQueue || []"),
   "this script runs before the nav defines that function; a direct call registers nothing");

function run() {
  const reloads = [];
  const hidden = { value: false };
  const key = { value: "k-test" };
  const win = {};
  const env = {
    window: win,
    document: { get hidden() { return hidden.value; } },
    $: (id) => (id === "key" ? key : { value: "" }),
    loadConfig(changedElsewhere) { reloads.push(!!changedElsewhere); },
  };
  new Function(...Object.keys(env), src)(...Object.values(env));
  const queue = win.__matrixarkFrameQueue;
  const callback = queue && queue[0];
  return { callback, reloads, hidden, key };
}

const first = run();
ok("the watcher is on the queue", typeof first.callback === "function");
if (typeof first.callback !== "function") { console.log("FAILED " + failures); process.exit(1); }

/* ---------- the first frame is not a change ---------- */
first.callback({ config_changed_at: 100 });
ok("the first frame does not trigger a re-read", first.reloads.length === 0,
   "the configuration was read a moment ago on load");

/* ---------- an unchanged frame is not a change ---------- */
first.callback({ config_changed_at: 100 });
ok("an unchanged value does not trigger one either", first.reloads.length === 0);

/* ---------- a change is ---------- */
first.callback({ config_changed_at: 200 });
ok("a changed value re-reads the configuration", first.reloads.length === 1,
   String(first.reloads.length));
ok("and the re-read is marked as caused by a change elsewhere", first.reloads[0] === true,
   "the header would say 'checked', which is true about when it looked and false about what "
   + "it is showing");

/* ---------- and only once per change ---------- */
first.callback({ config_changed_at: 200 });
ok("the same value again does not re-read", first.reloads.length === 1,
   String(first.reloads.length));

/* ---------- a frame without the field ---------- */
first.callback({});
ok("a frame carrying no configuration stamp is ignored", first.reloads.length === 1);

/* ---------- a hidden tab ---------- */
const hiddenRun = run();
hiddenRun.callback({ config_changed_at: 1 });
hiddenRun.hidden.value = true;
hiddenRun.callback({ config_changed_at: 2 });
ok("a hidden tab does not re-read", hiddenRun.reloads.length === 0,
   "nobody is looking at the table it would refresh");
hiddenRun.hidden.value = false;
hiddenRun.callback({ config_changed_at: 3 });
ok("and it re-reads once the tab is looked at again", hiddenRun.reloads.length === 1,
   "the change while hidden would otherwise never be picked up");

/* ---------- no key ---------- */
const noKey = run();
noKey.key.value = "";
noKey.callback({ config_changed_at: 1 });
noKey.callback({ config_changed_at: 2 });
ok("with no key it does not ask", noKey.reloads.length === 0,
   "the read would be refused and the panel already says to enter one");

console.log(failures === 0 ? "PASS" : "FAILED " + failures);
process.exit(failures === 0 ? 0 : 1);
