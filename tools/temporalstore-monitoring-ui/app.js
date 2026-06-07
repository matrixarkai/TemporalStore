const defaultHealth = {
  updatedAt: "2026-06-07T01:20:00Z",
  prometheusUrl: "../temporalstore-vars.prom",
  topology: [
    { role: "Metaserver, proxy, UI", instance: "i-05f55360d92c43908", privateAddress: "10.70.1.161", purpose: "Control plane, client/test runner, static UI", status: "running" },
    { role: "TemporalStore data01", instance: "i-0cfbef56e86551535", privateAddress: "10.70.1.214", purpose: "Primary/replica data service on port 17001", status: "running" },
    { role: "TemporalStore data02", instance: "i-04c93ad8271e5b64a", privateAddress: "10.70.1.24", purpose: "Primary/replica data service on port 17002", status: "running" }
  ],
  products: [
    {
      id: "temporalstore",
      name: "TemporalStore",
      href: "./temporalstore.html",
      status: "Live metrics wired",
      summary: "Online temporal feature engine for high-cardinality windows, filtered event state, long sequence features, and persisted serving.",
      tags: ["temporal windows", "risk features", "sequence state", "Prometheus live"]
    },
    {
      id: "matrixdb",
      name: "MatrixDB",
      href: "./matrixdb.html",
      status: "Observability page ready",
      summary: "Multi-tenant KV/profile engine for large online profile objects, hot-key serving, cache-plus-storage, and Redis-compatible access.",
      tags: ["multi-tenant", "profile KV", "Redis path", "cache storage"]
    },
    {
      id: "matrixkv",
      name: "MatrixKV",
      href: "./matrixkv.html",
      status: "Observability page ready",
      summary: "Transactional distributed KV service with timestamp coordination, partitioned storage, strong consistency, and release deployment paths.",
      tags: ["transaction KV", "TSO", "strong consistency", "partitioned"]
    }
  ],
  temporalStore: {
    latestRun: {
      name: "AWS 30-minute service smoke",
      runDir: "/var/lib/temporalstore/scale30_20260607T003847Z",
      iterations: 1525,
      duration: "30 minutes",
      table: "aws30m_primary/temporalagg",
      notes: "Core modules stayed stable; TemporalAggregate writes currently fail with a response-size check and need follow-up debugging."
    },
    modules: [
      { name: "STRING", result: "1525 pass", state: "ok" },
      { name: "COMMON", result: "1525 pass", state: "ok" },
      { name: "HASH", result: "1525 pass", state: "ok" },
      { name: "SET", result: "1525 pass", state: "ok" },
      { name: "FEATURE", result: "1525 pass", state: "ok" },
      { name: "IPS", result: "1525 pass", state: "ok" },
      { name: "RISK", result: "1525 pass", state: "ok" },
      { name: "TEMPORAL_AGGREGATE", result: "blocked by response-size check", state: "bad" }
    ],
    issues: [
      "TemporalAggregate secondary replay/read visibility must be fixed before aggregate secondary reads are trusted.",
      "Two-replica table creation currently hits Missing condition info in one path.",
      "Prometheus scrape is live for TemporalStore; MatrixDB and MatrixKV need their own target registration."
    ],
    kpis: [
      { label: "Topology", value: "1 meta + 2 data", hint: "Current AWS reuse cluster" },
      { label: "Stable modules", value: "7 of 8", hint: "Latest 30-minute service smoke" },
      { label: "Prometheus sources", value: "3", hint: "metaserver, data01, data02" },
      { label: "Exporter interval", value: "5s", hint: "vars-to-Prometheus bridge" }
    ]
  },
  productDetails: {
    matrixdb: [
      { title: "Multi-tenant control", body: "Designed around tenant and namespace isolation so profile serving can scale without mixing ownership or quota signals." },
      { title: "Large online profiles", body: "Targets profile objects, hot keys, and cache-plus-storage access patterns where simple in-memory-only caching becomes expensive." },
      { title: "Protocol flexibility", body: "The observation page tracks Redis-compatible and direct service paths separately so migration issues are visible." },
      { title: "Placement health", body: "Shard placement, rebalance state, failover, and storage movement are the first-class runtime signals to wire." }
    ],
    matrixkv: [
      { title: "Strong consistency path", body: "Tracks transaction commits, timestamp allocation, partition state, and leader health for correctness-first KV workloads." },
      { title: "Partitioned serving", body: "Observability should separate master, timestamp service, proxy, and partition-server signals." },
      { title: "Release packaging", body: "Client and server packages should expose dependency loading and direct SDK/proxy readiness." },
      { title: "Scale tests", body: "Read/write latency, retry pressure, and CPU should be collected under fixed concurrency levels for each node type." }
    ]
  }
};

let health = defaultHealth;

async function loadHealth() {
  try {
    const res = await fetch("./health.json", { cache: "no-store" });
    if (res.ok) health = await res.json();
  } catch {
    health = defaultHealth;
  }
}

function setActiveNav() {
  const page = document.body.dataset.page;
  document.querySelectorAll("[data-nav]").forEach((link) => {
    link.classList.toggle("active", link.dataset.nav === page);
  });
}

function stateClass(state) {
  if (state === "ok" || state === "running") return "state-ok";
  if (state === "bad" || state === "blocked") return "state-bad";
  return "state-warn";
}

function renderOverview() {
  const grid = document.querySelector("[data-product-grid]");
  if (grid) {
    grid.innerHTML = health.products.map((product) => `
      <a class="product-card" href="${product.href}">
        <div>
          <p class="eyebrow">${product.status}</p>
          <h2>${product.name}</h2>
          <p>${product.summary}</p>
        </div>
        <div class="tag-row">${product.tags.map((tag) => `<span class="tag">${tag}</span>`).join("")}</div>
      </a>
    `).join("");
  }

  const topology = document.querySelector("[data-topology-table]");
  if (topology) {
    topology.innerHTML = health.topology.map((node) => `
      <tr>
        <td>${node.role}</td>
        <td>${node.instance}</td>
        <td>${node.privateAddress}</td>
        <td>${node.purpose}</td>
        <td class="${stateClass(node.status)}">${node.status}</td>
      </tr>
    `).join("");
  }

  const run = document.querySelector("[data-temporal-run]");
  if (run) {
    const latest = health.temporalStore.latestRun;
    run.innerHTML = [
      ["Duration", latest.duration],
      ["Iterations", latest.iterations.toLocaleString()],
      ["Table", latest.table],
      ["Run directory", latest.runDir],
      ["Important note", latest.notes]
    ].map(([k, v]) => `<div class="metric-row"><span>${k}</span><strong>${v}</strong></div>`).join("");
  }
}

function renderTemporalStore() {
  const kpis = document.querySelector("[data-temporal-kpis]");
  if (kpis) {
    kpis.innerHTML = health.temporalStore.kpis.map((item) => `
      <div class="kpi"><strong>${item.value}</strong><span>${item.label}. ${item.hint}</span></div>
    `).join("");
  }

  const modules = document.querySelector("[data-module-grid]");
  if (modules) {
    modules.innerHTML = health.temporalStore.modules.map((item) => `
      <div class="module">
        <strong>${item.name}</strong>
        <span class="${stateClass(item.state)}">${item.result}</span>
      </div>
    `).join("");
  }

  const issues = document.querySelector("[data-temporal-issues]");
  if (issues) {
    issues.innerHTML = health.temporalStore.issues.map((issue) => `<li>${issue}</li>`).join("");
  }
}

function renderProductDetails() {
  document.querySelectorAll("[data-product-detail]").forEach((el) => {
    const id = el.dataset.productDetail;
    const items = health.productDetails[id] || [];
    el.innerHTML = items.map((item) => `
      <div class="detail-tile"><strong>${item.title}</strong><span>${item.body}</span></div>
    `).join("");
  });
}

function parsePrometheus(text) {
  const samples = {};
  const seriesBySource = {};
  text.split(/\n/).forEach((line) => {
    if (!line || line.startsWith("#")) return;
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(.+)$/);
    if (!match) return;
    const name = match[1];
    const labels = match[3] || "";
    const value = Number.parseFloat(match[4]);
    const sourceMatch = labels.match(/source="([^"]+)"/);
    const source = sourceMatch ? sourceMatch[1] : "unlabeled";
    seriesBySource[source] = (seriesBySource[source] || 0) + 1;
    if (["process_cpu_usage", "bthread_worker_usage", "bthread_count"].includes(name)) {
      samples[`${name}:${source}`] = Number.isFinite(value) ? value : match[4];
    }
  });
  return { samples, seriesBySource };
}

async function loadPrometheus() {
  const url = health.prometheusUrl || "../temporalstore-vars.prom";
  const status = document.querySelector("[data-prom-status]");
  const summary = document.querySelector("[data-prom-summary]");
  const dot = document.querySelector("[data-live-dot]");
  try {
    const res = await fetch(url, { cache: "no-store" });
    const text = await res.text();
    if (!res.ok || !text.trim()) throw new Error("empty metrics");
    const parsed = parsePrometheus(text);
    const sources = Object.keys(parsed.seriesBySource);
    if (status) status.textContent = "Prometheus endpoint is live";
    if (summary) summary.textContent = `${sources.length} sources, ${text.split(/\n/).length.toLocaleString()} metric lines`;
    if (dot) dot.classList.add("ok");
    renderPrometheus(parsed);
  } catch {
    if (status) status.textContent = "Prometheus endpoint unavailable";
    if (summary) summary.textContent = "The static pages still render with saved health data.";
    if (dot) dot.classList.add("bad");
    renderPrometheus({ samples: {}, seriesBySource: {} });
  }
}

function formatNumber(value) {
  if (typeof value !== "number") return value ?? "n/a";
  if (Math.abs(value) >= 100) return value.toFixed(0);
  if (Math.abs(value) >= 10) return value.toFixed(2);
  return value.toFixed(3);
}

function renderPrometheus(parsed) {
  const grid = document.querySelector("[data-prom-grid]");
  if (grid) {
    const sourceCards = Object.entries(parsed.seriesBySource).map(([source, count]) => ({
      title: source,
      value: `${count.toLocaleString()} series`,
      body: `CPU ${formatNumber(parsed.samples[`process_cpu_usage:${source}`])}, workers ${formatNumber(parsed.samples[`bthread_worker_usage:${source}`])}`
    }));
    grid.innerHTML = (sourceCards.length ? sourceCards : [{ title: "No live scrape", value: "unavailable", body: "Check exporter process and nginx route." }]).map((item) => `
      <div class="prom-card"><strong>${item.title}</strong><span>${item.value}<br>${item.body}</span></div>
    `).join("");
  }

  const nodeTable = document.querySelector("[data-temporal-node-table]");
  if (nodeTable) {
    const rows = health.topology.map((node) => {
      const source = node.role.includes("data01") ? "data01" : node.role.includes("data02") ? "data02" : "metaserver";
      return `
        <tr>
          <td>${node.role}</td>
          <td>${node.privateAddress}</td>
          <td>${parsed.seriesBySource[source] || "n/a"}</td>
          <td>${formatNumber(parsed.samples[`process_cpu_usage:${source}`])}</td>
          <td>${formatNumber(parsed.samples[`bthread_worker_usage:${source}`])}</td>
        </tr>
      `;
    });
    nodeTable.innerHTML = rows.join("");
  }
}

async function main() {
  setActiveNav();
  await loadHealth();
  renderOverview();
  renderTemporalStore();
  renderProductDetails();
  await loadPrometheus();
  document.querySelector("[data-refresh-prom]")?.addEventListener("click", loadPrometheus);
}

main();
