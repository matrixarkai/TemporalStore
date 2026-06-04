const healthIds = ["metaserver", "proxy", "exporter", "data_nodes", "efs", "blockcache"];

const fallbackHealth = {
  cluster: {
    name: "aws-scale",
    status: "pending",
    environment: "AWS test cluster",
    metaservers: 1,
    data_nodes: 2,
  },
  health: {
    metaserver: { status: "pending", detail: "waiting for live health.json" },
    proxy: { status: "pending", detail: "waiting for live health.json" },
    exporter: { status: "pending", detail: "waiting for live health.json" },
    data_nodes: { status: "pending", detail: "primary and secondary expected" },
    efs: { status: "pending", detail: "shared file store" },
    blockcache: { status: "pending", detail: "DRAM + SSD cache" },
  },
  runtime_config: {
    storage_zone_size: "256 MB",
    stream_max_blob_size: "256 MB",
    storage_oplog_delay_dump_length: "0",
    replicator_loop_interval_us: "1000",
    replicator_max_oplog_per_loop: "20000",
    replicator_update_remote_interval_ms: "20",
    blockcache_dram_capacity: "64 MB",
    blockcache_ssd_capacity: "2 GB",
  },
  nodes: [
    {
      name: "meta-01",
      role: "metaserver + proxy + UI",
      status: "pending",
      endpoint: "10.70.1.88:17000",
      cpu: "-",
      memory: "-",
      storage: "EFS mounted",
      replay: "n/a",
    },
    {
      name: "data-01",
      role: "primary",
      status: "pending",
      endpoint: "10.70.1.139:17001",
      cpu: "-",
      memory: "-",
      storage: "EFS + blockcache",
      replay: "writer",
    },
    {
      name: "data-02",
      role: "secondary",
      status: "pending",
      endpoint: "10.70.1.195:17001",
      cpu: "-",
      memory: "-",
      storage: "EFS + blockcache",
      replay: "waiting",
    },
  ],
  replication: {
    mode: "shared file store + primary-pull fallback",
    secondary_lag_ms: "-",
    replay_source: "EFS or primary stream",
    visibility: "pending",
  },
  scale_tests: [
    {
      name: "TemporalAggregate 10k features",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "feature x bucket increments with window query",
    },
    {
      name: "STRING primary",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "plain SET/GET baseline",
    },
    {
      name: "Sequence feature",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "long behavior sequence window scan",
    },
  ],
  module_tests: [
    {
      module: "TemporalAggregate",
      test: "high-cardinality window aggregate",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary and replica-eligible",
      latency: "-",
      notes: "count/sum over event buckets",
    },
    {
      module: "Feature",
      test: "module ingest/query",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary",
      latency: "-",
      notes: "profile-like feature object",
    },
    {
      module: "IPS",
      test: "risk/frequency cap sample",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary",
      latency: "-",
      notes: "slot/table/action dimensions",
    },
    {
      module: "STRING",
      test: "SET/GET baseline",
      status: "pending",
      write_path: "direct SDK and proxy",
      read_path: "primary and replica-eligible",
      latency: "-",
      notes: "plain KV baseline",
    },
  ],
  diagnostics: {
    last_result_dir: "-",
    release_build: "pending",
    proxy_sdk: "pending",
    direct_sdk: "pending",
  },
};

function byId(id) {
  return document.getElementById(id);
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function text(value, fallback = "-") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function statusClass(status) {
  const normalized = text(status, "unknown").toLowerCase();
  if (["ok", "pass", "passed", "healthy", "ready", "running"].includes(normalized)) {
    return "ok";
  }
  if (["fail", "failed", "bad", "error", "unhealthy", "down"].includes(normalized)) {
    return "bad";
  }
  return "warn";
}

function setText(id, value) {
  const el = byId(id);
  if (el) {
    el.textContent = text(value);
  }
}

function badge(status) {
  const cls = statusClass(status);
  return `<span class="badge ${cls}">${escapeHtml(text(status, "unknown"))}</span>`;
}

function escapeHtml(value) {
  return text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderMetricList(id, rows) {
  const el = byId(id);
  el.innerHTML = rows
    .map(
      (row) => `
        <div class="metric-row">
          <span>${escapeHtml(row.label)}</span>
          <strong>${row.html || escapeHtml(row.value)}</strong>
        </div>
      `,
    )
    .join("");
}

function renderNodes(nodes) {
  byId("nodes-body").innerHTML = asArray(nodes)
    .map(
      (node) => `
        <tr>
          <td><strong>${escapeHtml(node.name)}</strong></td>
          <td>${escapeHtml(node.role)}</td>
          <td>${badge(node.status)}</td>
          <td>${escapeHtml(node.endpoint)}</td>
          <td>${escapeHtml(node.cpu)}</td>
          <td>${escapeHtml(node.memory)}</td>
          <td>${escapeHtml(node.storage)}</td>
          <td>${escapeHtml(node.replay)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderScaleTests(tests) {
  byId("scale-tests").innerHTML = asArray(tests)
    .map(
      (test) => `
        <article class="test-card">
          <div class="test-title">
            <strong>${escapeHtml(test.name)}</strong>
            ${badge(test.status)}
          </div>
          <p>${escapeHtml(test.workload)}</p>
          <div class="mini-grid">
            <span>Write QPS<strong>${escapeHtml(test.write_qps)}</strong></span>
            <span>Read p50<strong>${escapeHtml(test.read_p50_ms)}</strong></span>
            <span>Read p99<strong>${escapeHtml(test.read_p99_ms)}</strong></span>
            <span>Replica lag<strong>${escapeHtml(test.secondary_lag_ms)}</strong></span>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderModules(modules) {
  byId("modules-body").innerHTML = asArray(modules)
    .map(
      (item) => `
        <tr>
          <td><strong>${escapeHtml(item.module)}</strong></td>
          <td>${escapeHtml(item.test)}</td>
          <td>${badge(item.status)}</td>
          <td>${escapeHtml(item.write_path)}</td>
          <td>${escapeHtml(item.read_path)}</td>
          <td>${escapeHtml(item.latency)}</td>
          <td>${escapeHtml(item.notes)}</td>
        </tr>
      `,
    )
    .join("");
}

function normalizeHealth(payload) {
  if (payload.health || payload.cluster || payload.nodes) {
    return { ...fallbackHealth, ...payload };
  }
  return {
    ...fallbackHealth,
    health: {
      ...fallbackHealth.health,
      metaserver: payload.metaserver || fallbackHealth.health.metaserver,
      proxy: payload.proxy || fallbackHealth.health.proxy,
      exporter: payload.exporter || fallbackHealth.health.exporter,
    },
  };
}

function render(data) {
  const cluster = data.cluster || {};
  const health = data.health || {};
  const config = data.runtime_config || {};
  const replication = data.replication || {};
  const diagnostics = data.diagnostics || {};
  const healthyCount = healthIds.filter((id) => statusClass(health[id]?.status) === "ok").length;

  byId("cluster-status").className = `status-pill ${statusClass(cluster.status)}`;
  byId("cluster-status").innerHTML = `<span class="dot"></span>${escapeHtml(cluster.environment || cluster.name || "cluster")}`;

  setText("summary-topology", `${text(cluster.metaservers, 1)} meta / ${text(cluster.data_nodes, 2)} data`);
  setText("summary-topology-note", text(cluster.name, "cluster"));
  setText("summary-write-qps", data.scale_tests?.[0]?.write_qps || "-");
  setText("summary-lag", text(replication.secondary_lag_ms));
  setText("summary-lag-note", text(replication.mode, "replication"));
  setText("summary-cache", text(config.blockcache_ssd_capacity || "configured"));
  setText("summary-cache-note", `DRAM ${text(config.blockcache_dram_capacity)}`);

  renderNodes(data.nodes);
  renderScaleTests(data.scale_tests);
  renderModules(data.module_tests);

  renderMetricList(
    "health-list",
    healthIds.map((id) => ({
      label: id.replaceAll("_", " "),
      html: `${badge(health[id]?.status)} <small>${escapeHtml(health[id]?.detail || "")}</small>`,
    })),
  );

  renderMetricList(
    "config-list",
    Object.entries(config).map(([key, value]) => ({
      label: key.replaceAll("_", " "),
      value,
    })),
  );

  renderMetricList("replication-list", [
    { label: "Mode", value: replication.mode },
    { label: "Replay source", value: replication.replay_source },
    { label: "Secondary lag", value: replication.secondary_lag_ms },
    { label: "Visibility", value: replication.visibility },
  ]);

  renderMetricList("diagnostics-list", [
    { label: "Healthy checks", value: `${healthyCount}/${healthIds.length}` },
    { label: "Last result", value: diagnostics.last_result_dir },
    { label: "Release build", value: diagnostics.release_build },
    { label: "Direct SDK", value: diagnostics.direct_sdk },
    { label: "Proxy SDK", value: diagnostics.proxy_sdk },
  ]);

  setText("last-refresh", `refreshed ${new Date().toLocaleString()}`);
}

async function refreshHealth() {
  try {
    const res = await fetch(`/health.json?ts=${Date.now()}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    render(normalizeHealth(await res.json()));
  } catch (err) {
    render(fallbackHealth);
    setText("last-refresh", `offline sample ${new Date().toLocaleString()}`);
  }
}

byId("refresh").addEventListener("click", refreshHealth);
refreshHealth();
setInterval(refreshHealth, 15000);
