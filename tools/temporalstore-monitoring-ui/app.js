const healthIds = {
  metaserver: "metaserver-health",
  proxy: "proxy-health",
  exporter: "exporter-health",
};

function setValue(id, value, cls) {
  const el = document.getElementById(id);
  el.textContent = value;
  el.className = `value ${cls || ""}`.trim();
}

async function refreshHealth() {
  try {
    const res = await fetch(`/health.json?ts=${Date.now()}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const health = await res.json();
    for (const [name, id] of Object.entries(healthIds)) {
      const item = health[name] || {};
      setValue(id, item.status || "unknown", item.status === "ok" ? "ok" : "warn");
    }
    document.getElementById("last-refresh").textContent = new Date().toLocaleString();
  } catch (err) {
    for (const id of Object.values(healthIds)) {
      setValue(id, "unavailable", "bad");
    }
    document.getElementById("last-refresh").textContent = new Date().toLocaleString();
  }
}

document.getElementById("refresh").addEventListener("click", refreshHealth);
refreshHealth();
setInterval(refreshHealth, 15000);
