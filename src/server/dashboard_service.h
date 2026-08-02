#pragma once

#include <byte/include/macros.h>
#include <protocol/server.pb.h>

#include <string>
#include <utility>

#include "brpc/builtin/common.h"
#include "brpc/builtin/tabbed.h"
#include "brpc/closure_guard.h"
#ifndef __CYGWIN__
#include "brpc/details/tcmalloc_extension.h"
#endif

namespace bcache2 {
namespace server {

class DashboardServiceImpl : public DashboardService, public brpc::Tabbed {
 public:
    DashboardServiceImpl(std::string cluster_name, int port)
        : cluster_name_(std::move(cluster_name)), port_(port) {}
    ~DashboardServiceImpl() override = default;

    void GetTabInfo(brpc::TabInfoList* info_list) const override {
        brpc::TabInfo* info = info_list->add();
        info->tab_name = "monitoring";
        info->path = "/DashboardService/GetInfo";
    }

    void GetInfo(::google::protobuf::RpcController* controller,
                 const ::bcache2::EmptyMessage* request, ::bcache2::EmptyMessage* response,
                 ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* ctrl = static_cast<brpc::Controller*>(controller);
        butil::IOBufBuilder os;

        os << R"TSUI(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TemporalStore Ops Console</title>
<style>
:root {
  --bg: #f6f8fb;
  --panel: #ffffff;
  --ink: #17202a;
  --muted: #667085;
  --line: #d9e0ea;
  --blue: #2f6fed;
  --green: #168a5b;
  --amber: #b7791f;
  --red: #c2413b;
  --cyan: #087f95;
  --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
}
button, input, select {
  font: inherit;
}
.shell {
  min-height: 100vh;
  display: grid;
  grid-template-columns: 248px 1fr;
}
.rail {
  background: #101828;
  color: #f8fafc;
  padding: 22px 18px;
  position: sticky;
  top: 0;
  height: 100vh;
}
.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 24px;
}
.mark {
  width: 28px;
  height: 28px;
  border-radius: 6px;
  background: linear-gradient(135deg, #4f8cff, #20a06b);
}
.brand strong { display: block; font-size: 15px; }
.brand span { display: block; color: #b8c2d6; font-size: 12px; margin-top: 2px; }
.nav { display: grid; gap: 6px; }
.nav button {
  width: 100%;
  border: 0;
  border-radius: 7px;
  background: transparent;
  color: #d7deeb;
  text-align: left;
  padding: 10px 12px;
  cursor: pointer;
}
.nav button.active, .nav button:hover { background: #243147; color: white; }
.rail-footer {
  position: absolute;
  bottom: 18px;
  left: 18px;
  right: 18px;
  color: #b8c2d6;
  font-size: 12px;
  line-height: 1.5;
}
.main {
  padding: 24px;
}
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}
h1 {
  font-size: 22px;
  margin: 0 0 5px;
  letter-spacing: 0;
}
.subtle { color: var(--muted); }
.actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.button {
  border: 1px solid var(--line);
  background: var(--panel);
  color: var(--ink);
  border-radius: 7px;
  padding: 8px 11px;
  cursor: pointer;
  text-decoration: none;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 36px;
}
.button.primary { background: var(--blue); border-color: var(--blue); color: white; }
.button:hover { filter: brightness(0.98); }
.grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.metric {
  padding: 15px;
  min-height: 122px;
}
.label {
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.value {
  font-size: 28px;
  line-height: 1.1;
  margin-top: 10px;
  font-weight: 700;
  word-break: break-word;
}
.hint { margin-top: 8px; color: var(--muted); font-size: 12px; }
.status-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--green);
  margin-right: 6px;
}
.status-dot.warn { background: var(--amber); }
.status-dot.bad { background: var(--red); }
.section { display: none; }
.section.active { display: block; }
.section-stack {
  display: grid;
  gap: 14px;
}
.row {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(320px, 0.75fr);
  gap: 12px;
  margin-top: 12px;
}
.panel-header {
  padding: 14px 16px;
  border-bottom: 1px solid var(--line);
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
}
.panel-header h2 {
  margin: 0;
  font-size: 15px;
  letter-spacing: 0;
}
.panel-body { padding: 14px 16px; }
.toolbar {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
}
.input {
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 8px 10px;
  min-height: 36px;
  background: white;
  min-width: 230px;
}
.chips { display: flex; gap: 7px; flex-wrap: wrap; }
.chip {
  border: 1px solid var(--line);
  background: white;
  border-radius: 999px;
  padding: 6px 10px;
  color: var(--muted);
  cursor: pointer;
}
.chip.active { color: white; border-color: var(--blue); background: var(--blue); }
table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
th, td {
  text-align: left;
  padding: 9px 8px;
  border-bottom: 1px solid #edf0f5;
  vertical-align: top;
}
th { color: var(--muted); font-size: 12px; font-weight: 600; }
td:first-child, th:first-child { width: 58%; }
td:nth-child(2), th:nth-child(2) { width: 20%; }
td { word-break: break-word; }
.tag {
  display: inline-block;
  border-radius: 5px;
  padding: 3px 7px;
  background: #eef4ff;
  color: #2158c9;
  font-size: 12px;
}
.alert-list { display: grid; gap: 8px; }
.alert {
  border: 1px solid var(--line);
  border-left: 4px solid var(--green);
  border-radius: 7px;
  padding: 10px;
  background: #fbfcfe;
}
.alert.warn { border-left-color: var(--amber); }
.alert.bad { border-left-color: var(--red); }
.alert strong { display: block; margin-bottom: 4px; }
.chart-wrap {
  height: 260px;
}
canvas {
  width: 100%;
  height: 240px;
  display: block;
}
.diag-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-top: 12px;
}
.diag-card {
  padding: 15px;
  min-height: 122px;
}
.diag-card h3 { margin: 0 0 8px; font-size: 14px; }
.diag-card p { margin: 0 0 12px; color: var(--muted); line-height: 1.45; }
pre {
  margin: 0;
  background: #101828;
  color: #e6edf6;
  border-radius: 7px;
  padding: 12px;
  overflow: auto;
  max-height: 520px;
  line-height: 1.45;
}
.empty {
  color: var(--muted);
  padding: 24px;
  text-align: center;
}

/* Operator-console polish layer. Kept separate so the dashboard remains easy to tune. */
:root {
  --bg: #f3f6fa;
  --panel: #ffffff;
  --ink: #121926;
  --muted: #697586;
  --line: #dde5f0;
  --line-soft: #eef2f7;
  --rail: #111827;
  --rail-2: #1f2937;
  --blue: #2764d8;
  --green: #13845b;
  --amber: #b76e00;
  --red: #b8322b;
  --cyan: #087a8f;
  --shadow: 0 8px 22px rgba(16, 24, 40, 0.06);
}
body {
  background:
    linear-gradient(180deg, #f7f9fc 0, #f3f6fa 340px),
    var(--bg);
  -webkit-font-smoothing: antialiased;
}
.shell { grid-template-columns: 260px minmax(0, 1fr); }
.rail {
  background: linear-gradient(180deg, #101828 0%, #141c2b 100%);
  border-right: 1px solid rgba(255,255,255,0.08);
  padding: 20px 16px;
}
.mark {
  border-radius: 7px;
  background:
    linear-gradient(90deg, #4f8cff 0 45%, transparent 45%),
    linear-gradient(180deg, #20a06b 0 50%, #f2b84b 50%);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,0.18);
}
.nav { gap: 4px; }
.nav button {
  position: relative;
  min-height: 38px;
  padding: 9px 12px 9px 34px;
  color: #c9d3e5;
}
.nav button::before {
  content: "";
  position: absolute;
  left: 13px;
  top: 50%;
  width: 7px;
  height: 7px;
  border-radius: 2px;
  background: #667085;
  transform: translateY(-50%);
}
.nav button.active {
  background: rgba(255,255,255,0.09);
  box-shadow: inset 3px 0 0 #66d19e;
}
.nav button.active::before { background: #66d19e; }
.rail-footer {
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.09);
  border-radius: 8px;
  padding: 10px;
}
.main {
  max-width: 1480px;
  width: 100%;
  margin: 0 auto;
  padding: 22px 24px 34px;
}
.topbar {
  align-items: flex-start;
  border-bottom: 1px solid var(--line);
  padding-bottom: 16px;
}
.topbar h1 { font-size: 24px; }
.subtle strong { color: var(--ink); font-weight: 650; }
.status-strip {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 9px;
}
.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 28px;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 4px 10px;
  background: #fff;
  color: var(--muted);
  font-size: 12px;
}
.instance-hero {
  margin-bottom: 14px;
}
.instance-top {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(360px, 0.8fr);
  gap: 14px;
}
.instance-summary {
  padding: 16px;
}
.instance-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 14px;
}
.instance-title h2 {
  margin: 0;
  font-size: 18px;
}
.state-badge {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 1px solid #bfe6d3;
  background: #eefaf4;
  color: var(--green);
  border-radius: 999px;
  padding: 5px 10px;
  font-weight: 750;
  font-size: 12px;
}
.state-badge.warn {
  border-color: #f1d09b;
  background: #fff7e8;
  color: var(--amber);
}
.state-badge.bad {
  border-color: #efb7b3;
  background: #fff0ee;
  color: var(--red);
}
.state-badge::before {
  content: "";
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: currentColor;
}
.detail-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}
.detail-item {
  border: 1px solid var(--line);
  border-radius: 7px;
  background: #fbfcfe;
  padding: 10px;
}
.detail-label {
  color: var(--muted);
  font-size: 12px;
  margin-bottom: 5px;
}
.detail-value {
  font-weight: 750;
  word-break: break-word;
}
.page-state-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}
.page-state {
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 10px;
  background: #fff;
}
.page-state.active {
  border-color: #bfe6d3;
  background: #f5fbf8;
}
.page-state.warn {
  border-color: #f1d09b;
  background: #fffaf0;
}
.page-state strong {
  display: block;
  font-size: 13px;
  margin-bottom: 5px;
}
.page-state span {
  display: block;
  color: var(--muted);
  line-height: 1.35;
}
.pulse {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 0 4px rgba(19,132,91,0.12);
}
.actions { justify-content: flex-end; }
.button {
  border-color: #cfd8e6;
  font-weight: 650;
  min-height: 34px;
  padding: 7px 11px;
  box-shadow: 0 1px 2px rgba(16,24,40,0.04);
}
.button.primary {
  background: #245fd3;
  border-color: #245fd3;
}
.button::before {
  content: "";
  width: 8px;
  height: 8px;
  border-radius: 2px;
  background: currentColor;
  opacity: 0.65;
}
.nav button:focus-visible,
.button:focus-visible,
.chip:focus-visible,
.input:focus-visible {
  outline: 2px solid rgba(47,111,237,0.35);
  outline-offset: 2px;
}
.panel { overflow: hidden; }
.grid { gap: 14px; }
.metric {
  position: relative;
  min-height: 116px;
  padding: 16px;
}
.metric::before {
  content: "";
  position: absolute;
  left: 0;
  right: 0;
  top: 0;
  height: 3px;
  background: var(--blue);
}
.metric:nth-child(1)::before { background: var(--green); }
.metric:nth-child(3)::before { background: var(--amber); }
.metric:nth-child(4)::before { background: var(--cyan); }
.label {
  letter-spacing: 0.03em;
  font-weight: 750;
}
.value {
  font-size: 25px;
  margin-top: 9px;
  font-variant-numeric: tabular-nums;
}
.hint {
  color: #7a8699;
  line-height: 1.35;
}
.row { gap: 14px; margin-top: 14px; }
.panel-header {
  min-height: 52px;
  background: #fbfcfe;
  padding: 12px 15px;
}
.panel-header h2 {
  font-size: 14px;
  font-weight: 750;
}
.panel-body { padding: 13px 15px 15px; }
.toolbar { justify-content: flex-end; }
.input {
  min-height: 34px;
  border-color: #cfd8e6;
  background: #fbfcfe;
}
.chips {
  margin-bottom: 10px;
  gap: 6px;
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 30px;
  border-radius: 7px;
  padding: 5px 9px;
  font-weight: 650;
}
.chip span {
  border-radius: 999px;
  background: #eef2f7;
  padding: 1px 6px;
  color: #4b5565;
  font-size: 11px;
}
.chip.active span {
  background: rgba(255,255,255,0.22);
  color: #fff;
}
table { font-size: 13px; }
thead th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: #f8fafc;
  border-bottom: 1px solid var(--line);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
tbody tr:hover { background: #f8fbff; }
th, td {
  padding: 8px 9px;
  border-bottom: 1px solid var(--line-soft);
}
.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line-soft);
  border-radius: 7px;
}
.table-wrap table {
  min-width: 680px;
}
.table-wrap th:first-child,
.table-wrap td:first-child {
  padding-left: 11px;
}
.table-wrap th:last-child,
.table-wrap td:last-child {
  padding-right: 11px;
}
.metric-name {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  color: #344054;
}
.metric-value {
  font-variant-numeric: tabular-nums;
  font-weight: 650;
}
.tag { background: #eef4ff; color: #245fd3; }
.tag-request { background: #eaf2ff; color: #245fd3; }
.tag-latency { background: #fff4e5; color: #a15c00; }
.tag-storage { background: #edf7f3; color: #0f7650; }
.tag-cache { background: #e8f6f8; color: #087a8f; }
.tag-replication { background: #f1edff; color: #6046b6; }
.tag-error { background: #fdeceb; color: #b8322b; }
.alert {
  background: #fff;
  border-color: var(--line);
  padding: 11px 12px;
}
.alert span {
  color: var(--muted);
  line-height: 1.42;
}
.diag-grid { gap: 14px; }
.diag-card {
  min-height: 112px;
  padding: 14px;
}
.diag-card h3 { font-size: 14px; }
.diag-card p { min-height: 40px; }
.runbook-list {
  display: grid;
  gap: 8px;
}
.runbook-item {
  display: grid;
  grid-template-columns: 126px 1fr;
  gap: 10px;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 10px;
  background: #fff;
}
.runbook-item strong { font-size: 13px; }
.runbook-item span { color: var(--muted); line-height: 1.4; }
.parameter-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.parameter-row {
  border: 1px solid var(--line);
  border-radius: 7px;
  background: #fff;
  padding: 10px;
}
.parameter-row strong {
  display: block;
  font-size: 13px;
  margin-bottom: 5px;
}
.parameter-row span {
  color: var(--muted);
}
.test-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}
.test-card {
  display: grid;
  gap: 10px;
  min-height: 150px;
  padding: 14px;
}
.test-card h3 {
  margin: 0;
  font-size: 14px;
}
.test-card p {
  margin: 0;
  color: var(--muted);
  line-height: 1.45;
}
.endpoint-list {
  display: grid;
  gap: 8px;
}
.endpoint-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 9px 10px;
  background: #fff;
}
.endpoint-row code {
  color: #344054;
}
.node-layout {
  display: grid;
  grid-template-columns: minmax(0, 0.95fr) minmax(420px, 1.05fr);
  gap: 14px;
}
.node-card {
  padding: 16px;
}
.node-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 14px;
}
.node-title h2 {
  margin: 0;
  font-size: 18px;
}
.node-title p {
  margin: 4px 0 0;
  color: var(--muted);
  line-height: 1.45;
}
.node-kv {
  display: grid;
  gap: 9px;
}
.node-kv-row {
  display: grid;
  grid-template-columns: 120px minmax(0, 1fr);
  gap: 10px;
  border: 1px solid var(--line);
  border-radius: 7px;
  background: #fff;
  padding: 9px 10px;
}
.node-kv-row span {
  color: var(--muted);
}
.node-kv-row strong {
  min-width: 0;
  word-break: break-word;
}
.node-signal-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}
.node-signal {
  border: 1px solid var(--line);
  border-radius: 7px;
  background: #fff;
  padding: 11px;
  min-height: 92px;
}
.node-signal span {
  display: block;
  color: var(--muted);
  font-size: 12px;
  margin-bottom: 7px;
}
.node-signal strong {
  display: block;
  font-size: 20px;
  line-height: 1.1;
  font-variant-numeric: tabular-nums;
  word-break: break-word;
}
.node-signal small {
  display: block;
  color: var(--muted);
  margin-top: 6px;
  line-height: 1.35;
}
pre {
  font-size: 12px;
  background: #0f172a;
}
@media (max-width: 1100px) {
  .shell {
    display: block;
  }
  .rail {
    position: sticky;
    top: 0;
    z-index: 20;
    height: auto;
    display: grid;
    grid-template-columns: auto minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    padding: 12px 14px;
  }
  .brand {
    margin-bottom: 0;
  }
  .nav {
    display: flex;
    gap: 6px;
    overflow-x: auto;
    padding-bottom: 2px;
    scrollbar-width: none;
  }
  .nav::-webkit-scrollbar {
    display: none;
  }
  .nav button {
    width: auto;
    flex: 0 0 auto;
    min-height: 34px;
    white-space: nowrap;
    padding: 8px 11px 8px 28px;
  }
  .nav button::before {
    left: 11px;
  }
  .rail-footer { display: none; }
  .main { padding: 18px; }
  .topbar { margin-bottom: 12px; }
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .row { grid-template-columns: 1fr; }
  .diag-grid { grid-template-columns: 1fr; }
  .node-layout { grid-template-columns: 1fr; }
  .instance-top { grid-template-columns: 1fr; }
  .detail-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .page-state-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .test-grid { grid-template-columns: 1fr; }
}
@media (max-width: 640px) {
  .main { padding: 16px; }
  .rail {
    grid-template-columns: 1fr;
    gap: 10px;
    padding: 12px 16px;
  }
  .brand span { display: none; }
  .nav {
    margin: 0 -4px;
  }
  .nav button {
    padding-top: 7px;
    padding-bottom: 7px;
  }
  .grid { grid-template-columns: 1fr; }
  .topbar { align-items: flex-start; flex-direction: column; }
  .topbar h1 { font-size: 22px; }
  .input { min-width: 0; width: 100%; }
  .actions, .toolbar { width: 100%; justify-content: stretch; }
  .actions {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
  .button { flex: 1 1 auto; justify-content: center; }
  .runbook-item { grid-template-columns: 1fr; }
  .detail-grid, .page-state-grid, .parameter-grid, .node-signal-grid { grid-template-columns: 1fr; }
  .node-kv-row { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div class="shell">
  <aside class="rail">
    <div class="brand">
      <div class="mark"></div>
      <div>
        <strong>TemporalStore</strong>
        <span>Monitoring console</span>
      </div>
    </div>
    <nav class="nav" aria-label="Dashboard sections">
      <button class="active" data-tab="overview">Overview</button>
      <button data-tab="node">Node</button>
      <button data-tab="testing">Testing</button>
      <button data-tab="monitoring">Monitoring</button>
      <button data-tab="diagnostics">Diagnostics</button>
      <button data-tab="storage">Storage</button>
    </nav>
    <div class="rail-footer">
      <div id="refreshState">Waiting for first sample</div>
      <div><code>/vars</code> every <span id="pollSeconds">5</span>s</div>
    </div>
  </aside>
  <main class="main">
    <div class="topbar">
      <div>
        <h1>TemporalStore Ops Console</h1>
        <div class="subtle">Cluster: <strong id="clusterName">)TSUI";
        os << HtmlEscape(cluster_name_);
        os << R"TSUI(</strong> / Port: <strong id="nodePort">)TSUI";
        os << port_;
        os << R"TSUI(</strong></div>
        <div class="status-strip">
          <span class="status-pill"><span class="pulse" id="refreshPulse"></span><span id="liveState">Live polling</span></span>
          <span class="status-pill">Metrics <strong id="metricCount">0</strong></span>
          <span class="status-pill">Samples <strong id="samplePill">0</strong></span>
        </div>
      </div>
      <div class="actions">
        <button class="button primary" id="refreshNow">Refresh</button>
        <a class="button" href="/vars" target="_blank" rel="noreferrer">/vars</a>
        <a class="button" href="/rpcz" target="_blank" rel="noreferrer">rpcz</a>
      </div>
    </div>

    <section id="overview" class="section active">
      <div class="instance-hero">
        <div class="instance-top">
          <div class="panel instance-summary">
            <div class="instance-title">
              <div>
                <h2>Instance Detail</h2>
                <div class="subtle">Current service identity, endpoint, protocol, and scrape source.</div>
              </div>
              <span class="state-badge" id="instanceState">Starting</span>
            </div>
            <div class="detail-grid">
              <div class="detail-item">
                <div class="detail-label">Cluster</div>
                <div class="detail-value" id="instanceClusterName">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Endpoint</div>
                <div class="detail-value" id="instanceEndpoint">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Engine</div>
                <div class="detail-value">Temporal KV</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Protocol</div>
                <div class="detail-value">BRPC / Redis</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Metric Source</div>
                <div class="detail-value">/vars</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Refresh</div>
                <div class="detail-value"><span id="refreshIntervalValue">5s</span></div>
              </div>
            </div>
          </div>
          <div class="panel instance-summary">
            <div class="instance-title">
              <div>
                <h2>Operations Map</h2>
                <div class="subtle">The shortest path from health check to testing, monitoring, and diagnosis.</div>
              </div>
              <span class="tag">Workflow</span>
            </div>
            <div class="page-state-grid">
              <div class="page-state active" id="stateServing"><strong>Serving</strong><span id="stateServingText">Waiting for the first sample</span></div>
              <div class="page-state" id="stateNode"><strong>Node</strong><span>Per-node identity, runtime, and resource signals</span></div>
              <div class="page-state active" id="stateMonitoring"><strong>Monitoring</strong><span>Live counters and metric search</span></div>
              <div class="page-state" id="stateEvents"><strong>Diagnostics</strong><span id="stateEventsText">No active alert hints</span></div>
              <div class="page-state" id="stateBackup"><strong>Storage</strong><span>Persistence, replay, and replication</span></div>
              <div class="page-state" id="stateParams"><strong>Testing</strong><span>Smoke checks and data-path probes</span></div>
            </div>
          </div>
        </div>
      </div>
      <div class="grid">
        <div class="panel metric">
          <div class="label">Health</div>
          <div class="value"><span id="healthDot" class="status-dot"></span><span id="healthValue">Unknown</span></div>
          <div class="hint" id="healthHint">No sample yet.</div>
        </div>
        <div class="panel metric">
          <div class="label">Request rate</div>
          <div class="value" id="qpsValue">-</div>
          <div class="hint">Visible QPS counters</div>
        </div>
        <div class="panel metric">
          <div class="label">High latency</div>
          <div class="value" id="latencyValue">-</div>
          <div class="hint">Largest latency-like metric</div>
        </div>
        <div class="panel metric">
          <div class="label">Replication lag</div>
          <div class="value" id="lagValue">-</div>
          <div class="hint">Largest lag-like metric</div>
        </div>
      </div>
      <div class="row">
        <div class="panel">
          <div class="panel-header">
            <h2>Live Request Trend</h2>
            <span class="tag" id="sampleCount">0 samples</span>
          </div>
          <div class="panel-body chart-wrap"><canvas id="qpsChart" width="900" height="240"></canvas></div>
        </div>
        <div class="panel">
          <div class="panel-header"><h2>Signals</h2></div>
          <div class="panel-body"><div id="alerts" class="alert-list"></div></div>
        </div>
      </div>
    </section>

    <section id="node" class="section">
      <div class="section-stack">
        <div class="node-layout">
          <div class="panel node-card">
            <div class="node-title">
              <div>
                <h2>Node Identity</h2>
                <p>Use this page when debugging one data node, one pod, or one local process.</p>
              </div>
              <span class="state-badge" id="nodeStateBadge">Starting</span>
            </div>
            <div class="node-kv">
              <div class="node-kv-row"><span>Cluster</span><strong id="nodeClusterNameValue">-</strong></div>
              <div class="node-kv-row"><span>Endpoint</span><strong id="nodeEndpointValue">-</strong></div>
              <div class="node-kv-row"><span>Host</span><strong id="nodeHostValue">-</strong></div>
              <div class="node-kv-row"><span>Port</span><strong id="nodePortValue">-</strong></div>
              <div class="node-kv-row"><span>Protocol</span><strong>BRPC / Redis</strong></div>
              <div class="node-kv-row"><span>Metrics</span><strong><span id="nodeMetricCountValue">0</span> scraped from /vars</strong></div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">
              <h2>Node Signals</h2>
              <span class="tag" id="nodeLastSampleValue">No sample</span>
            </div>
            <div class="panel-body">
              <div class="node-signal-grid">
                <div class="node-signal"><span>Health</span><strong id="nodeHealthValue">Unknown</strong><small id="nodeHealthHint">No sample yet.</small></div>
                <div class="node-signal"><span>Request rate</span><strong id="nodeQpsValue">-</strong><small>Visible request counters</small></div>
                <div class="node-signal"><span>High latency</span><strong id="nodeLatencyValue">-</strong><small>Largest latency-like metric</small></div>
                <div class="node-signal"><span>Error counters</span><strong id="nodeErrorValue">-</strong><small>fail, error, abort, reject</small></div>
                <div class="node-signal"><span>Cache</span><strong id="nodeCacheValue">-</strong><small>hit ratio or hit-like counter</small></div>
                <div class="node-signal"><span>Storage</span><strong id="nodeStorageValue">-</strong><small>bytes, memory, page, or blob signals</small></div>
                <div class="node-signal"><span>Replication</span><strong id="nodeReplicationValue">-</strong><small>lag, replica, oplog, index log</small></div>
                <div class="node-signal"><span>Samples</span><strong id="nodeSamplesValue">0</strong><small>Browser-side polling samples</small></div>
                <div class="node-signal"><span>Refresh</span><strong id="nodeRefreshValue">5s</strong><small>Dashboard polling interval</small></div>
              </div>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-header">
            <h2>Node Debug Links</h2>
            <span class="tag">Local process</span>
          </div>
          <div class="panel-body">
            <div class="endpoint-list">
              <div class="endpoint-row"><code>/vars</code><a class="button" href="/vars" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/rpcz</code><a class="button" href="/rpcz" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/flags</code><a class="button" href="/flags" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/connections</code><a class="button" href="/connections" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/cpu_profile</code><a class="button" href="/cpu_profile" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/heap</code><a class="button" href="/heap" target="_blank" rel="noreferrer">open</a></div>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-header"><h2>Node Metric Breakdown</h2></div>
          <div class="panel-body">
            <div class="table-wrap">
              <table>
                <thead><tr><th>Metric</th><th>Value</th><th>Type</th></tr></thead>
                <tbody id="nodeMetricRows"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section id="testing" class="section">
      <div class="section-stack">
        <div class="panel">
          <div class="panel-header">
            <h2>Testing Checklist</h2>
            <span class="tag">Smoke</span>
          </div>
          <div class="panel-body">
            <div class="test-grid">
              <div class="panel test-card">
                <h3>1. Service Health</h3>
                <p>Confirm the process serves dashboard, vars, and runtime endpoints before running client tests.</p>
                <a class="button" href="/vars" target="_blank" rel="noreferrer">Open /vars</a>
              </div>
              <div class="panel test-card">
                <h3>2. Data Path</h3>
                <p>Run SDK or Redis protocol write/read tests, then watch request, latency, and error counters.</p>
                <button class="button primary" data-tab="monitoring">View metrics</button>
              </div>
              <div class="panel test-card">
                <h3>3. Failure Path</h3>
                <p>Restart a node or force a slow storage path, then inspect lag, replay, and recovery signals.</p>
                <button class="button" data-tab="storage">View storage</button>
              </div>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-header"><h2>Quick Endpoint Checks</h2></div>
          <div class="panel-body">
            <div class="endpoint-list">
              <div class="endpoint-row"><code>/vars</code><a class="button" href="/vars" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/rpcz</code><a class="button" href="/rpcz" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/flags</code><a class="button" href="/flags" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/connections</code><a class="button" href="/connections" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/cpu_profile</code><a class="button" href="/cpu_profile" target="_blank" rel="noreferrer">open</a></div>
              <div class="endpoint-row"><code>/heap</code><a class="button" href="/heap" target="_blank" rel="noreferrer">open</a></div>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section id="monitoring" class="section">
      <div class="panel">
        <div class="panel-header">
          <h2>Metric Explorer</h2>
          <div class="toolbar">
            <input id="metricSearch" class="input" placeholder="Search metric names">
            <select id="metricLimit" class="input">
              <option value="80">80 rows</option>
              <option value="200">200 rows</option>
              <option value="10000">All</option>
            </select>
          </div>
        </div>
        <div class="panel-body">
          <div class="chips" id="metricChips"></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Metric</th><th>Value</th><th>Type</th></tr></thead>
              <tbody id="metricRows"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section id="diagnostics" class="section">
      <div class="section-stack">
      <div class="grid">
        <div class="panel metric">
          <div class="label">Error counters</div>
          <div class="value" id="errorValue">-</div>
          <div class="hint">error, fail, abort, loss, reject</div>
        </div>
        <div class="panel metric">
          <div class="label">Storage bytes</div>
          <div class="value" id="bytesValue">-</div>
          <div class="hint">bytes and throughput signals</div>
        </div>
        <div class="panel metric">
          <div class="label">Cache hit</div>
          <div class="value" id="cacheValue">-</div>
          <div class="hint">hit ratio or hit count</div>
        </div>
        <div class="panel metric">
          <div class="label">Last refresh</div>
          <div class="value" id="lastRefresh">-</div>
          <div class="hint">local browser time</div>
        </div>
      </div>
      <div class="diag-grid">
        <div class="panel diag-card">
          <h3>RPC tracing</h3>
          <p>Slow calls, failed calls, request distribution.</p>
          <a class="button" href="/rpcz" target="_blank" rel="noreferrer">rpcz</a>
        </div>
        <div class="panel diag-card">
          <h3>Runtime variables</h3>
          <p>Counters, gauges, histograms, internals.</p>
          <a class="button" href="/vars" target="_blank" rel="noreferrer">vars</a>
        </div>
        <div class="panel diag-card">
          <h3>Configuration</h3>
          <p>Runtime flags for memory, storage, networking.</p>
          <a class="button" href="/flags" target="_blank" rel="noreferrer">flags</a>
        </div>
        <div class="panel diag-card">
          <h3>Connections</h3>
          <p>Client and server connection state.</p>
          <a class="button" href="/connections" target="_blank" rel="noreferrer">connections</a>
        </div>
        <div class="panel diag-card">
          <h3>CPU profile</h3>
          <p>CPU cost when latency rises.</p>
          <a class="button" href="/cpu_profile" target="_blank" rel="noreferrer">profile</a>
        </div>
        <div class="panel diag-card">
          <h3>Heap</h3>
          <p>Allocator, cache, and process memory.</p>
          <a class="button" href="/heap" target="_blank" rel="noreferrer">heap</a>
        </div>
      </div>
        <div class="panel">
          <div class="panel-header">
            <h2>Runtime Parameters</h2>
            <a class="button" href="/flags" target="_blank" rel="noreferrer">flags</a>
          </div>
          <div class="panel-body">
            <div class="parameter-grid">
              <div class="parameter-row"><strong>Memory and Cache</strong><span>Object cache, block cache, allocator, and reclaim settings.</span></div>
              <div class="parameter-row"><strong>Persistence</strong><span>Page, index-log, and oplog write paths.</span></div>
              <div class="parameter-row"><strong>Replication</strong><span>Primary, secondary, replay, and lag controls.</span></div>
              <div class="parameter-row"><strong>Network</strong><span>BRPC, Redis protocol, timeout, and connection behavior.</span></div>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-header"><h2>Allocator Stats</h2></div>
          <div class="panel-body"><pre>)TSUI";
#ifndef __CYGWIN__
        if (MallocExtension::instance()) {
            char buf[10240];
            MallocExtension::instance()->GetStats(buf, sizeof(buf));
            os << HtmlEscape(buf);
        } else {
            os << "MallocExtension is not available in this build.";
        }
#else
        os << "MallocExtension is not available in this build.";
#endif
        os << R"TSUI(</pre></div>
        </div>
      </div>
    </section>

    <section id="storage" class="section">
      <div class="row">
        <div class="panel">
          <div class="panel-header"><h2>Storage and Replication Metrics</h2></div>
          <div class="panel-body">
            <div class="table-wrap">
              <table>
                <thead><tr><th>Metric</th><th>Value</th><th>Type</th></tr></thead>
                <tbody id="storageRows"></tbody>
              </table>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-header"><h2>Runbook Signals</h2></div>
          <div class="panel-body">
            <div class="runbook-list">
              <div class="runbook-item"><strong>Replay</strong><span>index log, oplog, lag_ms</span></div>
              <div class="runbook-item"><strong>Memory</strong><span>object hit, object in memory, reclaim latency</span></div>
              <div class="runbook-item"><strong>Cold read</strong><span>blob read latency, block cache hit, page read</span></div>
              <div class="runbook-item"><strong>GC</strong><span>page GC, compaction ratio, rewrite throughput</span></div>
            </div>
          </div>
        </div>
      </div>
    </section>
  </main>
</div>
<script>
const state = {
  metrics: [],
  category: "all",
  samples: [],
  pollMs: 5000,
  timer: null
};

const categories = [
  ["all", "All"],
  ["request", "Requests"],
  ["latency", "Latency"],
  ["storage", "Storage"],
  ["cache", "Cache"],
  ["replication", "Replication"],
  ["error", "Errors"]
];

function byId(id) { return document.getElementById(id); }

function setText(id, value) {
  const el = byId(id);
  if (el) el.textContent = value;
}

function renderInstanceIdentity() {
  const cluster = byId("clusterName") ? byId("clusterName").textContent : "-";
  const port = byId("nodePort") ? byId("nodePort").textContent : "";
  const host = window.location.hostname || "127.0.0.1";
  setText("instanceClusterName", cluster);
  setText("instanceEndpoint", port ? `${host}:${port}` : window.location.host);
  setText("refreshIntervalValue", String(state.pollMs / 1000) + "s");
  setText("nodeClusterNameValue", cluster);
  setText("nodeHostValue", host);
  setText("nodePortValue", port || "-");
  setText("nodeEndpointValue", port ? `${host}:${port}` : window.location.host);
  setText("nodeRefreshValue", String(state.pollMs / 1000) + "s");
}

function classify(name) {
  const n = name.toLowerCase();
  if (/fail|error|abort|loss|reject|timeout/.test(n)) return "error";
  if (/latency|cost|elapsed|p99|p95|p90/.test(n)) return "latency";
  if (/qps|request|cmd|read|write|append/.test(n)) return "request";
  if (/cache|hit|blockcache/.test(n)) return "cache";
  if (/replica|replicat|oplog|index_log|lag/.test(n)) return "replication";
  if (/page|slot|object|blob|store|storage|throughput|bytes|memory|gc|compact/.test(n)) return "storage";
  return "other";
}

function stripHtml(input) {
  const el = document.createElement("textarea");
  el.innerHTML = input.replace(/<br\s*\/?>/gi, "\n").replace(/<\/tr>/gi, "\n").replace(/<[^>]+>/g, " ");
  return el.value;
}

function parseNumber(raw) {
  const cleaned = String(raw).replace(/,/g, "").trim();
  const match = cleaned.match(/[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?/i);
  return match ? Number(match[0]) : NaN;
}

function parseVars(text) {
  const clean = stripHtml(text);
  const seen = new Map();
  const lines = clean.split(/\r?\n/);
  for (const line of lines) {
    const m = line.match(/^\s*([A-Za-z0-9_.:\/-]+)\s*[:=]\s*([^ \t].*?)\s*$/);
    if (!m) continue;
    const name = m[1];
    const raw = m[2];
    const value = parseNumber(raw);
    if (!Number.isFinite(value)) continue;
    seen.set(name, { name, raw, value, type: classify(name) });
  }
  return Array.from(seen.values()).sort((a, b) => a.name.localeCompare(b.name));
}

function fmt(num, unit = "") {
  if (!Number.isFinite(num)) return "-";
  const abs = Math.abs(num);
  if (abs >= 1e12) return (num / 1e12).toFixed(2) + "T" + unit;
  if (abs >= 1e9) return (num / 1e9).toFixed(2) + "B" + unit;
  if (abs >= 1e6) return (num / 1e6).toFixed(2) + "M" + unit;
  if (abs >= 1e3) return (num / 1e3).toFixed(2) + "K" + unit;
  if (abs >= 10) return num.toFixed(0) + unit;
  if (abs > 0) return num.toFixed(2) + unit;
  return "0" + unit;
}

function sumWhere(regex) {
  return state.metrics.filter(m => regex.test(m.name.toLowerCase())).reduce((s, m) => s + Math.max(0, m.value), 0);
}

function maxWhere(regex) {
  let best = NaN;
  for (const m of state.metrics) {
    if (regex.test(m.name.toLowerCase())) best = Number.isFinite(best) ? Math.max(best, m.value) : m.value;
  }
  return best;
}

function bestMetric(regex) {
  let best = null;
  for (const m of state.metrics) {
    if (!regex.test(m.name.toLowerCase())) continue;
    if (!best || m.value > best.value) best = m;
  }
  return best;
}

function updateStateBadge(id, label, health) {
  const badge = byId(id);
  if (!badge) return;
  badge.className = "state-badge";
  badge.textContent = label;
  if (health === "Watch") badge.classList.add("warn");
  if (health === "Degraded") badge.classList.add("bad");
}

function renderNodeView(qps, latency, lag, errors, bytes, cache, health, hint, now) {
  updateStateBadge("nodeStateBadge", health === "Healthy" ? "Running" : health, health);
  setText("nodeHealthValue", health);
  setText("nodeHealthHint", hint);
  setText("nodeQpsValue", fmt(qps));
  setText("nodeLatencyValue", Number.isFinite(latency) ? fmt(latency, "us") : "-");
  setText("nodeErrorValue", fmt(errors));
  setText("nodeCacheValue", cache ? fmt(cache.value) : "-");
  setText("nodeStorageValue", Number.isFinite(bytes) ? fmt(bytes) : "-");
  setText("nodeReplicationValue", Number.isFinite(lag) ? fmt(lag, "ms") : "-");
  setText("nodeSamplesValue", String(state.samples.length));
  setText("nodeMetricCountValue", String(state.metrics.length));
  setText("nodeLastSampleValue", "Updated " + now.toLocaleTimeString());
}

function renderOverview() {
  const qps = sumWhere(/qps|request.*count|cmd.*count/);
  const latency = maxWhere(/latency|p99|p95|elapsed/);
  const lag = maxWhere(/lag_ms|lag|gap/);
  const errors = sumWhere(/fail|error|abort|loss|reject|timeout/);
  const bytes = maxWhere(/bytes|throughput|memory/);
  const cache = bestMetric(/hit_ratio|hit.*ratio|cache.*hit|object_hit/);
  const now = new Date();
  state.samples.push({ t: now, qps });
  state.samples = state.samples.slice(-60);

  byId("qpsValue").textContent = fmt(qps);
  byId("latencyValue").textContent = Number.isFinite(latency) ? fmt(latency, "us") : "-";
  byId("lagValue").textContent = Number.isFinite(lag) ? fmt(lag, "ms") : "-";
  byId("errorValue").textContent = fmt(errors);
  byId("bytesValue").textContent = Number.isFinite(bytes) ? fmt(bytes) : "-";
  byId("cacheValue").textContent = cache ? fmt(cache.value) : "-";
  byId("lastRefresh").textContent = now.toLocaleTimeString();
  byId("sampleCount").textContent = state.samples.length + " samples";
  byId("metricCount").textContent = state.metrics.length;
  byId("samplePill").textContent = state.samples.length;

  const dot = byId("healthDot");
  dot.className = "status-dot";
  let health = "Healthy";
  let hint = "No severe signals in this sample.";
  if (errors > 0 || (Number.isFinite(lag) && lag > 10000)) {
    health = "Degraded";
    hint = "Errors or high lag are present.";
    dot.classList.add("bad");
  } else if ((Number.isFinite(lag) && lag > 1000) || (Number.isFinite(latency) && latency > 50000)) {
    health = "Watch";
    hint = "Lag or latency is elevated.";
    dot.classList.add("warn");
  }
  byId("healthValue").textContent = health;
  byId("healthHint").textContent = hint;
  updateStateBadge("instanceState", health === "Healthy" ? "Running" : health, health);

  const servingState = byId("stateServing");
  servingState.className = "page-state active";
  if (health !== "Healthy") servingState.classList.add("warn");
  setText("stateServingText", health === "Healthy" ? "Accepting reads and writes" : hint);
  const nodeState = byId("stateNode");
  nodeState.className = "page-state";
  if (health !== "Healthy") nodeState.classList.add("warn");

  const eventState = byId("stateEvents");
  eventState.className = "page-state";
  if (errors > 0 || (Number.isFinite(lag) && lag > 1000) || (Number.isFinite(latency) && latency > 50000)) {
    eventState.classList.add("warn");
    setText("stateEventsText", "Review current alert hints");
  } else {
    setText("stateEventsText", "No active alert hints");
  }
  renderNodeView(qps, latency, lag, errors, bytes, cache, health, hint, now);
  renderAlerts(errors, lag, latency);
  drawChart();
}

function renderAlerts(errors, lag, latency) {
  const alerts = [];
  if (errors > 0) alerts.push(["bad", "Error counters are non-zero", "Open rpcz and filter error metrics."]);
  if (Number.isFinite(lag) && lag > 1000) alerts.push(["warn", "Replication lag is visible", "Check oplog/index log gap and replay metrics."]);
  if (Number.isFinite(latency) && latency > 50000) alerts.push(["warn", "Latency is elevated", "Compare rpcz, CPU profile, and storage read latency."]);
  if (alerts.length === 0) alerts.push(["", "No active alert hints", "No obvious error, lag, or latency signal."]);
  byId("alerts").innerHTML = alerts.map(a => `<div class="alert ${a[0]}"><strong>${a[1]}</strong><span>${a[2]}</span></div>`).join("");
}

function drawChart() {
  const canvas = byId("qpsChart");
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = "#d9e0ea";
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i++) {
    const y = 18 + i * ((h - 38) / 4);
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
  }
  if (state.samples.length < 2) return;
  const max = Math.max(1, ...state.samples.map(s => s.qps));
  ctx.strokeStyle = "#2f6fed";
  ctx.lineWidth = 3;
  ctx.beginPath();
  state.samples.forEach((s, i) => {
    const x = (i / Math.max(1, state.samples.length - 1)) * (w - 24) + 12;
    const y = h - 20 - (s.qps / max) * (h - 42);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#667085";
  ctx.font = "12px system-ui";
  ctx.fillText("max " + fmt(max), 12, 16);
}

function renderChips() {
  const counts = Object.fromEntries(categories.map(([key]) => [key, key === "all" ? state.metrics.length : state.metrics.filter(m => m.type === key).length]));
  byId("metricChips").innerHTML = categories.map(([key, label]) =>
    `<button class="chip ${state.category === key ? "active" : ""}" data-category="${key}">${label}<span>${counts[key]}</span></button>`
  ).join("");
  document.querySelectorAll("[data-category]").forEach(btn => {
    btn.onclick = () => {
      state.category = btn.dataset.category;
      renderChips();
      renderTables();
    };
  });
}

function renderRows(rows, id) {
  if (rows.length === 0) {
    byId(id).innerHTML = `<tr><td colspan="3"><div class="empty">No matching metrics in the current sample.</div></td></tr>`;
    return;
  }
  byId(id).innerHTML = rows.map(m =>
    `<tr><td class="metric-name">${escapeHtml(m.name)}</td><td class="metric-value">${escapeHtml(m.raw)}</td><td><span class="tag tag-${m.type}">${m.type}</span></td></tr>`
  ).join("");
}

function renderTables() {
  const q = byId("metricSearch").value.toLowerCase().trim();
  const limit = Number(byId("metricLimit").value);
  let rows = state.metrics;
  if (state.category !== "all") rows = rows.filter(m => m.type === state.category);
  if (q) rows = rows.filter(m => m.name.toLowerCase().includes(q));
  renderRows(rows.slice(0, limit), "metricRows");
  const nodeRows = state.metrics.filter(m => /request|latency|storage|cache|replication|error/.test(m.type)).slice(0, 180);
  renderRows(nodeRows, "nodeMetricRows");
  const storageRows = state.metrics.filter(m => /storage|replication|cache|latency|error/.test(m.type)).slice(0, 160);
  renderRows(storageRows, "storageRows");
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function refresh() {
  byId("refreshState").textContent = "Refreshing...";
  try {
    const res = await fetch("/vars", { cache: "no-store" });
    const text = await res.text();
    state.metrics = parseVars(text);
    renderOverview();
    renderChips();
    renderTables();
    byId("refreshState").textContent = `Loaded ${state.metrics.length} metrics at ${new Date().toLocaleTimeString()}`;
    byId("liveState").textContent = "Live polling";
  } catch (err) {
    byId("refreshState").textContent = "Refresh failed: " + err.message;
    byId("liveState").textContent = "Polling failed";
    byId("alerts").innerHTML = `<div class="alert bad"><strong>Cannot fetch /vars</strong><span>${escapeHtml(err.message)}</span></div>`;
  }
}

function setActiveTab(tabName) {
  document.querySelectorAll("[data-tab]").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.tab === tabName);
  });
  document.querySelectorAll(".section").forEach(section => {
    section.classList.toggle("active", section.id === tabName);
  });
}

document.querySelectorAll("[data-tab]").forEach(btn => {
  btn.onclick = () => {
    setActiveTab(btn.dataset.tab);
  };
});
byId("refreshNow").onclick = refresh;
byId("metricSearch").oninput = renderTables;
byId("metricLimit").onchange = renderTables;
byId("pollSeconds").textContent = String(state.pollMs / 1000);
renderInstanceIdentity();
renderChips();
refresh();
state.timer = setInterval(refresh, state.pollMs);
</script>
</body>
</html>)TSUI";

        ctrl->http_response().set_content_type("text/html; charset=utf-8");
        os.move_to(ctrl->response_attachment());
    }

 private:
    static std::string HtmlEscape(const std::string& input) {
        std::string out;
        out.reserve(input.size());
        for (char ch : input) {
            switch (ch) {
            case '&':
                out += "&amp;";
                break;
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            case '"':
                out += "&quot;";
                break;
            case '\'':
                out += "&#39;";
                break;
            default:
                out += ch;
                break;
            }
        }
        return out;
    }

    std::string cluster_name_;
    int port_;

    DISALLOW_COPY_AND_ASSIGN(DashboardServiceImpl);
};

}  // namespace server
}  // namespace bcache2
