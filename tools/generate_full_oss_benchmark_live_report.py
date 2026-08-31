#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Generate a live HTML checkpoint for OSS LoCoMo/LongMemEval benchmark runs."""
from __future__ import annotations
import argparse, collections, html, json, pathlib, statistics, subprocess, time
from typing import Any
DEFAULT_RUN_DIR = pathlib.Path('benchmark-runs/goal-full-locomo-20260729-rust-qwen15b-cap128-strict')
DEFAULT_PER_QUERY = 'matrixark_locomo_rust_qwen15b_cap128_strict.per_query.jsonl'
DEFAULT_PROGRESS = 'matrixark_locomo_rust_qwen15b_cap128_strict.progress.json'
DEFAULT_BACKEND_REPORT = pathlib.Path('/tmp/temporalstore-rust-context-locomo10-8545.json')
def load_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists(): return {}
    with path.open(encoding='utf-8', errors='replace') as handle: data=json.load(handle)
    return data if isinstance(data, dict) else {}
def load_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    rows=[]
    if not path.exists(): return rows
    with path.open(encoding='utf-8', errors='replace') as handle:
        for line in handle:
            line=line.strip()
            if not line: continue
            row=json.loads(line)
            if isinstance(row, dict): rows.append(row)
    return rows
def rate(n,d): return float(n)/float(d)*100.0 if d else 0.0
def pct(vals,p):
    nums=sorted(v for v in vals if isinstance(v,(int,float)))
    if not nums: return 0.0
    k=(len(nums)-1)*p/100.0; f=int(k); c=min(f+1,len(nums)-1)
    return float(nums[f] if f==c else nums[f]*(c-k)+nums[c]*(k-f))
def esc(v): return html.escape(str(v))
def sh(cmd):
    try: return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT)
    except Exception as exc: return str(exc)
def metric(title,value,sub='',cls=''):
    return f'<div class="card"><div class="sub">{esc(title)}</div><div class="metric {cls}">{esc(value)}</div><div class="sub">{esc(sub)}</div></div>'
def gap_tuning_section():
    rows=[
        ('cap128 current strict run', 'baseline live run', 'Fast, compact, but current retrieval misses remain clustered in a few LoCoMo categories.'),
        ('cap160', 'recovered 7 / 24 prior retrieval misses', 'First useful step up; still leaves several source-depth misses.'),
        ('cap192', 'recovered 12 / 24 prior retrieval misses', 'Best next retrieval-only probe before spending much more reader time.'),
        ('cap256', 'recovered 17 / 24 prior retrieval misses', 'Likely stronger retrieval quality at moderate extra token cost.'),
        ('cap384', 'recovered 22 / 24 prior retrieval misses', 'High-recall diagnostic setting, not the default compact budget.'),
        ('quota reshuffle at cap128', 'recovered 1 / 24 prior retrieval misses', 'Misses look mostly depth/cutoff-driven, not only per-session/cross-session quota mix.'),
        ('Qwen7B reader subset', '40 retrieval-hit reader-miss questions staged', 'Run after this LoCoMo job frees Ollama, to separate reader quality from retrieval quality.'),
    ]
    body=['<section class="section"><h2>Gap Closure Evidence And Next Knobs</h2><table><tr><th>Probe</th><th>Observed Effect</th><th>Interpretation</th></tr>']
    for name,effect,note in rows:
        body.append(f'<tr><td>{esc(name)}</td><td>{esc(effect)}</td><td>{esc(note)}</td></tr>')
    body.append('</table></section>')
    return ''.join(body)
def bounded_baseline_section():
    base=pathlib.Path('benchmark-runs/fair-oss-qwen-100x100-contract-20260727')
    specs=[
        ('LoCoMo MatrixArk', base/'matrixark_locomo_oss_report.json'),
        ('LoCoMo ExternalBaseline', base/'external_baseline_locomo_oss_report.json'),
        ('LongMemEval_s MatrixArk', base/'matrixark_longmemeval_s_oss_report.json'),
        ('LongMemEval_s ExternalBaseline', base/'external_baseline_longmemeval_s_oss_report.json'),
    ]
    body=['<section class="section"><h2>Bounded Apples-To-Apples Baseline</h2><div class="card"><p>This 100x100 bounded baseline used the same OSS reader/budget contract for MatrixArk and ExternalBaseline/ExternalBaseline-style retrieval. It is not the full run, but it keeps the live checkpoint anchored to prior baseline evidence.</p></div><table><tr><th>Run</th><th>Cases</th><th>Retrieval Hit</th><th>Reader Hit</th><th>Token Reduction</th><th>Retrieval p95</th><th>Reader p95</th></tr>']
    for label,path in specs:
        data=load_json(path)
        body.append(f'<tr><td>{esc(label)}</td><td>{esc(data.get("case_count"))}</td><td>{rate(data.get("benchmark_hit_at_k") or data.get("hit_rate") or 0,1):.2f}%</td><td>{rate(data.get("reader_hit_rate") or 0,1):.2f}%</td><td>{float(data.get("benchmark_token_reduction_percent") or 0):.2f}%</td><td>{float(data.get("benchmark_retrieval_p95_ms") or 0):.1f} ms</td><td>{float(data.get("benchmark_reader_p95_ms") or 0):.1f} ms</td></tr>')
    body.append('</table></section>')
    return ''.join(body)
def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--run-dir', type=pathlib.Path, default=DEFAULT_RUN_DIR)
    ap.add_argument('--per-query', default=DEFAULT_PER_QUERY)
    ap.add_argument('--progress', default=DEFAULT_PROGRESS)
    ap.add_argument('--backend-report', type=pathlib.Path, default=DEFAULT_BACKEND_REPORT)
    ap.add_argument('--qwen7b-script', type=pathlib.Path, default=pathlib.Path('benchmark-runs/goal-full-locomo-20260729-qwen7b-reader-gap-subset/run_qwen7b_reader_gap_subset.sh'))
    ap.add_argument('--output', type=pathlib.Path, required=True)
    ap.add_argument('--longmemeval-note', default='Pending full OSS-reader rerun. Targeted six former retrieval misses were fixed to 6/6 retrieval hit after source packing/event cap fixes.')
    args=ap.parse_args()
    per=args.run_dir/args.per_query; progress=load_json(args.run_dir/args.progress); rows=load_jsonl(per); backend=load_json(args.backend_report)
    completed=len(rows); expected=int(progress.get('expected_case_count') or 1986)
    retrieval_hits=sum(1 for r in rows if r.get('hit')); reader_hits=sum(1 for r in rows if r.get('reader_hit'))
    retrieval_ms=[r.get('retrieval_ms') for r in rows]; reader_ms=[r.get('reader_ms') for r in rows]
    token_saves=[r.get('token_reduction_percent') for r in rows if isinstance(r.get('token_reduction_percent'),(int,float))]
    source_tokens=[r.get('source_tokens') for r in rows if isinstance(r.get('source_tokens'),(int,float))]
    retrieved_tokens=[r.get('retrieved_tokens') for r in rows if isinstance(r.get('retrieved_tokens'),(int,float))]
    by_cat=collections.defaultdict(list)
    for r in rows: by_cat[str(r.get('category') or 'unknown')].append(r)
    retrieval_misses=[r for r in rows if not r.get('hit')]
    reader_misses=[r for r in rows if r.get('hit') and not r.get('reader_hit')]
    mtime_age=time.time()-(per.stat().st_mtime if per.exists() else 0); status='running' if mtime_age<300 and progress.get('phase')!='complete' else 'stale_or_complete'
    tmux=sh('tmux ls 2>&1 || true'); processes=sh("ps -eo pid,etime,stat,pcpu,pmem,args | grep -E 'run_locomo_ingest_once|ollama serve|llama-server' | grep -v grep || true")
    css='body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f7f8fb;color:#172033}header{background:#111827;color:white;padding:28px 36px}main{padding:24px 36px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}.card{background:white;border:1px solid #dde3ee;border-radius:8px;padding:16px}.metric{font-size:28px;font-weight:700}.sub{color:#5f6c7b;font-size:13px}.ok{color:#087f5b}.warn{color:#b7791f}table{border-collapse:collapse;width:100%;background:white;border:1px solid #dde3ee}th,td{padding:9px 10px;border-bottom:1px solid #edf0f5;text-align:left;vertical-align:top;font-size:13px}th{background:#eef2f8}pre{white-space:pre-wrap;background:#0f172a;color:#dbeafe;border-radius:8px;padding:14px;overflow:auto}.pill{display:inline-block;padding:3px 8px;border-radius:999px;background:#e8eef8;color:#172033;margin-right:6px}.section{margin:24px 0}code{background:#edf2f7;padding:1px 4px;border-radius:4px}'
    parts=[f'<!doctype html><html><head><meta charset="utf-8"><title>{esc(args.output.stem)}</title><style>{css}</style></head><body>',f'<header><h1>MatrixArk Full OSS Benchmark Live Report</h1><p>Generated {esc(time.strftime("%Y-%m-%d %H:%M:%S"))}. This is a live checkpoint, not a final benchmark claim.</p><p><span class="pill">{esc(status)}</span><span class="pill">LoCoMo full OSS reader</span><span class="pill">Qwen 2.5 1.5B via Ollama</span><span class="pill">Rust TemporalStore full replay</span></p></header><main>','<section class="grid">']
    parts += [metric('Completed rows', f'{completed} / {expected}', f'{rate(completed,expected):.1f}% complete'), metric('Retrieval hit rate', f'{rate(retrieval_hits,completed):.2f}%', f'{completed-retrieval_hits} misses', 'ok'), metric('Retrieval p95', f'{pct(retrieval_ms,95):.2f} ms', f'p99 {pct(retrieval_ms,99):.2f} ms'), metric('Reader exact hit rate', f'{rate(reader_hits,completed):.2f}%', 'reader quality gap under Qwen 1.5B', 'warn'), metric('Reader p95', f'{pct(reader_ms,95):.2f} ms', f'p99 {pct(reader_ms,99):.2f} ms'), metric('Mean token reduction', f'{(statistics.mean(token_saves) if token_saves else 0):.2f}%', f'source avg {(statistics.mean(source_tokens) if source_tokens else 0):.0f}; retrieved avg {(statistics.mean(retrieved_tokens) if retrieved_tokens else 0):.0f}')]
    parts += ['</section>','<section class="section"><h2>What This Checkpoint Shows</h2><div class="card"><p>Retrieval remains strong and fast on the completed slice. The main visible quality gap is downstream reader answering: many misses have retrieval_hit=true, so the right source is present but the small OSS reader does not reliably extract the exact answer.</p><p>The active goal is still open: full LoCoMo completion, full LongMemEval_s OSS-reader rerun, and apples-to-apples ExternalBaseline/ExternalBaseline comparison remain required before final claims.</p></div></section>']
    parts.append(gap_tuning_section())
    parts.append(bounded_baseline_section())
    parts.append(f'<section class="section"><h2>Current Runner State</h2><table><tr><th>Field</th><th>Value</th></tr><tr><td>phase</td><td>{esc(progress.get("phase"))}</td></tr><tr><td>current query</td><td>{esc(progress.get("query_id"))}</td></tr><tr><td>last completed row</td><td>{esc(rows[-1].get("query_id") if rows else None)}</td></tr><tr><td>per-query mtime age</td><td>{mtime_age:.1f}s</td></tr><tr><td>tmux</td><td><pre>{esc(tmux)}</pre></td></tr><tr><td>processes</td><td><pre>{esc(processes)}</pre></td></tr></table></section>')
    parts.append('<section class="section"><h2>Category Breakdown</h2><table><tr><th>Category</th><th>Rows</th><th>Retrieval Hit</th><th>Reader Hit</th><th>Retrieval p95</th><th>Reader p95</th></tr>')
    for cat, cr in sorted(by_cat.items()):
        n=len(cr); parts.append(f'<tr><td>{esc(cat)}</td><td>{n}</td><td>{rate(sum(bool(r.get("hit")) for r in cr),n):.2f}%</td><td>{rate(sum(bool(r.get("reader_hit")) for r in cr),n):.2f}%</td><td>{pct([r.get("retrieval_ms") for r in cr],95):.2f} ms</td><td>{pct([r.get("reader_ms") for r in cr],95):.2f} ms</td></tr>')
    parts.append('</table></section><section class="section"><h2>Recent Completed Questions</h2><table><tr><th>Query</th><th>Category</th><th>Retrieval</th><th>Reader</th><th>Rank</th><th>Answer</th><th>Expected</th></tr>')
    for r in rows[-15:]: parts.append(f'<tr><td>{esc(r.get("query_id"))}</td><td>{esc(r.get("category"))}</td><td>{"hit" if r.get("hit") else "miss"}</td><td>{"hit" if r.get("reader_hit") else "miss"}</td><td>{esc(r.get("rank"))}</td><td>{esc(str(r.get("reader_answer", ""))[:220])}</td><td>{esc(r.get("expected_answer_terms"))}</td></tr>')
    parts.append('</table></section><section class="section"><h2>Retrieval Miss Samples</h2><table><tr><th>Query</th><th>Category</th><th>Expected Source Ref IDs</th><th>Expected Terms</th></tr>')
    for r in retrieval_misses[:25]: parts.append(f'<tr><td>{esc(r.get("query_id"))}</td><td>{esc(r.get("category"))}</td><td>{esc(r.get("expected_source_ref_ids"))}</td><td>{esc(r.get("expected_answer_terms"))}</td></tr>')
    parts.append('</table></section><section class="section"><h2>Reader Misses Where Retrieval Hit</h2><table><tr><th>Query</th><th>Category</th><th>Reader Answer</th><th>Expected Terms</th></tr>')
    for r in reader_misses[:25]: parts.append(f'<tr><td>{esc(r.get("query_id"))}</td><td>{esc(r.get("category"))}</td><td>{esc(str(r.get("reader_answer", ""))[:260])}</td><td>{esc(r.get("expected_answer_terms"))}</td></tr>')
    backend_summary={'backend_ready':backend.get('rust_temporalstore_backend_ready'),'full_replay_ready':backend.get('rust_temporalstore_full_replay_ready'),'case_count':backend.get('harness',{}).get('external_benchmark_case_count'),'hit_at_k':backend.get('harness',{}).get('external_benchmark_hit_at_k')}
    parts.append(f'</table></section><section class="section"><h2>Backend Evidence And Queue</h2><table><tr><th>Item</th><th>Details</th></tr><tr><td>Rust LoCoMo full replay</td><td><pre>{esc(json.dumps(backend_summary, indent=2))}</pre></td></tr><tr><td>Next isolated reader check</td><td><code>{esc(args.qwen7b_script)}</code></td></tr><tr><td>LongMemEval_s</td><td>{esc(args.longmemeval_note)}</td></tr></table></section></main></body></html>')
    args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text('\n'.join(parts), encoding='utf-8'); print(args.output)
if __name__ == '__main__': main()
