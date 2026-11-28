# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Require one positive exact Rust result for every slab locking control."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

GROUP_COUNTS = dict(prepare=8, record=9, active=7, api_open_io=2, retirement=1)
EXISTING_COUNT = 73


def controls_from(data):
    if data.get('schema_version') != 1 or data.get('new_group_counts') != GROUP_COUNTS:
        raise ValueError('unexpected control schema or group denominator')
    rows = data['controls']
    selectors = [row['selector'] for row in rows]
    if len(selectors) != len(set(selectors)) or any(not re.fullmatch(r'block_store(?:::[A-Za-z_][A-Za-z_0-9]*)+', s) for s in selectors):
        raise ValueError('duplicate or invalid exact selector')
    for group, count in GROUP_COUNTS.items():
        if sum(row['kind'] == 'new' and row['group'] == group for row in rows) != count:
            raise ValueError('new control list is incomplete')
    if data.get('new_control_count') != 27 or sum(row['kind'] == 'new' for row in rows) != 27:
        raise ValueError('expected 27 distinct new controls')
    existing = [row for row in rows if row['kind'] == 'existing' and row['group'] == 'affected_existing']
    if len(existing) != EXISTING_COUNT or len(existing) != data.get('affected_existing_count') or len(rows) != 27 + len(existing):
        raise ValueError('affected existing controls are incomplete')
    return rows


def command(selector):
    return ['cargo', 'test', '--locked', '--release', '-p', 'temporalstore-rust', '--lib', selector,
            '--', '--exact', '--test-threads=1']


def require_one_pass(selector, output, returncode):
    if returncode != 0: raise ValueError(f'{selector}: cargo exited {returncode}')
    lines = output.splitlines()
    if any(re.search(r'\b(?:cancelled|canceled|cancellation|interrupted|SIGTERM|SIGINT)\b', line, re.I) for line in lines):
        raise ValueError(f'{selector}: cancelled or interrupted output')
    if [line for line in lines if re.fullmatch(r'running \d+ tests?', line)] != ['running 1 test']:
        raise ValueError(f'{selector}: expected exactly one running test')
    if [line for line in lines if line.startswith('test ') and not line.startswith('test result:')] != [f'test {selector} ... ok']:
        raise ValueError(f'{selector}: expected the exact named positive result')
    summaries = [line for line in lines if line.startswith('test result:')]
    if len(summaries) != 1 or not re.fullmatch(
        r'test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; \d+ filtered out; finished in .+', summaries[0]
    ): raise ValueError(f'{selector}: missing one positive complete summary')
    return summaries[0]


def write_receipt(path, data):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def main():
    here = Path(__file__).resolve().parent
    rows = controls_from(json.loads((here / 'owned_slab_locking_controls.json').read_text()))
    folder = Path(os.environ.get('RUNNER_TEMP', tempfile.gettempdir())) / 'owned-slab-locking'
    folder.mkdir(parents=True, exist_ok=True)
    receipt = dict(status='running', complete=False, expected=len(rows), results=[], started_at=time.time())
    path = folder / 'receipt.json'
    write_receipt(path, receipt)
    def cancelled(signum, _frame):
        raise InterruptedError(f'cancelled by signal {signum}')
    for signum in (signal.SIGINT, signal.SIGTERM): signal.signal(signum, cancelled)
    try:
        for index, row in enumerate(rows):
            argv = command(row['selector'])
            log = folder / f'{index:03d}.log'
            print(' '.join(argv), flush=True)
            with log.open('wb') as stream:
                process = subprocess.Popen(argv, cwd=here.parent, stdout=stream, stderr=subprocess.STDOUT,
                                           env={**os.environ, 'CARGO_TERM_COLOR': 'never'}, start_new_session=True)
                try:
                    code = process.wait(timeout=600)
                except BaseException:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise
            if log.stat().st_size > 8 * 1024 * 1024: raise ValueError('control log exceeded bounded size')
            summary = require_one_pass(row['selector'], log.read_text(encoding='utf-8', errors='replace'), code)
            receipt['results'].append(dict(selector=row['selector'], command=argv, exit_code=code, summary=summary))
            write_receipt(path, receipt)
        if len(receipt['results']) != receipt['expected']: raise ValueError('control list did not finish')
        receipt.update(status='passed', complete=True)
    except BaseException as error:
        receipt.update(status='failed', complete=False, error=str(error))
        raise
    finally:
        write_receipt(path, receipt)


if __name__ == '__main__': main()
