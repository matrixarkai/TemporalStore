# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_context_backfill.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import hashlib
import json
import shlex
import time
from typing import Any

try:  # package path (tools.matrixark_context_backfill)
    from .matrixark_context_backfill import (
        BackfillError,
        Json,
        MatrixKVBackfillTarget,
        Path,
        ROOT,
        _prom_labels,
        make_kv,
        normalize_raw_backend,
    )
except ImportError:  # top-level path (matrixark_context_backfill)
    from matrixark_context_backfill import (
        BackfillError,
        Json,
        MatrixKVBackfillTarget,
        Path,
        ROOT,
        _prom_labels,
        make_kv,
        normalize_raw_backend,
    )

__all__ = ['_plan_arg', '_append_plan_arg', 'build_plan_command_base_args', 'build_plan_active_args', 'build_plan_validation_args', 'build_plan_execution', 'build_plan_execution_readiness', '_has_plan_arg', '_safe_evidence_stem', '_script_command', '_render_plan_script', '_write_plan_script', '_artifact_file_info', '_resolve_artifact_manifest_path', '_expected_plan_script_payloads', '_verify_plan_script_payloads', '_require_plan_output_dir_writable', 'write_plan_artifacts', 'run_verify_plan_artifacts', 'verify_plan_artifacts_to_prometheus', 'run_export_dead_letters', 'dead_letter_export_to_prometheus']


def _plan_arg(name: str, value: Any) -> str:
    return f'--{name}={value}'


def _append_plan_arg(args_out: list[str], name: str, value: Any) -> None:
    if value not in (None, ''):
        args_out.append(_plan_arg(name, value))


def build_plan_command_base_args(args: argparse.Namespace, *, start_seq: int, end_seq: int) -> list[str]:
    out: list[str] = [
        _plan_arg('metaserver', args.metaserver),
        _plan_arg('namespace', args.namespace),
        _plan_arg('table', args.table),
        _plan_arg('source-prefix', args.source_prefix),
        _plan_arg('raw-backend', normalize_raw_backend(args.raw_backend)),
        _plan_arg('start-seq', start_seq),
        _plan_arg('end-seq', end_seq),
        _plan_arg('batch-size', args.batch_size),
        _plan_arg('source-scan-max-empty-shards', args.source_scan_max_empty_shards),
    ]
    _append_plan_arg(out, 'library-path', getattr(args, 'library_path', ''))
    _append_plan_arg(out, 'local-kv', getattr(args, 'local_kv', ''))
    if getattr(args, 'partial', False):
        out.append('--partial=1')
    for option_name, attr_name in [
        ('partial-record-types', 'partial_record_types'),
        ('partial-tenant-ids', 'partial_tenant_ids'),
        ('partial-user-ids', 'partial_user_ids'),
        ('partial-session-ids', 'partial_session_ids'),
        ('partial-filter-json', 'partial_filter_json'),
    ]:
        _append_plan_arg(out, option_name, getattr(args, attr_name, ''))
    if not bool(getattr(args, 'partial_require_bounded', True)):
        out.append('--partial-require-bounded=0')
    if not bool(getattr(args, 'resume', True)):
        out.append('--resume=0')
    _append_plan_arg(out, 'confirm-resume-range-change', getattr(args, 'confirm_resume_range_change', ''))
    if bool(getattr(args, 'fail_fast', False)):
        out.append('--fail-fast')
    return out


def build_plan_active_args(args: argparse.Namespace) -> list[str]:
    out = [_plan_arg('active-prefix-key', args.active_prefix_key)]
    _append_plan_arg(out, 'expect-active-prefix', getattr(args, 'expect_active_prefix', ''))
    _append_plan_arg(out, 'repair-active-prefix', getattr(args, 'repair_active_prefix', ''))
    _append_plan_arg(out, 'confirm-no-active-prefix-precondition', getattr(args, 'confirm_no_active_prefix_precondition', ''))
    return out


def build_plan_validation_args(args: argparse.Namespace) -> list[str]:
    out = [
        _plan_arg('validation-strict', 1 if bool(getattr(args, 'validation_strict', True)) else 0),
        _plan_arg('skip-validation', 1 if bool(getattr(args, 'skip_validation', False)) else 0),
    ]
    _append_plan_arg(out, 'confirm-skip-validation', getattr(args, 'confirm_skip_validation', ''))
    _append_plan_arg(out, 'confirm-non-strict-validation', getattr(args, 'confirm_non_strict_validation', ''))
    _append_plan_arg(out, 'confirm-unvalidated-target-state', getattr(args, 'confirm_unvalidated_target_state', ''))
    return out


def build_plan_execution(args: argparse.Namespace, windows: list[Json]) -> Json:
    requested_parallelism = max(1, int(getattr(args, 'plan_parallelism', 1) or 1))
    local_kv_serialized = bool(str(getattr(args, 'local_kv', '') or ''))
    parallelism = 1 if local_kv_serialized else requested_parallelism
    waves: list[Json] = []
    for offset in range(0, len(windows), parallelism):
        wave_windows = windows[offset:offset + parallelism]
        waves.append({
            'wave': len(waves),
            'window_indexes': [int(window['index']) for window in wave_windows],
            'shadow_command_args': [window['shadow_command_args'] for window in wave_windows],
            'validate_command_args': [window['validate_command_args'] for window in wave_windows],
        })
    return {
        'plan_parallelism': parallelism,
        'requested_plan_parallelism': requested_parallelism,
        'local_kv_serialized': local_kv_serialized,
        'shadow_validation_waves': waves,
        'promotion_sequence': [
            {
                'order': index,
                'window_index': int(window['index']),
                'incremental_repair_command_args': window['incremental_repair_command_args'],
            }
            for index, window in enumerate(windows)
        ],
        'execution_order': [
            'run each shadow_validation_waves[].shadow_command_args group concurrently up to plan_parallelism',
            'run each shadow_validation_waves[].validate_command_args group after its shadow wave finishes',
            'run promotion_sequence[].incremental_repair_command_args one at a time in order',
        ],
    }


def build_plan_execution_readiness(chunk_plan: Json) -> Json:
    if not chunk_plan.get('enabled'):
        return {
            'ready': False,
            'status': 'disabled',
            'blockers': ['chunk_plan_disabled'],
            'total_windows': int(chunk_plan.get('total_windows', 0) or 0),
            'emitted_windows': int(chunk_plan.get('emitted_windows', 0) or 0),
            'coverage_record_count': 0,
        }
    windows = [window for window in (chunk_plan.get('windows') or []) if isinstance(window, dict)]
    blockers: list[str] = []
    total_windows = int(chunk_plan.get('total_windows', 0) or 0)
    emitted_windows = int(chunk_plan.get('emitted_windows', len(windows)) or 0)
    truncated = bool(chunk_plan.get('truncated'))
    if truncated:
        blockers.append('chunk_plan_truncated')
    if total_windows != emitted_windows:
        blockers.append('emitted_window_count_mismatch')
    if not windows and total_windows > 0:
        blockers.append('no_emitted_windows')
    expected_start: int | None = None
    expected_end: int | None = None
    coverage_record_count = 0
    contiguous = True
    for index, window in enumerate(windows):
        start = int(window.get('start_seq', 0) or 0)
        end = int(window.get('end_seq', start) or start)
        if index == 0:
            expected_start = start
        elif expected_end is not None and start != expected_end:
            contiguous = False
        if end < start:
            blockers.append(f'window_{index}_negative_range')
        coverage_record_count += max(0, end - start)
        expected_end = end
    if windows and not contiguous:
        blockers.append('window_ranges_not_contiguous')
    execution_plan = chunk_plan.get('execution_plan') if isinstance(chunk_plan.get('execution_plan'), dict) else {}
    waves = [wave for wave in (execution_plan.get('shadow_validation_waves') or []) if isinstance(wave, dict)]
    promotion_sequence = [item for item in (execution_plan.get('promotion_sequence') or []) if isinstance(item, dict)]
    if len(promotion_sequence) != len(windows):
        blockers.append('promotion_sequence_length_mismatch')
    wave_window_indexes = [
        int(index)
        for wave in waves
        for index in (wave.get('window_indexes') or [])
    ]
    expected_indexes = [int(window.get('index', offset) or offset) for offset, window in enumerate(windows)]
    if sorted(wave_window_indexes) != expected_indexes:
        blockers.append('shadow_validation_wave_coverage_mismatch')
    ready = not blockers
    return {
        'ready': ready,
        'status': 'ready' if ready else 'blocked',
        'blockers': blockers,
        'total_windows': total_windows,
        'emitted_windows': emitted_windows,
        'truncated': truncated,
        'coverage_start_seq': expected_start,
        'coverage_end_seq': expected_end,
        'coverage_record_count': coverage_record_count,
        'wave_count': len(waves),
        'promotion_step_count': len(promotion_sequence),
        'plan_parallelism': int(execution_plan.get('plan_parallelism', 0) or 0),
        'requested_plan_parallelism': int(execution_plan.get('requested_plan_parallelism', 0) or 0),
        'local_kv_serialized': bool(execution_plan.get('local_kv_serialized')),
        'parallel_write_safety': str(chunk_plan.get('parallel_write_safety') or ''),
    }


def _has_plan_arg(args_list: list[str], name: str) -> bool:
    prefix = f'--{name}='
    flag = f'--{name}'
    return any(arg == flag or arg.startswith(prefix) for arg in args_list)


def _safe_evidence_stem(value: str) -> str:
    cleaned = ''.join(ch if ch.isalnum() or ch in {'-', '_', '.'} else '_' for ch in value)
    return cleaned.strip('._') or 'command'


def _script_command(args_list: list[str], *, evidence_stem: str = '') -> str:
    command = ['python3', 'tools/matrixark_context_backfill.py', *args_list]
    rendered = ' '.join(shlex.quote(part) for part in command)
    if evidence_stem:
        stem = _safe_evidence_stem(evidence_stem)
        if not _has_plan_arg(args_list, 'prometheus-output'):
            rendered += f' --prometheus-output="${{PLAN_BUNDLE_DIR}}/execution_evidence/{stem}.prom"'
        rendered += (
            f' > "${{PLAN_BUNDLE_DIR}}/execution_evidence/{stem}.json"'
            f' 2> "${{PLAN_BUNDLE_DIR}}/execution_evidence/{stem}.stderr.log"'
        )
    return rendered


def _render_plan_script(commands: list[list[str]], *, parallel: bool, script_name: str = '') -> str:
    lines = [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        f'cd {shlex.quote(str(ROOT))}',
        'PLAN_BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
        'mkdir -p "${PLAN_BUNDLE_DIR}/execution_evidence"',
        '',
    ]
    if parallel:
        lines.append('pids=()')
        for index, args_list in enumerate(commands):
            stem = f'{script_name or "plan"}_cmd_{index:04d}'
            lines.append(f'{_script_command(args_list, evidence_stem=stem)} &')
            lines.append('pids+=("$!")')
        lines.extend([
            'for pid in "${pids[@]}"; do',
            '  wait "$pid"',
            'done',
        ])
    else:
        for index, args_list in enumerate(commands):
            stem = f'{script_name or "plan"}_cmd_{index:04d}'
            lines.append(_script_command(args_list, evidence_stem=stem))
    return '\n'.join(lines) + '\n'


def _write_plan_script(path: Path, commands: list[list[str]], *, parallel: bool) -> None:
    path.write_text(_render_plan_script(commands, parallel=parallel, script_name=path.stem), encoding='utf-8')
    path.chmod(0o755)


def _artifact_file_info(path: Path, *, output_dir: Path | None = None) -> Json:
    payload = path.read_bytes()
    info: Json = {
        'path': str(path),
        'size_bytes': len(payload),
        'sha256': hashlib.sha256(payload).hexdigest(),
        'executable': bool(path.stat().st_mode & 0o111),
    }
    if output_dir is not None:
        try:
            info['relative_path'] = str(path.relative_to(output_dir))
        except ValueError:
            info['relative_path'] = path.name
    return info


def _resolve_artifact_manifest_path(item: Json, output_dir: Path) -> tuple[Path, str]:
    relative_path = str(item.get('relative_path') or '')
    if relative_path:
        path = Path(relative_path)
        if path.is_absolute() or '..' in path.parts:
            return output_dir / path, 'unsafe_relative_path'
        return output_dir / path, 'relative_path'
    path = Path(str(item.get('path') or ''))
    if not path.is_absolute():
        return output_dir / path, 'path_relative_to_output_dir'
    return path, 'absolute_path'


def _expected_plan_script_payloads(plan: Json) -> dict[str, str]:
    chunk_plan = plan.get('chunk_plan') if isinstance(plan.get('chunk_plan'), dict) else {}
    execution_plan = chunk_plan.get('execution_plan') if isinstance(chunk_plan.get('execution_plan'), dict) else {}
    expected: dict[str, str] = {}
    for wave in execution_plan.get('shadow_validation_waves') or []:
        if not isinstance(wave, dict):
            continue
        wave_id = int(wave.get('wave', len(expected)) or 0)
        shadow_commands = [
            command for command in (wave.get('shadow_command_args') or [])
            if isinstance(command, list)
        ]
        validate_commands = [
            command for command in (wave.get('validate_command_args') or [])
            if isinstance(command, list)
        ]
        shadow_name = f'shadow_wave_{wave_id:04d}'
        validate_name = f'validate_wave_{wave_id:04d}'
        expected[f'{shadow_name}.sh'] = _render_plan_script(shadow_commands, parallel=True, script_name=shadow_name)
        expected[f'{validate_name}.sh'] = _render_plan_script(validate_commands, parallel=True, script_name=validate_name)
    promotion_commands = [
        item.get('incremental_repair_command_args')
        for item in (execution_plan.get('promotion_sequence') or [])
        if isinstance(item, dict) and isinstance(item.get('incremental_repair_command_args'), list)
    ]
    if promotion_commands:
        expected['promote_serial.sh'] = _render_plan_script(promotion_commands, parallel=False, script_name='promote_serial')
    return expected


def _verify_plan_script_payloads(output_dir: Path) -> tuple[bool, list[Json], list[str]]:
    plan_path = output_dir / 'plan.json'
    if not plan_path.exists():
        return False, [], ['plan.json not found for script semantic verification']
    try:
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        return False, [], [f'invalid plan.json for script semantic verification: {exc}']
    if not isinstance(plan, dict):
        return False, [], ['plan.json is not an object']
    expected = _expected_plan_script_payloads(plan)
    checks: list[Json] = []
    errors: list[str] = []
    for relative_path, expected_payload in sorted(expected.items()):
        path = output_dir / relative_path
        exists = path.exists()
        actual_payload = path.read_text(encoding='utf-8') if exists else ''
        matches = exists and actual_payload == expected_payload
        checks.append({
            'relative_path': relative_path,
            'exists': exists,
            'matches_plan': matches,
            'expected_sha256': hashlib.sha256(expected_payload.encode('utf-8')).hexdigest(),
            'actual_sha256': hashlib.sha256(actual_payload.encode('utf-8')).hexdigest() if exists else '',
        })
        if not matches:
            errors.append(f'generated plan script does not match plan.json execution arguments: {relative_path}')
    return bool(expected) and all(bool(item.get('matches_plan')) for item in checks), checks, errors


def _require_plan_output_dir_writable(args: argparse.Namespace, output_dir: Path) -> None:
    if not output_dir.exists():
        return
    existing = [item for item in output_dir.iterdir() if item.name not in {'.', '..'}]
    if existing and getattr(args, 'confirm_plan_output_overwrite', '') != 'YES':
        raise BackfillError(
            f'plan output directory {output_dir} is not empty; '
            'use --confirm-plan-output-overwrite=YES to replace generated plan artifacts'
        )


def write_plan_artifacts(args: argparse.Namespace, summary: Json) -> Json:
    output_dir_arg = str(getattr(args, 'plan_output_dir', '') or '')
    if not output_dir_arg:
        return {}
    output_dir = Path(output_dir_arg)
    _require_plan_output_dir_writable(args, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_plan = summary.get('chunk_plan') if isinstance(summary.get('chunk_plan'), dict) else {}
    execution_plan = chunk_plan.get('execution_plan') if isinstance(chunk_plan.get('execution_plan'), dict) else {}
    shadow_scripts: list[str] = []
    validate_scripts: list[str] = []
    for wave in execution_plan.get('shadow_validation_waves') or []:
        if not isinstance(wave, dict):
            continue
        wave_id = int(wave.get('wave', len(shadow_scripts)) or 0)
        shadow_path = output_dir / f'shadow_wave_{wave_id:04d}.sh'
        validate_path = output_dir / f'validate_wave_{wave_id:04d}.sh'
        _write_plan_script(shadow_path, list(wave.get('shadow_command_args') or []), parallel=True)
        _write_plan_script(validate_path, list(wave.get('validate_command_args') or []), parallel=True)
        shadow_scripts.append(str(shadow_path))
        validate_scripts.append(str(validate_path))
    promotion_commands = [
        item.get('incremental_repair_command_args')
        for item in (execution_plan.get('promotion_sequence') or [])
        if isinstance(item, dict) and isinstance(item.get('incremental_repair_command_args'), list)
    ]
    promote_script = ''
    if promotion_commands:
        promote_path = output_dir / 'promote_serial.sh'
        _write_plan_script(promote_path, promotion_commands, parallel=False)
        promote_script = str(promote_path)
    plan_path = output_dir / 'plan.json'
    manifest_path = output_dir / 'artifact_manifest.json'
    artifact_summary = {
        'output_dir': str(output_dir),
        'plan_json': str(plan_path),
        'artifact_manifest': str(manifest_path),
        'shadow_wave_scripts': shadow_scripts,
        'validate_wave_scripts': validate_scripts,
        'promote_serial_script': promote_script,
        'script_cwd': str(ROOT),
        'overwrite_confirmed': getattr(args, 'confirm_plan_output_overwrite', '') == 'YES',
    }
    summary_with_artifacts = dict(summary)
    summary_with_artifacts['plan_artifacts'] = artifact_summary
    plan_path.write_text(json.dumps(summary_with_artifacts, sort_keys=True, indent=2), encoding='utf-8')
    artifact_paths = [
        plan_path,
        *[Path(path) for path in shadow_scripts],
        *[Path(path) for path in validate_scripts],
        *([Path(promote_script)] if promote_script else []),
    ]
    manifest = {
        'manifest_schema': 'matrixark_context_backfill_plan_artifacts_v1',
        'job_id': args.job_id,
        'generated_at_ms': int(time.time() * 1000),
        'output_dir': str(output_dir),
        'files': [_artifact_file_info(path, output_dir=output_dir) for path in artifact_paths],
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding='utf-8')
    artifact_summary['artifact_manifest_sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    return artifact_summary


def run_verify_plan_artifacts(args: argparse.Namespace) -> Json:
    output_dir_arg = str(getattr(args, 'plan_output_dir', '') or '')
    if not output_dir_arg:
        raise BackfillError('verify_plan_artifacts requires --plan-output-dir')
    output_dir = Path(output_dir_arg)
    manifest_path = output_dir / 'artifact_manifest.json'
    checks: Json = {
        'output_dir_exists': output_dir.exists() and output_dir.is_dir(),
        'manifest_found': manifest_path.exists(),
        'manifest_json_valid': False,
        'manifest_schema_supported': False,
        'manifest_job_id_matches': False,
        'all_files_exist': False,
        'all_paths_safe': False,
        'all_file_sizes_match': False,
        'all_file_sha256_match': False,
        'all_executable_bits_match': False,
        'generated_scripts_match_plan': False,
    }
    manifest: Json = {}
    errors: list[str] = []
    if not checks['output_dir_exists']:
        errors.append(f'plan output directory not found: {output_dir}')
    if not checks['manifest_found']:
        errors.append(f'artifact manifest not found: {manifest_path}')
    if checks['manifest_found']:
        try:
            decoded = json.loads(manifest_path.read_text(encoding='utf-8'))
            if isinstance(decoded, dict):
                manifest = decoded
                checks['manifest_json_valid'] = True
            else:
                errors.append('artifact manifest JSON is not an object')
        except json.JSONDecodeError as exc:
            errors.append(f'invalid artifact manifest JSON: {exc}')
    checks['manifest_schema_supported'] = manifest.get('manifest_schema') == 'matrixark_context_backfill_plan_artifacts_v1'
    if manifest and not checks['manifest_schema_supported']:
        errors.append('unsupported artifact manifest schema')
    expected_job_id = str(getattr(args, 'job_id', '') or '')
    manifest_job_id = str(manifest.get('job_id') or '')
    checks['manifest_job_id_matches'] = not expected_job_id or manifest_job_id == expected_job_id
    if expected_job_id and manifest_job_id != expected_job_id:
        errors.append(f'artifact manifest job_id mismatch: expected {expected_job_id}, found {manifest_job_id or "<empty>"}')
    file_checks: list[Json] = []
    files = manifest.get('files') if isinstance(manifest.get('files'), list) else []
    for item in files:
        if not isinstance(item, dict):
            file_checks.append({'status': 'failed', 'error': 'file entry is not an object'})
            continue
        path, path_source = _resolve_artifact_manifest_path(item, output_dir)
        path_safe = path_source != 'unsafe_relative_path'
        exists = path.exists() if path_safe else False
        size_matches = False
        sha_matches = False
        executable_matches = False
        actual_sha = ''
        actual_size = None
        actual_executable = False
        if exists:
            info = _artifact_file_info(path)
            actual_sha = str(info['sha256'])
            actual_size = int(info['size_bytes'])
            actual_executable = bool(info['executable'])
            size_matches = actual_size == int(item.get('size_bytes', -1) or -1)
            sha_matches = actual_sha == str(item.get('sha256') or '')
            executable_matches = actual_executable == bool(item.get('executable'))
        file_checks.append({
            'path': str(path),
            'manifest_path': str(item.get('path') or ''),
            'manifest_relative_path': str(item.get('relative_path') or ''),
            'path_source': path_source,
            'path_safe': path_safe,
            'exists': exists,
            'size_matches': size_matches,
            'sha256_matches': sha_matches,
            'executable_matches': executable_matches,
            'expected_size_bytes': item.get('size_bytes'),
            'actual_size_bytes': actual_size,
            'expected_sha256': item.get('sha256'),
            'actual_sha256': actual_sha,
            'expected_executable': bool(item.get('executable')),
            'actual_executable': actual_executable,
        })
    checks['all_files_exist'] = bool(files) and all(bool(item.get('exists')) for item in file_checks)
    checks['all_paths_safe'] = bool(files) and all(bool(item.get('path_safe')) for item in file_checks)
    checks['all_file_sizes_match'] = bool(files) and all(bool(item.get('size_matches')) for item in file_checks)
    checks['all_file_sha256_match'] = bool(files) and all(bool(item.get('sha256_matches')) for item in file_checks)
    checks['all_executable_bits_match'] = bool(files) and all(bool(item.get('executable_matches')) for item in file_checks)
    for item in file_checks:
        if not item.get('path_safe'):
            errors.append(f'unsafe artifact relative path: {item.get("manifest_relative_path")}')
        elif not item.get('exists'):
            errors.append(f'missing artifact file: {item.get("path")}')
        elif not item.get('size_matches') or not item.get('sha256_matches') or not item.get('executable_matches'):
            errors.append(f'artifact file mismatch: {item.get("path")}')
    script_semantics_match, script_checks, script_errors = _verify_plan_script_payloads(output_dir)
    checks['generated_scripts_match_plan'] = script_semantics_match
    errors.extend(script_errors)
    status = 'ok' if all(bool(value) for value in checks.values()) else 'failed'
    summary = {
        'status': status,
        'mode': 'verify_plan_artifacts',
        'job_id': expected_job_id or manifest_job_id,
        'plan_output_dir': str(output_dir),
        'artifact_manifest': str(manifest_path),
        'artifact_manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest() if manifest_path.exists() else '',
        'manifest_schema': manifest.get('manifest_schema', ''),
        'manifest_file_count': len(files),
        'checks': checks,
        'file_checks': file_checks,
        'script_semantic_checks': script_checks,
        'errors': errors,
    }
    prometheus_output = str(getattr(args, 'prometheus_output', '') or '')
    if prometheus_output:
        Path(prometheus_output).write_text(verify_plan_artifacts_to_prometheus(summary), encoding='utf-8')
    return summary


def verify_plan_artifacts_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'mode': 'verify_plan_artifacts',
    }
    lines = [
        '# HELP matrixark_context_backfill_plan_artifact_verification_status Plan artifact verification status.',
        '# TYPE matrixark_context_backfill_plan_artifact_verification_status gauge',
        f'matrixark_context_backfill_plan_artifact_verification_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_plan_artifact_verification_check Plan artifact verification check result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_plan_artifact_verification_check gauge',
    ]
    checks = summary.get('checks') if isinstance(summary.get('checks'), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(f'matrixark_context_backfill_plan_artifact_verification_check{{{_prom_labels(**base, check=check_name)}}} {1 if passed else 0}')
    lines.extend([
        '# HELP matrixark_context_backfill_plan_artifact_file_check Per-file plan artifact verification result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_plan_artifact_file_check gauge',
    ])
    for item in summary.get('file_checks') or []:
        if not isinstance(item, dict):
            continue
        relative_path = str(item.get('manifest_relative_path') or Path(str(item.get('path') or '')).name)
        for check_name in ['path_safe', 'exists', 'size_matches', 'sha256_matches', 'executable_matches']:
            lines.append(
                f'matrixark_context_backfill_plan_artifact_file_check{{{_prom_labels(**base, file=relative_path, check=check_name)}}} '
                f'{1 if item.get(check_name) else 0}'
            )
    lines.extend([
        '# HELP matrixark_context_backfill_plan_artifact_script_semantic_check Generated plan script semantic check, 1 when script matches plan.json.',
        '# TYPE matrixark_context_backfill_plan_artifact_script_semantic_check gauge',
    ])
    for item in summary.get('script_semantic_checks') or []:
        if not isinstance(item, dict):
            continue
        relative_path = str(item.get('relative_path') or '')
        lines.append(
            f'matrixark_context_backfill_plan_artifact_script_semantic_check{{{_prom_labels(**base, file=relative_path, check="matches_plan")}}} '
            f'{1 if item.get("matches_plan") else 0}'
        )
    return '\n'.join(lines) + '\n'


def run_export_dead_letters(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('export_dead_letters requires --target-prefix')
    raw_backend = normalize_raw_backend(args.raw_backend)
    kv = make_kv(args)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    total = target.count_dead_letters()
    start = max(0, int(getattr(args, 'dead_letter_start', 0) or 0))
    limit = max(0, int(getattr(args, 'dead_letter_limit', 100) or 0))
    rows = target.read_dead_letters(start=start, limit=limit)
    fingerprint = hashlib.sha256()
    for row in rows:
        fingerprint.update(json.dumps(row, sort_keys=True, separators=(',', ':')).encode('utf-8'))
        fingerprint.update(b'\n')
    output_path = str(getattr(args, 'dead_letter_output', '') or '')
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows), encoding='utf-8')
    summary = {
        'status': 'ok',
        'mode': 'export_dead_letters',
        'job_id': args.job_id,
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'dead_letter_total': total,
        'dead_letter_start': start,
        'dead_letter_limit': limit,
        'exported_count': len(rows),
        'has_more': start + len(rows) < total,
        'next_start': start + len(rows) if start + len(rows) < total else None,
        'dead_letter_fingerprint': fingerprint.hexdigest(),
        'dead_letter_output': output_path,
        'dead_letters': rows,
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(dead_letter_export_to_prometheus(summary), encoding='utf-8')
    return summary


def dead_letter_export_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'target_prefix': str(summary.get('target_prefix') or ''),
        'mode': 'export_dead_letters',
    }
    fingerprint = str(summary.get('dead_letter_fingerprint') or '')
    lines = [
        '# HELP matrixark_context_backfill_dead_letter_export_status Dead-letter export status.',
        '# TYPE matrixark_context_backfill_dead_letter_export_status gauge',
        f'matrixark_context_backfill_dead_letter_export_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_dead_letter_export_records Dead-letter export record counts.',
        '# TYPE matrixark_context_backfill_dead_letter_export_records gauge',
        f'matrixark_context_backfill_dead_letter_export_records{{{_prom_labels(**base, kind="total")}}} {int(summary.get("dead_letter_total", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_records{{{_prom_labels(**base, kind="exported")}}} {int(summary.get("exported_count", 0) or 0)}',
        '# HELP matrixark_context_backfill_dead_letter_export_page Dead-letter export pagination state.',
        '# TYPE matrixark_context_backfill_dead_letter_export_page gauge',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="start")}}} {int(summary.get("dead_letter_start", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="limit")}}} {int(summary.get("dead_letter_limit", 0) or 0)}',
        f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="has_more")}}} {1 if summary.get("has_more") else 0}',
        '# HELP matrixark_context_backfill_dead_letter_export_fingerprint_info Stable fingerprint of exported dead-letter rows.',
        '# TYPE matrixark_context_backfill_dead_letter_export_fingerprint_info gauge',
        f'matrixark_context_backfill_dead_letter_export_fingerprint_info{{{_prom_labels(**base, fingerprint=fingerprint)}}} 1',
    ]
    next_start = summary.get('next_start')
    if next_start is not None:
        lines.append(f'matrixark_context_backfill_dead_letter_export_page{{{_prom_labels(**base, field="next_start")}}} {int(next_start)}')
    return '\n'.join(lines) + '\n'


