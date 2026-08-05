"""Split out of matrixark_context_backfill.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import json
import time
from typing import Any

try:  # package path (tools.matrixark_context_backfill)
    from .matrixark_context_backfill import (
        BackfillError,
        Json,
        MatrixKVBackfillTarget,
        Namespace,
        Path,
        _prom_labels,
        active_prefix_precondition_bypassed,
        build_partial_spec,
        expected_serving_type_counts,
        make_kv,
        normalize_raw_backend,
        require_active_prefix_precondition,
        require_expected_active_prefix,
        require_non_noop_rollback,
        rollback_noop_bypassed,
        run_backfill,
    )
except ImportError:  # top-level path (matrixark_context_backfill)
    from matrixark_context_backfill import (
        BackfillError,
        Json,
        MatrixKVBackfillTarget,
        Namespace,
        Path,
        _prom_labels,
        active_prefix_precondition_bypassed,
        build_partial_spec,
        expected_serving_type_counts,
        make_kv,
        normalize_raw_backend,
        require_active_prefix_precondition,
        require_expected_active_prefix,
        require_non_noop_rollback,
        rollback_noop_bypassed,
        run_backfill,
    )

__all__ = ['validation_to_prometheus', 'build_promotion_readiness', 'validation_audit_fields', 'require_skip_validation_confirmation', 'inspect_activation_target_state', 'require_unvalidated_target_state', 'inspect_rollback_target_state', 'require_rollback_target_state', 'require_non_strict_validation_confirmation', 'empty_activation_confirmed', 'require_non_empty_activation', 'run_validate_shadow', 'LOCAL_RECOVERY_REQUIRED_TYPES', '_context_metric_name', 'build_local_recovery_checks', 'run_local_recovery_report', 'local_recovery_report_to_prometheus', 'run_activate_shadow', 'run_rollback_activation', 'activation_to_prometheus', 'rollback_activation_to_prometheus']


def validation_to_prometheus(validation: Json) -> str:
    job_id = str(validation.get('job_id') or '')
    raw_backend = str(validation.get('raw_backend') or '')
    target_prefix = str(validation.get('target_prefix') or '')
    mode = str(validation.get('mode') or 'validate_shadow')
    base = {
        'job_id': job_id,
        'raw_backend': raw_backend,
        'target_prefix': target_prefix,
        'mode': mode,
    }
    lines = [
        '# HELP matrixark_context_backfill_validation_status Validation status for a shadow target.',
        '# TYPE matrixark_context_backfill_validation_status gauge',
        f'matrixark_context_backfill_validation_status{{{_prom_labels(**base, status=str(validation.get("status") or "unknown"))}}} 1',
        '# HELP matrixark_context_backfill_validation_records Record counts observed during validation.',
        '# TYPE matrixark_context_backfill_validation_records gauge',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="expected")}}} {int(validation.get("expected_records", 0) or 0)}',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="actual")}}} {int(validation.get("actual_records", 0) or 0)}',
        f'matrixark_context_backfill_validation_records{{{_prom_labels(**base, kind="dead_letter")}}} {int(validation.get("dead_letters", 0) or 0)}',
        '# HELP matrixark_context_backfill_validation_check Validation check result, 1 for pass and 0 for fail.',
        '# TYPE matrixark_context_backfill_validation_check gauge',
    ]
    checks = validation.get('checks') if isinstance(validation.get('checks'), dict) else {}
    for check_name, passed in sorted(checks.items()):
        lines.append(f'matrixark_context_backfill_validation_check{{{_prom_labels(**base, check=check_name)}}} {1 if passed else 0}')
    readiness = validation.get('promotion_readiness') if isinstance(validation.get('promotion_readiness'), dict) else {}
    readiness_status = str(readiness.get('status') or 'unknown')
    lines.extend([
        '# HELP matrixark_context_backfill_promotion_readiness_status Whether validation proved a shadow prefix ready for activation or incremental repair.',
        '# TYPE matrixark_context_backfill_promotion_readiness_status gauge',
        f'matrixark_context_backfill_promotion_readiness_status{{{_prom_labels(**base, status=readiness_status)}}} {1 if readiness.get("ready") else 0}',
    ])
    readiness_blockers = readiness.get('blockers') if isinstance(readiness.get('blockers'), list) else []
    for blocker in readiness_blockers:
        lines.append(f'matrixark_context_backfill_promotion_readiness_status{{{_prom_labels(**base, status="blocked", blocker=blocker)}}} 1')
    target_state = validation.get('target_state') if isinstance(validation.get('target_state'), dict) else {}
    target_scan = target_state.get('serving_type_count_scan') if isinstance(target_state.get('serving_type_count_scan'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_validation_target_scan Target serving-record scan stats observed during validation.',
        '# TYPE matrixark_context_backfill_validation_target_scan gauge',
    ])
    for name in ['record_count', 'batch_size', 'batches', 'read_errors', 'missing_records']:
        lines.append(f'matrixark_context_backfill_validation_target_scan{{{_prom_labels(**base, stat=name)}}} {int(target_scan.get(name, 0) or 0)}')
    expected_fingerprint = str(validation.get('expected_serving_record_fingerprint') or '')
    actual_fingerprint = str(validation.get('actual_serving_record_fingerprint') or '')
    lines.extend([
        '# HELP matrixark_context_backfill_validation_serving_record_fingerprint_info Ordered serving-record fingerprints compared during validation.',
        '# TYPE matrixark_context_backfill_validation_serving_record_fingerprint_info gauge',
        f'matrixark_context_backfill_validation_serving_record_fingerprint_info{{{_prom_labels(**base, kind="expected", fingerprint=expected_fingerprint)}}} 1',
        f'matrixark_context_backfill_validation_serving_record_fingerprint_info{{{_prom_labels(**base, kind="actual", fingerprint=actual_fingerprint)}}} 1',
    ])
    source_range = validation.get('source_range') if isinstance(validation.get('source_range'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_validation_source_range Source range boundary used during validation.',
        '# TYPE matrixark_context_backfill_validation_source_range gauge',
    ])
    for name in [
        'effective_start_seq',
        'effective_end_seq',
        'source_high_watermark_seq',
        'source_record_count',
        'discovered_record_count',
        'discovered_start_seq',
        'discovered_high_watermark_seq',
        'scan_hash_max_empty_shards',
    ]:
        value = source_range.get(name)
        if value is not None:
            lines.append(f'matrixark_context_backfill_validation_source_range{{{_prom_labels(**base, boundary=name)}}} {int(value)}')
    lines.extend([
        '# HELP matrixark_context_backfill_validation_source_range_info Source range boolean metadata observed during validation.',
        '# TYPE matrixark_context_backfill_validation_source_range_info gauge',
        f'matrixark_context_backfill_validation_source_range_info{{{_prom_labels(**base, property="source_record_count_estimated")}}} {1 if source_range.get("source_record_count_estimated") else 0}',
        f'matrixark_context_backfill_validation_source_range_info{{{_prom_labels(**base, property="user_bounded_end")}}} {1 if source_range.get("user_bounded_end") else 0}',
        '# HELP matrixark_context_backfill_validation_source_scan_mode Source scan mode observed during validation.',
        '# TYPE matrixark_context_backfill_validation_source_scan_mode gauge',
        f'matrixark_context_backfill_validation_source_scan_mode{{{_prom_labels(**base, scan_mode=str(source_range.get("scan_mode") or "unknown"))}}} 1',
    ])
    return '\n'.join(lines) + '\n'


def build_promotion_readiness(validation: Json) -> Json:
    checks = validation.get('checks') if isinstance(validation.get('checks'), dict) else {}
    blockers = [name for name, passed in sorted(checks.items()) if not bool(passed)]
    ready = validation.get('status') == 'ok' and not blockers
    expected_records = int(validation.get('expected_records', 0) or 0)
    actual_records = int(validation.get('actual_records', 0) or 0)
    dead_letters = int(validation.get('dead_letters', 0) or 0)
    expected_scan = validation.get('expected_scan') if isinstance(validation.get('expected_scan'), dict) else {}
    source_failures = int(expected_scan.get('failed', 0) or 0)
    return {
        'status': 'ready' if ready else 'blocked',
        'ready': ready,
        'action': 'activate_shadow_or_incremental_repair',
        'blockers': blockers,
        'validation_strict': bool(validation.get('validation_strict')),
        'expected_records': expected_records,
        'actual_records': actual_records,
        'dead_letters': dead_letters,
        'source_failures': source_failures,
    }


def validation_audit_fields(validation: Json | None, *, skip_validation: bool = False) -> Json:
    if not isinstance(validation, dict):
        return {
            'validation_status': 'skipped',
            'validation_skipped': True,
            'validation_skip_reason': 'skip_validation_flag' if skip_validation else 'validation_not_run',
            'validation_source_range': {},
            'validation_target_state': {},
        }
    return {
        'validation_status': validation.get('status', 'unknown'),
        'validation_skipped': False,
        'validation_skip_reason': '',
        'validation_source_range': validation.get('source_range', {}),
        'validation_target_state': validation.get('target_state', {}),
    }


def require_skip_validation_confirmation(args: argparse.Namespace, *, mode: str) -> None:
    if not args.skip_validation:
        return
    if getattr(args, 'confirm_skip_validation', '') != 'YES':
        raise BackfillError(f'{mode} with --skip-validation=1 requires --confirm-skip-validation=YES')


def inspect_activation_target_state(args: argparse.Namespace, kv: Any) -> Json:
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    counts, scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    dead_letters = target.count_dead_letters()
    record_count = int(scan.get('record_count', 0) or 0)
    read_errors = int(scan.get('read_errors', 0) or 0)
    missing_records = int(scan.get('missing_records', 0) or 0)
    return {
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'record_count': record_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': counts,
        'serving_record_fingerprint': str(scan.get('serving_record_fingerprint') or ''),
        'serving_type_count_scan': scan,
        'healthy_for_unvalidated_activation': record_count > 0 and dead_letters == 0 and read_errors == 0 and missing_records == 0,
    }


def require_unvalidated_target_state(args: argparse.Namespace, kv: Any, *, mode: str) -> Json:
    if not args.skip_validation:
        return {}
    state = inspect_activation_target_state(args, kv)
    if state['healthy_for_unvalidated_activation']:
        return state
    if getattr(args, 'confirm_unvalidated_target_state', '') == 'YES':
        return state
    raise BackfillError(
        f'{mode} with --skip-validation=1 found an empty or unhealthy target prefix; '
        'run validate_shadow or pass --confirm-unvalidated-target-state=YES to audit the break-glass activation'
    )


def inspect_rollback_target_state(args: argparse.Namespace, kv: Any, target_prefix: str) -> Json:
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=target_prefix, raw_backend=raw_backend)
    counts, scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    dead_letters = target.count_dead_letters()
    record_count = int(scan.get('record_count', 0) or 0)
    read_errors = int(scan.get('read_errors', 0) or 0)
    missing_records = int(scan.get('missing_records', 0) or 0)
    return {
        'target_prefix': target_prefix,
        'raw_backend': raw_backend,
        'record_count': record_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': counts,
        'serving_record_fingerprint': str(scan.get('serving_record_fingerprint') or ''),
        'serving_type_count_scan': scan,
        'healthy_for_rollback': record_count > 0 and dead_letters == 0 and read_errors == 0 and missing_records == 0,
    }


def require_rollback_target_state(args: argparse.Namespace, kv: Any, target_prefix: str) -> Json:
    state = inspect_rollback_target_state(args, kv, target_prefix)
    if state['healthy_for_rollback']:
        return state
    if getattr(args, 'confirm_rollback_target_state', '') == 'YES':
        return state
    raise BackfillError(
        'rollback_activation previous prefix is empty or unhealthy; '
        'restore/validate the target prefix or pass --confirm-rollback-target-state=YES to audit the break-glass rollback'
    )


def require_non_strict_validation_confirmation(args: argparse.Namespace, *, mode: str) -> None:
    if args.skip_validation or args.validation_strict:
        return
    if getattr(args, 'confirm_non_strict_validation', '') != 'YES':
        raise BackfillError(f'{mode} with --validation-strict=0 requires --confirm-non-strict-validation=YES')


def empty_activation_confirmed(args: argparse.Namespace) -> bool:
    return getattr(args, 'confirm_empty_activation', '') == 'YES'


def require_non_empty_activation(args: argparse.Namespace, validation: Json | None) -> None:
    if not isinstance(validation, dict):
        return
    expected_records = int(validation.get('expected_records', 0) or 0)
    actual_records = int(validation.get('actual_records', 0) or 0)
    if expected_records > 0 and actual_records > 0:
        return
    if empty_activation_confirmed(args):
        return
    raise BackfillError(
        'activate_shadow validation found an empty source or target; '
        'pass --confirm-empty-activation=YES only for an explicitly reviewed empty cutover'
    )


def run_validate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('validate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('validate_shadow target-prefix must differ from source-prefix')
    validation_args = Namespace(**vars(args))
    validation_args.mode = 'shadow'
    validation_args.dry_run = True
    validation_args.dry_run_check_target = False
    validation_args.resume = False
    validation_args.prometheus_output = ''
    expected_summary = run_backfill(validation_args)
    kv = make_kv(args)
    raw_backend = normalize_raw_backend(args.raw_backend)
    target = MatrixKVBackfillTarget(kv, prefix=args.target_prefix, raw_backend=raw_backend)
    actual_count = target.count()
    dead_letters = target.count_dead_letters()
    expected_count = int(expected_summary['metrics']['written'])
    expected_type_counts = expected_serving_type_counts(expected_summary['metrics'])
    actual_type_counts, target_scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
    expected_fingerprint = str(expected_summary['metrics'].get('serving_record_fingerprint') or '')
    actual_fingerprint = str(target_scan.get('serving_record_fingerprint') or '')
    target_state: Json = {
        'target_prefix': args.target_prefix,
        'raw_backend': raw_backend,
        'record_count': actual_count,
        'dead_letter_count': dead_letters,
        'serving_type_counts': actual_type_counts,
        'serving_record_fingerprint': actual_fingerprint,
        'serving_type_count_scan': target_scan,
    }
    exact_match = actual_count == expected_count
    enough_records = actual_count >= expected_count
    exact_type_match = actual_type_counts == expected_type_counts
    enough_type_records = all(int(actual_type_counts.get(record_type, 0)) >= int(count) for record_type, count in expected_type_counts.items())
    type_counts_passed = exact_type_match if args.validation_strict else enough_type_records
    fingerprint_match = actual_fingerprint == expected_fingerprint
    target_records_readable = int(target_scan.get('read_errors', 0) or 0) == 0
    passed = (
        (exact_match if args.validation_strict else enough_records)
        and type_counts_passed
        and fingerprint_match
        and target_records_readable
        and dead_letters == 0
        and int(expected_summary['metrics']['failed']) == 0
    )
    summary = {
        'status': 'ok' if passed else 'failed',
        'job_id': args.job_id,
        'mode': 'validate_shadow',
        'source_prefix': args.source_prefix,
        'raw_backend': raw_backend,
        'target_prefix': args.target_prefix,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation_strict': bool(args.validation_strict),
        'expected_records': expected_count,
        'actual_records': actual_count,
        'expected_type_counts': expected_type_counts,
        'actual_type_counts': actual_type_counts,
        'expected_serving_record_fingerprint': expected_fingerprint,
        'actual_serving_record_fingerprint': actual_fingerprint,
        'dead_letters': dead_letters,
        'expected_scan': expected_summary['metrics'],
        'source_range': expected_summary.get('source_range', {}),
        'target_state': target_state,
        'checks': {
            'exact_record_count_match': exact_match,
            'actual_records_at_least_expected': enough_records,
            'exact_serving_type_counts_match': exact_type_match,
            'actual_serving_type_counts_at_least_expected': enough_type_records,
            'serving_record_fingerprint_match': fingerprint_match,
            'target_records_readable': target_records_readable,
            'no_shadow_dead_letters': dead_letters == 0,
            'source_scan_had_no_failures': int(expected_summary['metrics']['failed']) == 0,
        },
    }
    summary['promotion_readiness'] = build_promotion_readiness(summary)
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(validation_to_prometheus(summary), encoding='utf-8')
    return summary


LOCAL_RECOVERY_REQUIRED_TYPES = {
    'context_event': 'recovery:context_event_missing',
    'context_entity': 'recovery:context_entity_missing',
    'context_embedding': 'recovery:context_embedding_missing',
    'context_index': 'recovery:context_index_missing',
    'context_summary': 'recovery:context_summary_missing',
    'context_pack_telemetry': 'recovery:context_pack_telemetry_missing',
}


def _context_metric_name(record_type: str) -> str:
    if record_type == 'context_index':
        return 'context_indexes'
    if record_type == 'context_embedding':
        return 'context_embeddings'
    if record_type == 'context_entity':
        return 'context_entities'
    if record_type == 'context_event':
        return 'context_events'
    if record_type == 'context_summary':
        return 'context_summaries'
    if record_type == 'context_pack_telemetry':
        return 'context_telemetry'
    return f'{record_type}s'


def build_local_recovery_checks(expected_summary: Json, target_state: Json | None, *, raw_backend: str) -> tuple[Json, list[str]]:
    metrics = expected_summary.get('metrics') if isinstance(expected_summary.get('metrics'), dict) else {}
    source_range = expected_summary.get('source_range') if isinstance(expected_summary.get('source_range'), dict) else {}
    expected_records = int(metrics.get('written', 0) or 0)
    blockers: list[str] = []
    checks: Json = {
        'canonical_temporalstore_source': raw_backend in {'temporalstore', 'matrixkv'},
        'source_scan_had_no_failures': int(metrics.get('failed', 0) or 0) == 0,
        'source_has_rebuildable_records': expected_records > 0,
        'raw_source_range_bounded_or_discovered': source_range.get('effective_end_seq') is not None,
    }
    for record_type, blocker in sorted(LOCAL_RECOVERY_REQUIRED_TYPES.items()):
        metric_name = _context_metric_name(record_type)
        checks[f'has_{record_type}'] = int(metrics.get(metric_name, 0) or 0) > 0
        if not checks[f'has_{record_type}']:
            blockers.append(blocker)
    if not checks['canonical_temporalstore_source']:
        blockers.append('recovery:external_raw_source')
    if not checks['source_scan_had_no_failures']:
        blockers.append('recovery:source_scan_failed')
    if not checks['source_has_rebuildable_records']:
        blockers.append('recovery:empty_rebuild_source')
    if not checks['raw_source_range_bounded_or_discovered']:
        blockers.append('recovery:unbounded_source_range')
    if target_state is not None:
        actual_records = int(target_state.get('record_count', 0) or 0)
        actual_fingerprint = str(target_state.get('serving_record_fingerprint') or '')
        expected_fingerprint = str(metrics.get('serving_record_fingerprint') or '')
        target_scan = target_state.get('serving_type_count_scan') if isinstance(target_state.get('serving_type_count_scan'), dict) else {}
        target_read_errors = int(target_scan.get('read_errors', 0) or 0)
        target_missing = int(target_scan.get('missing_records', 0) or 0)
        target_dead_letters = int(target_state.get('dead_letter_count', 0) or 0)
        checks.update({
            'target_records_readable': target_read_errors == 0 and target_missing == 0,
            'target_has_no_dead_letters': target_dead_letters == 0,
            'target_record_count_matches_rebuild': actual_records == expected_records,
            'target_fingerprint_matches_rebuild': bool(expected_fingerprint) and actual_fingerprint == expected_fingerprint,
        })
        if not checks['target_records_readable']:
            blockers.append('recovery:target_records_unreadable')
        if not checks['target_has_no_dead_letters']:
            blockers.append('recovery:target_dead_letters_present')
        if not checks['target_record_count_matches_rebuild']:
            blockers.append('recovery:target_record_count_mismatch')
        if not checks['target_fingerprint_matches_rebuild']:
            blockers.append('recovery:target_fingerprint_mismatch')
    else:
        checks.update({
            'target_records_readable': None,
            'target_has_no_dead_letters': None,
            'target_record_count_matches_rebuild': None,
            'target_fingerprint_matches_rebuild': None,
        })
    return checks, blockers


def run_local_recovery_report(args: argparse.Namespace) -> Json:
    """Fail-closed local non-Raft recovery report from TemporalStore-owned data."""
    raw_backend = normalize_raw_backend(args.raw_backend)
    validation_args = Namespace(**vars(args))
    validation_args.mode = 'shadow'
    validation_args.dry_run = True
    validation_args.dry_run_check_target = False
    validation_args.resume = False
    validation_args.prometheus_output = ''
    expected_summary = run_backfill(validation_args)
    kv = make_kv(args)
    target_prefix = str(getattr(args, 'target_prefix', '') or '')
    if not target_prefix and getattr(args, 'active_prefix_key', ''):
        target_prefix = kv.get_string(args.active_prefix_key)
    target_state = None
    if target_prefix:
        target = MatrixKVBackfillTarget(kv, prefix=target_prefix, raw_backend=raw_backend)
        counts, scan = target.serving_type_counts_with_stats(batch_size=max(1, int(args.batch_size)))
        target_state = {
            'target_prefix': target_prefix,
            'raw_backend': raw_backend,
            'record_count': int(scan.get('record_count', 0) or 0),
            'dead_letter_count': target.count_dead_letters(),
            'serving_type_counts': counts,
            'serving_record_fingerprint': str(scan.get('serving_record_fingerprint') or ''),
            'serving_type_count_scan': scan,
        }
    checks, blockers = build_local_recovery_checks(expected_summary, target_state, raw_backend=raw_backend)
    ready = not blockers
    metrics = expected_summary.get('metrics') if isinstance(expected_summary.get('metrics'), dict) else {}
    summary = {
        'status': 'ok' if ready else 'failed',
        'ready': ready,
        'mode': 'local_recovery_report',
        'recovery_model': 'non_raft_local_reopen_rebuild',
        'source_of_truth': f'{raw_backend}:raw_ingestion_or_serving_log',
        'job_id': args.job_id,
        'source_prefix': args.source_prefix,
        'target_prefix': target_prefix,
        'raw_backend': raw_backend,
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'source_range': expected_summary.get('source_range', {}),
        'expected_rebuild_metrics': metrics,
        'target_state': target_state or {},
        'checks': checks,
        'blockers': blockers,
        'serving_layers': {
            'events': int(metrics.get('context_events', 0) or 0),
            'entities': int(metrics.get('context_entities', 0) or 0),
            'embeddings': int(metrics.get('context_embeddings', 0) or 0),
            'secondary_indexes': int(metrics.get('context_indexes', 0) or 0),
            'summaries': int(metrics.get('context_summaries', 0) or 0),
            'retrieval_telemetry': int(metrics.get('context_telemetry', 0) or 0),
            'retrieval_audits': int(metrics.get('context_audits', 0) or 0),
        },
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(local_recovery_report_to_prometheus(summary), encoding='utf-8')
    return summary


def local_recovery_report_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'mode': 'local_recovery_report',
    }
    lines = [
        '# HELP matrixark_context_local_recovery_status Local non-Raft context recovery readiness status.',
        '# TYPE matrixark_context_local_recovery_status gauge',
        f'matrixark_context_local_recovery_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"))}}} {1 if summary.get("ready") else 0}',
        '# HELP matrixark_context_local_recovery_check Local recovery readiness check result.',
        '# TYPE matrixark_context_local_recovery_check gauge',
    ]
    checks = summary.get('checks') if isinstance(summary.get('checks'), dict) else {}
    for name, value in sorted(checks.items()):
        if value is None:
            continue
        lines.append(f'matrixark_context_local_recovery_check{{{_prom_labels(**base, check=name)}}} {1 if value else 0}')
    lines.extend([
        '# HELP matrixark_context_local_recovery_blocker Local recovery blocker presence.',
        '# TYPE matrixark_context_local_recovery_blocker gauge',
    ])
    blockers = [str(item) for item in (summary.get('blockers') or []) if str(item)]
    if not blockers:
        lines.append(f'matrixark_context_local_recovery_blocker{{{_prom_labels(**base, blocker="none")}}} 0')
    else:
        for blocker in sorted(blockers):
            lines.append(f'matrixark_context_local_recovery_blocker{{{_prom_labels(**base, blocker=blocker)}}} 1')
    lines.extend([
        '# HELP matrixark_context_local_recovery_serving_layers Rebuildable serving records by memory layer.',
        '# TYPE matrixark_context_local_recovery_serving_layers gauge',
    ])
    layers = summary.get('serving_layers') if isinstance(summary.get('serving_layers'), dict) else {}
    for layer, count in sorted(layers.items()):
        lines.append(f'matrixark_context_local_recovery_serving_layers{{{_prom_labels(**base, layer=str(layer))}}} {int(count or 0)}')
    return '\n'.join(lines) + '\n'


def run_activate_shadow(args: argparse.Namespace) -> Json:
    if not args.target_prefix:
        raise BackfillError('activate_shadow requires --target-prefix')
    if args.target_prefix == args.source_prefix:
        raise BackfillError('activate_shadow target-prefix must differ from source-prefix')
    if args.confirm_activate != 'YES':
        raise BackfillError('activate_shadow requires --confirm-activate=YES')
    require_skip_validation_confirmation(args, mode='activate_shadow')
    require_non_strict_validation_confirmation(args, mode='activate_shadow')
    validation: Json | None = None
    if not args.skip_validation:
        validation = run_validate_shadow(args)
        if validation.get('status') != 'ok':
            raise BackfillError(f'shadow validation failed: {json.dumps(validation, sort_keys=True)}')
        require_non_empty_activation(args, validation)
    validation_audit = validation_audit_fields(validation, skip_validation=args.skip_validation)
    kv = make_kv(args)
    unvalidated_target_state = require_unvalidated_target_state(args, kv, mode='activate_shadow')
    if unvalidated_target_state:
        validation_audit['validation_target_state'] = unvalidated_target_state
    previous = kv.get_string(args.active_prefix_key)
    require_expected_active_prefix(args, previous)
    require_active_prefix_precondition(args, mode='activate_shadow')
    if args.dry_run:
        summary = {
            'status': 'ok',
            'mode': 'activate_shadow',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
            'previous_prefix': previous,
            'target_prefix': args.target_prefix,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'validation': validation,
            **validation_audit,
            'validation_strict': bool(args.validation_strict),
            'non_strict_validation_confirmed': bool(not args.validation_strict and args.confirm_non_strict_validation == 'YES'),
            'empty_activation_confirmed': empty_activation_confirmed(args),
            'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
            'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        }
        if args.prometheus_output:
            Path(args.prometheus_output).write_text(activation_to_prometheus(summary), encoding='utf-8')
        return summary
    activated_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'activated_at_ms': activated_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'source_prefix': args.source_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'start_seq': args.start_seq,
        'end_seq': args.end_seq,
        'partial': build_partial_spec(args),
        'validation': validation,
        **validation_audit,
        'validation_strict': bool(args.validation_strict),
        'non_strict_validation_confirmed': bool(not args.validation_strict and args.confirm_non_strict_validation == 'YES'),
        'empty_activation_confirmed': empty_activation_confirmed(args),
        'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
    }
    kv.put_string(f'{args.active_prefix_key}:previous:{args.job_id}', previous)
    kv.hset(f'{args.active_prefix_key}:audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, args.target_prefix)
    summary = {
        'status': 'ok',
        'mode': 'activate_shadow',
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'previous_prefix': previous,
        'new_prefix': args.target_prefix,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:audit',
        'job_id': args.job_id,
        'validation': validation,
        **validation_audit,
        'empty_activation_confirmed': empty_activation_confirmed(args),
        'unvalidated_target_state_confirmed': bool(args.skip_validation and args.confirm_unvalidated_target_state == 'YES'),
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(activation_to_prometheus(summary), encoding='utf-8')
    return summary


def run_rollback_activation(args: argparse.Namespace) -> Json:
    rollback_job_id = str(getattr(args, 'rollback_job_id', '') or args.job_id)
    if not rollback_job_id:
        raise BackfillError('rollback_activation requires --rollback-job-id or --job-id')
    if args.confirm_rollback != 'YES':
        raise BackfillError('rollback_activation requires --confirm-rollback=YES')
    kv = make_kv(args)
    previous_key = f'{args.active_prefix_key}:previous:{rollback_job_id}'
    previous_prefix = kv.get_string(previous_key)
    if not previous_prefix:
        raise BackfillError(f'rollback_activation could not find previous prefix at {previous_key}')
    current_prefix = kv.get_string(args.active_prefix_key)
    require_expected_active_prefix(args, current_prefix)
    require_active_prefix_precondition(args, mode='rollback_activation')
    require_non_noop_rollback(args, current_prefix, previous_prefix)
    rollback_target_state = require_rollback_target_state(args, kv, previous_prefix)
    rolled_back_at_ms = int(time.time() * 1000)
    audit = {
        'job_id': args.job_id,
        'rollback_job_id': rollback_job_id,
        'rolled_back_at_ms': rolled_back_at_ms,
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'previous_key': previous_key,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'rollback_target_state': rollback_target_state,
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        'rollback_noop_confirmed': rollback_noop_bypassed(args),
        'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
    }
    if args.dry_run:
        summary = {
            'status': 'ok',
            'mode': 'rollback_activation',
            'dry_run': True,
            'active_prefix_key': args.active_prefix_key,
            'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
            'from_prefix': current_prefix,
            'to_prefix': previous_prefix,
            'rollback_job_id': rollback_job_id,
            'raw_backend': normalize_raw_backend(args.raw_backend),
            'rollback_target_state': rollback_target_state,
            'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
            'rollback_noop_confirmed': rollback_noop_bypassed(args),
            'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
        }
        if args.prometheus_output:
            Path(args.prometheus_output).write_text(rollback_activation_to_prometheus(summary), encoding='utf-8')
        return summary
    kv.hset(f'{args.active_prefix_key}:rollback_audit', args.job_id, json.dumps(audit, sort_keys=True, separators=(',', ':')))
    kv.put_string(args.active_prefix_key, previous_prefix)
    summary = {
        'status': 'ok',
        'mode': 'rollback_activation',
        'active_prefix_key': args.active_prefix_key,
        'expected_active_prefix': str(getattr(args, 'expect_active_prefix', '') or ''),
        'from_prefix': current_prefix,
        'to_prefix': previous_prefix,
        'rollback_job_id': rollback_job_id,
        'raw_backend': normalize_raw_backend(args.raw_backend),
        'audit_key': f'{args.active_prefix_key}:rollback_audit',
        'job_id': args.job_id,
        'rollback_target_state': rollback_target_state,
        'active_prefix_precondition_bypassed': active_prefix_precondition_bypassed(args),
        'rollback_noop_confirmed': rollback_noop_bypassed(args),
        'rollback_target_state_confirmed': bool(getattr(args, 'confirm_rollback_target_state', '') == 'YES'),
    }
    if args.prometheus_output:
        Path(args.prometheus_output).write_text(rollback_activation_to_prometheus(summary), encoding='utf-8')
    return summary


def activation_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'active_prefix_key': str(summary.get('active_prefix_key') or ''),
        'previous_prefix': str(summary.get('previous_prefix') or ''),
        'new_prefix': str(summary.get('new_prefix') or summary.get('target_prefix') or ''),
        'mode': 'activate_shadow',
    }
    lines = [
        '# HELP matrixark_context_backfill_activation_status Shadow activation status.',
        '# TYPE matrixark_context_backfill_activation_status gauge',
        f'matrixark_context_backfill_activation_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"), dry_run=str(bool(summary.get("dry_run"))).lower())}}} 1',
        '# HELP matrixark_context_backfill_activation_validation_status Validation status observed before activation.',
        '# TYPE matrixark_context_backfill_activation_validation_status gauge',
        f'matrixark_context_backfill_activation_validation_status{{{_prom_labels(**base, status=str(summary.get("validation_status") or "unknown"), skipped=str(bool(summary.get("validation_skipped"))).lower())}}} 1',
        '# HELP matrixark_context_backfill_activation_guard_status Activation guard status. Value is 1 when the guard was explicitly bypassed or confirmed.',
        '# TYPE matrixark_context_backfill_activation_guard_status gauge',
        f'matrixark_context_backfill_activation_guard_status{{{_prom_labels(**base, guard="active_prefix_precondition_bypassed")}}} {1 if summary.get("active_prefix_precondition_bypassed") else 0}',
        f'matrixark_context_backfill_activation_guard_status{{{_prom_labels(**base, guard="empty_activation_confirmed")}}} {1 if summary.get("empty_activation_confirmed") else 0}',
        f'matrixark_context_backfill_activation_guard_status{{{_prom_labels(**base, guard="unvalidated_target_state_confirmed")}}} {1 if summary.get("unvalidated_target_state_confirmed") else 0}',
        '# HELP matrixark_context_backfill_activation_target_records Target state record counts observed during activation validation.',
        '# TYPE matrixark_context_backfill_activation_target_records gauge',
    ]
    target_state = summary.get('validation_target_state') if isinstance(summary.get('validation_target_state'), dict) else {}
    lines.append(f'matrixark_context_backfill_activation_target_records{{{_prom_labels(**base, kind="record_count")}}} {int(target_state.get("record_count", 0) or 0)}')
    lines.append(f'matrixark_context_backfill_activation_target_records{{{_prom_labels(**base, kind="dead_letter_count")}}} {int(target_state.get("dead_letter_count", 0) or 0)}')
    source_range = summary.get('validation_source_range') if isinstance(summary.get('validation_source_range'), dict) else {}
    lines.extend([
        '# HELP matrixark_context_backfill_activation_source_range Source range validated before activation.',
        '# TYPE matrixark_context_backfill_activation_source_range gauge',
    ])
    for name in ['effective_start_seq', 'effective_end_seq', 'source_high_watermark_seq', 'source_record_count']:
        value = source_range.get(name)
        if value is not None:
            lines.append(f'matrixark_context_backfill_activation_source_range{{{_prom_labels(**base, boundary=name)}}} {int(value)}')
    return '\n'.join(lines) + '\n'


def rollback_activation_to_prometheus(summary: Json) -> str:
    base = {
        'job_id': str(summary.get('job_id') or ''),
        'rollback_job_id': str(summary.get('rollback_job_id') or ''),
        'raw_backend': str(summary.get('raw_backend') or ''),
        'active_prefix_key': str(summary.get('active_prefix_key') or ''),
        'from_prefix': str(summary.get('from_prefix') or ''),
        'to_prefix': str(summary.get('to_prefix') or ''),
        'mode': 'rollback_activation',
    }
    lines = [
        '# HELP matrixark_context_backfill_rollback_status Activation rollback status.',
        '# TYPE matrixark_context_backfill_rollback_status gauge',
        f'matrixark_context_backfill_rollback_status{{{_prom_labels(**base, status=str(summary.get("status") or "unknown"), dry_run=str(bool(summary.get("dry_run"))).lower())}}} 1',
        '# HELP matrixark_context_backfill_rollback_guard_status Rollback guard status. Value is 1 when the guard was explicitly bypassed or confirmed.',
        '# TYPE matrixark_context_backfill_rollback_guard_status gauge',
        f'matrixark_context_backfill_rollback_guard_status{{{_prom_labels(**base, guard="active_prefix_precondition_bypassed")}}} {1 if summary.get("active_prefix_precondition_bypassed") else 0}',
        f'matrixark_context_backfill_rollback_guard_status{{{_prom_labels(**base, guard="rollback_noop_confirmed")}}} {1 if summary.get("rollback_noop_confirmed") else 0}',
        f'matrixark_context_backfill_rollback_guard_status{{{_prom_labels(**base, guard="rollback_target_state_confirmed")}}} {1 if summary.get("rollback_target_state_confirmed") else 0}',
        '# HELP matrixark_context_backfill_rollback_target_records Previous active-prefix record counts inspected before rollback.',
        '# TYPE matrixark_context_backfill_rollback_target_records gauge',
    ]
    target_state = summary.get('rollback_target_state') if isinstance(summary.get('rollback_target_state'), dict) else {}
    lines.append(f'matrixark_context_backfill_rollback_target_records{{{_prom_labels(**base, kind="record_count")}}} {int(target_state.get("record_count", 0) or 0)}')
    lines.append(f'matrixark_context_backfill_rollback_target_records{{{_prom_labels(**base, kind="dead_letter_count")}}} {int(target_state.get("dead_letter_count", 0) or 0)}')
    lines.extend([
        '# HELP matrixark_context_backfill_rollback_target_health Previous active-prefix rollback health. Value is 1 when healthy.',
        '# TYPE matrixark_context_backfill_rollback_target_health gauge',
        f'matrixark_context_backfill_rollback_target_health{{{_prom_labels(**base)}}} {1 if target_state.get("healthy_for_rollback") else 0}',
    ])
    return '\n'.join(lines) + '\n'


