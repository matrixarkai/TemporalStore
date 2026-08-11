// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::time::{SystemTime, UNIX_EPOCH};

use crate::control::{Config, ShardInfo};
use crate::types::ShardId;

use super::state::{AdmissionLimit, AdmissionScope, AdmissionState};

pub(super) fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    if let Some(limit) = (if write_command {
        config.write_qps
    } else {
        config.read_qps
    })
    // C++ QuotaManager treats qps == 0 as UNLIMITED (no limiter), not deny-all; only a
    // positive value installs a rate limit.
    .filter(|&limit| limit > 0)
    {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
        .map(|info| info.table_name.trim())
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = (if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        })
        .filter(|&limit| limit > 0)
        {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = (if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        })
        .filter(|&limit| limit > 0)
        {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

pub(super) fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

pub(super) fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

pub(super) fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
