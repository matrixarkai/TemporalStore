// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring, ips_features_from_c_array};
use crate::{IpsFeatureStat, IpsInstance, IpsLastQuery, Result};

impl Client {
    pub fn add_ips_instance(&self, instance: &IpsInstance) -> Result<()> {
        let table = cstring(&instance.table)?;
        let features = instance
            .features
            .iter()
            .map(|feature| CIpsFeatureStat {
                id: feature.id,
                slot: feature.slot,
                has_slot: if feature.has_slot { 1 } else { 0 },
                kind: feature.kind,
                v1: feature.v1,
                v2: feature.v2,
            })
            .collect::<Vec<_>>();
        let features_ptr = if features.is_empty() {
            ptr::null()
        } else {
            features.as_ptr()
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_add_ips_instance(
                self.raw,
                table.as_ptr(),
                instance.uid,
                instance.timestamp_us,
                instance.action_type,
                instance.logical_table,
                features_ptr,
                features.len(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_ips_last_instances(&self, query: &IpsLastQuery) -> Result<Vec<IpsFeatureStat>> {
        let table = cstring(&query.table)?;
        let mut out = CIpsFeatureArray {
            count: 0,
            features: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_ips_last_instances(
                self.raw,
                table.as_ptr(),
                query.uid,
                query.action_type,
                query.logical_table,
                query.slot,
                query.top_k,
                query.last_instances,
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let features = ips_features_from_c_array(&out);
        unsafe { temporalstore_ips_feature_array_free(&mut out) };
        Ok(features)
    }
}
