// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A cluster that lives in memory, behind the gateway's own plane trait, and a gateway over it.
//! Test support: the tests that need a real door on a socket or in this process serve it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions, ObjectPlane, PlaneFailure};
use matrixobjectstore_rs::object_manifest::{ManifestListPage, ManifestSummary, ObjectExtent, ObjectManifest};
use matrixobjectstore_rs::ObjectError;


#[derive(Default)]
pub(crate) struct Shared {
    pub(crate) objects: BTreeMap<(String, String), (ObjectManifest, Vec<u8>)>,
    /// The next append commits and then reports its publication unconfirmed.
    pub(crate) commit_then_unconfirmed: bool,
    /// The next append is lost before it commits and reports its publication unconfirmed.
    pub(crate) lose_then_unconfirmed: bool,
    pub(crate) appends: usize,
    /// How many unchanged publications (fences) the plane has been asked for.
    pub(crate) fences: usize,
    /// Pages are allocated per attempt: the next chunk id the plane hands out.
    pub(crate) chunks: u64,
}

/// The extent an attempt publishes: its own chunk, which no other attempt shares. It is the
/// attempt's identity, and a plane reports it when a publication is not confirmed.
pub(crate) fn own_extent(shared: &mut Shared, offset: u64, length: u64) -> ObjectExtent {
    shared.chunks += 1;
    ObjectExtent {
        group: "g".into(),
        volume_id: 1,
        chunk_id: shared.chunks,
        object_offset: offset,
        length,
        ..Default::default()
    }
}

pub(crate) struct MemoryPlane(pub(crate) Arc<Mutex<Shared>>);

pub(crate) fn manifest(bucket: &str, key: &str, generation: u64, length: u64, extents: usize) -> ObjectManifest {
    ObjectManifest {
        format_version: 2,
        bucket: bucket.into(),
        key: key.into(),
        generation,
        length,
        extents: (0..extents).map(|_| ObjectExtent::default()).collect(),
        deleted: false,
    }
}

impl ObjectPlane for MemoryPlane {
    fn read_manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        let shared = self.0.lock().unwrap();
        shared
            .objects
            .get(&(bucket.to_string(), key.to_string()))
            .map(|(m, _)| m.clone())
            .ok_or_else(|| ObjectError::NotFound("manifest".into()))
    }

    fn write(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        let mut shared = self.0.lock().unwrap();
        let slot = (bucket.to_string(), key.to_string());
        let current = shared.objects.get(&slot).map(|(m, _)| m.clone());
        if current != expected {
            return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
        }
        let generation = current.map_or(1, |m| m.generation + 1);
        let mut m = manifest(bucket, key, generation, data.len() as u64, 0);
        if !data.is_empty() {
            m.extents = vec![own_extent(&mut shared, 0, data.len() as u64)];
        }
        shared.objects.insert(slot, (m.clone(), data.to_vec()));
        Ok(m)
    }

    fn append(&mut self, expected: ObjectManifest, data: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
        let mut shared = self.0.lock().unwrap();
        shared.appends += 1;
        let unconfirmed = std::mem::take(&mut shared.commit_then_unconfirmed);
        let own = own_extent(&mut shared, expected.length, data.len() as u64);
        if std::mem::take(&mut shared.lose_then_unconfirmed) {
            return Err(PlaneFailure {
                error: ObjectError::PreconditionFailed("the publication is unconfirmed".into()),
                unconfirmed: true,
                attempted: vec![own],
            });
        }
        let slot = (expected.bucket.clone(), expected.key.clone());
        let Some((current, bytes)) = shared.objects.get_mut(&slot) else {
            return Err(ObjectError::NotFound("append target".into()).into());
        };
        if *current != expected {
            return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
        }
        bytes.extend_from_slice(data);
        let mut next = manifest(&expected.bucket, &expected.key, current.generation + 1, bytes.len() as u64, 0);
        next.extents = current.extents.clone();
        next.extents.push(own.clone());
        *current = next;
        let published = current.clone();
        if unconfirmed {
            return Err(PlaneFailure {
                error: ObjectError::PreconditionFailed("the publication is unconfirmed".into()),
                unconfirmed: true,
                attempted: vec![own],
            });
        }
        Ok(published)
    }

    fn publish_unchanged(&mut self, expected: &ObjectManifest) -> Result<ObjectManifest, PlaneFailure> {
        let mut shared = self.0.lock().unwrap();
        shared.fences += 1;
        let slot = (expected.bucket.clone(), expected.key.clone());
        let Some((current, bytes)) = shared.objects.get(&slot).cloned() else {
            return Err(ObjectError::NotFound("manifest".into()).into());
        };
        if &current != expected {
            return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
        }
        let mut next = current;
        next.generation += 1;
        shared.objects.insert(slot, (next.clone(), bytes));
        Ok(next)
    }

    fn read(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let shared = self.0.lock().unwrap();
        match shared.objects.get(&(bucket.to_string(), key.to_string())) {
            Some((m, bytes)) if !m.deleted => Ok((m.clone(), bytes.clone())),
            _ => Err(ObjectError::NotFound("object".into())),
        }
    }

    fn read_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let shared = self.0.lock().unwrap();
        match shared.objects.get(&(bucket.to_string(), key.to_string())) {
            Some((m, bytes)) if !m.deleted => {
                if offset + length > m.length {
                    return Err(ObjectError::InvalidArgument("object read range exceeds length".into()));
                }
                Ok((m.clone(), bytes[offset as usize..(offset + length) as usize].to_vec()))
            }
            _ => Err(ObjectError::NotFound("object".into())),
        }
    }

    fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        let mut shared = self.0.lock().unwrap();
        let slot = (bucket.to_string(), key.to_string());
        let Some((current, _)) = shared.objects.get(&slot).cloned() else {
            return Err(ObjectError::NotFound("manifest".into()));
        };
        let mut tomb = current;
        tomb.generation += 1;
        tomb.length = 0;
        tomb.extents.clear();
        tomb.deleted = true;
        shared.objects.insert(slot, (tomb.clone(), Vec::new()));
        Ok(tomb)
    }

    fn list(&mut self, bucket: &str, after: Option<&str>, limit: u32) -> Result<ManifestListPage, ObjectError> {
        let shared = self.0.lock().unwrap();
        let mut entries: Vec<ManifestSummary> = shared
            .objects
            .iter()
            .filter(|((b, k), _)| b == bucket && after.map_or(true, |a| k.as_str() > a))
            .map(|((b, k), (m, _))| ManifestSummary {
                key: k.clone(),
                generation: m.generation,
                length: m.length,
                deleted: m.deleted,
                bucket: b.clone(),
            })
            .collect();
        let has_more = entries.len() > limit as usize;
        entries.truncate(limit as usize);
        Ok(ManifestListPage {
            version: 1,
            bucket: bucket.into(),
            entries,
            has_more,
            revision: Vec::new(),
        })
    }
}

pub(crate) fn gateway_over(shared: &Arc<Mutex<Shared>>, bucket: &str) -> ClusterGateway {
    let handle = Arc::clone(shared);
    let gateway = ClusterGateway::new(
        move || Ok(Box::new(MemoryPlane(Arc::clone(&handle))) as Box<dyn ObjectPlane>),
        GatewayOptions::default(),
        bucket,
    );
    gateway.ensure_bucket(bucket).expect("test bucket is stored before object requests");
    gateway
}
