// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A MANIFEST THAT NAMES A BASE IS NEVER READ AS A WHOLE IMAGE, AND RETENTION KEEPS ITS BASE.
//!
//! A delta manifest carries what changed and names the manifest it builds on. Three things have to
//! hold before one is ever written:
//!
//! 1. Every reader that decodes, digests or installs the index image gets it through
//!    `installable_image`, so a delta is refused in one place instead of being read as an empty or
//!    short image. The source scan pins that no reader reaches around it; its CONTROL is a line that
//!    does, shown to be seen.
//! 2. The reader doors refuse a delta: validate, install preflight.
//! 3. Retention closes the manifests it keeps under `base_manifest_id`, because a delta is not
//!    installable without its base. `parent_manifest_id` is lineage and gets detached; this is not.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

fn engine_with_manifests(dir: &std::path::Path, count: usize) -> (TemporalEngine, Vec<BucketDumpManifest>) {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    let mut manifests = Vec::new();
    for round in 0..count {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("delta-{round}"),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
        let manifest = engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("a manifest persists");
        manifests.push(manifest);
    }
    (engine, manifests)
}

/// A delta built from a real manifest: it names `base`, carries no image, and is the newest.
fn delta_on(base: &BucketDumpManifest, newest: &BucketDumpManifest) -> BucketDumpManifest {
    let mut delta = newest.clone();
    delta.manifest_id = format!("{}-delta", newest.manifest_id);
    delta.base_manifest_id = Some(base.manifest_id.clone());
    delta.index_bytes = Vec::new();
    delta.index_sha256 = String::new();
    delta.index_log_sequence = newest.index_log_sequence + 1;
    delta.created_unix_ms = newest.created_unix_ms + 1;
    delta.checksum = String::new();
    delta.checksum = bucket_dump_io::bucket_dump_manifest_checksum_in_place(&mut delta)
        .expect("a checksum is derived");
    delta
}

#[test]
fn a_whole_manifest_gives_its_image_and_a_delta_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (_engine, manifests) = engine_with_manifests(dir.path(), 2);
    let whole = &manifests[0];
    assert_eq!(installable_image(whole).unwrap(), whole.index_bytes.as_slice());
    let delta = delta_on(&manifests[0], &manifests[1]);
    let refusal = installable_image(&delta).expect_err("a delta carries no whole image");
    assert_eq!(refusal.base_manifest_id, manifests[0].manifest_id);
}

#[test]
fn the_reader_doors_refuse_a_delta() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, manifests) = engine_with_manifests(dir.path(), 2);
    let delta = delta_on(&manifests[0], &manifests[1]);
    let validated = engine
        .validate_bucket_dump_manifest(&delta)
        .expect_err("validation reads the image");
    assert_eq!(validated.code, "slot_dump_delta_unreadable");
    let installed = engine
        .install_bucket_dump_manifest(&delta)
        .expect_err("install reads the image");
    assert_eq!(installed.code, "slot_dump_delta_unreadable");
    let preflight = engine.bucket_dump_install_preflight_report(&delta);
    assert!(!preflight.install_safe, "{preflight:?}");
    assert!(
        preflight
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("delta_manifest_unreadable:")),
        "{:?}",
        preflight.blockers
    );
}

#[test]
fn retention_keeps_the_base_of_the_delta_it_keeps() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, manifests) = engine_with_manifests(dir.path(), 3);
    let delta = delta_on(&manifests[0], &manifests[2]);
    engine
        .persist_bucket_dump_manifest(&delta)
        .expect("the delta persists");
    let plan = engine.bucket_dump_manifest_prune_plan(SHARD);
    assert!(
        plan.retained_manifest_ids.contains(&delta.manifest_id),
        "the newest manifest is kept: {plan:?}"
    );
    assert!(
        plan.retained_manifest_ids.contains(&manifests[0].manifest_id),
        "the delta's base is kept: {plan:?}"
    );
    assert!(
        plan.prunable_manifest_ids.contains(&manifests[1].manifest_id),
        "a manifest nothing builds on is still pruned: {plan:?}"
    );
}

#[test]
fn a_delta_whose_base_is_gone_keeps_every_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, manifests) = engine_with_manifests(dir.path(), 2);
    let mut delta = delta_on(&manifests[0], &manifests[1]);
    delta.base_manifest_id = Some("a-manifest-that-was-never-written".to_string());
    delta.checksum = String::new();
    delta.checksum = bucket_dump_io::bucket_dump_manifest_checksum_in_place(&mut delta).unwrap();
    engine.persist_bucket_dump_manifest(&delta).unwrap();
    let plan = engine.bucket_dump_manifest_prune_plan(SHARD);
    assert!(
        plan.prunable_manifest_ids.is_empty(),
        "a chain that cannot be resolved prunes nothing: {plan:?}"
    );
}

fn uncommented(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Lines that take a manifest's image bytes other than to count or clear them.
fn lines_reaching_around_the_resolver(code: &str) -> Vec<String> {
    code.lines()
        .filter(|line| {
            ["manifest.index_bytes", "partial.index_bytes", "shared.index_bytes"]
                .iter()
                .any(|needle| line.contains(needle))
                && !line.contains(".index_bytes.len()")
                && !line.contains(".index_bytes.clear()")
        })
        .map(|line| line.trim().to_string())
        .collect()
}

fn source_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests") {
                continue;
            }
            source_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_reader_takes_a_manifests_image_around_the_resolver() {
    // CONTROL: the scan sees a reader that bypasses the resolver, and ignores a count.
    assert_eq!(
        lines_reaching_around_the_resolver("let a = decode_index_bytes(&manifest.index_bytes);").len(),
        1
    );
    assert!(lines_reaching_around_the_resolver("let n = manifest.index_bytes.len();").is_empty());

    let mut files = Vec::new();
    source_files(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut files);
    // The wire codec writes and reads the field itself; the resolver is the door. Test files and
    // measurement binaries are not readers.
    let exempt = ["bucket_dump_codec.rs", "bucket_dump_chain.rs", "checkpoint_page_cost.rs"];
    let mut offenders = Vec::new();
    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let path_text = path.to_string_lossy().replace('\\', "/");
        if exempt.contains(&name.as_str())
            || name.contains("test")
            || path_text.contains("/src/bin/")
            || path_text.contains("/raft/")
        {
            continue;
        }
        let code = uncommented(&std::fs::read_to_string(&path).unwrap());
        for line in lines_reaching_around_the_resolver(&code) {
            offenders.push(format!("{name}: {line}"));
        }
    }
    assert!(offenders.is_empty(), "readers that bypass `installable_image`: {offenders:#?}");
}
