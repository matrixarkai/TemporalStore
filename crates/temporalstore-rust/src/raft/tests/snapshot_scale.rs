// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the snapshot path COSTS, rather than what it returns.
//!
//! The snapshot carries a state image: the served index plus every slab the engine holds,
//! materialised as owned byte vectors. That payload is sized by the STORE, so anything on this
//! path that copies it rather than moving it costs a whole second shard in resident memory --
//! and one of those copies ran while the cluster write lock was held, which is the half that
//! also costs availability.
//!
//! Measured on this box before the move, with a 128-byte value per record:
//!
//!   records   image bytes   cutting the single install chunk
//!     8,000     1,618,593                            106 ms
//!    80,000    16,482,599                          1,289 ms
//!
//! Cutting one chunk is bookkeeping and should be free. That time was the copy, and it tracked
//! the corpus 12x for 10x.
//!
//! The guard below counts BYTES ALLOCATED rather than reading RSS. On this box RSS is mostly
//! allocator retention and it moves with machine load; an allocation count does neither. It is
//! also written as a DELTA against the same call's own legitimate cost, measured in the same
//! test, so it does not depend on the fixture being large enough to dwarf a fixed baseline.

use super::*;

/// Slab bytes plus index bytes -- what one copy of the image actually costs.
#[cfg(feature = "alloc-probe")]
fn image_bytes(image: &crate::raft::RaftSnapshotStateImage) -> usize {
    image.index_bytes.len() + image.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>()
}

#[cfg(feature = "alloc-probe")]
fn cluster_with_an_image(records: usize) -> RaftCluster {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < records {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
        index += 1;
    }
    cluster
}

/// Cutting the install chunks must not leave a SECOND copy of the state image behind.
///
/// `build_install_snapshot_chunks` legitimately builds one image -- it calls `create_snapshot`
/// itself -- so the question is not whether it allocates, but whether it allocates an image MORE
/// than building that snapshot already costs. That baseline is measured here rather than assumed,
/// so the bound holds at any fixture size: it is a difference between two measurements of the
/// same machine in the same test, not a constant someone has to keep up to date.
///
/// Pre-fix the image was cloned into the single chunk, so the subject cost baseline + one whole
/// image. Post-fix it is moved, so the subject costs about the baseline. The threshold sits
/// halfway between, giving half an image of slack in each direction.
#[cfg(feature = "alloc-probe")]
#[test]
fn cutting_the_install_chunks_does_not_copy_the_state_image() {
    // The probe reads process-global counters, so nothing else may be allocating.
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed despite the alloc-probe feature being on"
    );
    drop(sink);

    let cluster = cluster_with_an_image(400);

    // Warm whatever is lazily built on the first snapshot, so the baseline below measures the
    // steady-state cost rather than one-time setup.
    drop(cluster.create_snapshot().expect("create_snapshot must succeed"));

    // BASELINE: building the snapshot, which is where the one legitimate image comes from.
    let baseline_probe = crate::alloc_probe::Probe::start();
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let baseline_bytes = baseline_probe.stop().alloc_bytes;

    let image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path must be the one under test");
    let one_image = image_bytes(image);

    // DENOMINATORS, printed. The floor is a resolution floor: below it an image-sized copy is
    // not distinguishable from allocator bookkeeping and the assertion proves nothing either way.
    println!("one_image_bytes  = {one_image}");
    println!("baseline_bytes   = {baseline_bytes}");
    assert!(
        one_image >= 8 * 1024,
        "fixture too small to resolve an image-sized copy: image is {one_image} bytes, need 8 KB"
    );
    assert!(
        baseline_bytes > 0,
        "baseline is zero: the probe saw nothing, so the comparison below is vacuous"
    );

    // POSITIVE CONTROL: an image-sized copy is something this probe can actually see. Without it
    // a subject that allocates nothing and a probe that counts nothing look identical.
    let control = crate::alloc_probe::Probe::start();
    let copied = image.clone();
    let control_bytes = control.stop().alloc_bytes;
    println!("control_clone_bytes = {control_bytes}");
    assert!(
        control_bytes >= one_image as u64,
        "the probe must see an image-sized copy: cloning a {one_image}-byte image registered only \
         {control_bytes} bytes"
    );
    drop(copied);
    drop(snapshot);

    // SUBJECT.
    let subject_probe = crate::alloc_probe::Probe::start();
    let chunks = cluster
        .build_install_snapshot_chunks(3, 64)
        .expect("chunking must succeed");
    let subject_bytes = subject_probe.stop().alloc_bytes;
    println!("subject_bytes    = {subject_bytes}");
    println!(
        "subject_minus_baseline = {}",
        subject_bytes as i64 - baseline_bytes as i64
    );

    let ceiling = baseline_bytes + (one_image as u64) / 2;
    assert!(
        subject_bytes < ceiling,
        "cutting the install chunks must not copy the image: it allocated {subject_bytes} bytes \
         against a {baseline_bytes}-byte snapshot build and a {one_image}-byte image, so it added \
         {} bytes where a move adds none (ceiling {ceiling})",
        subject_bytes as i64 - baseline_bytes as i64
    );

    // And the image still ARRIVES. A move that dropped it would also allocate nothing, so this
    // half is what stops the guard above from being satisfiable by losing the payload.
    let delivered = chunks
        .first()
        .expect("an image snapshot is a single chunk")
        .state_image
        .as_ref()
        .expect("the chunk must carry the state image");
    assert_eq!(
        image_bytes(delivered),
        one_image,
        "the chunk must carry the whole image, not a truncated one"
    );
}

/// Runs in the ordinary gate: the moves must not have cost the image its contents, on either
/// side. This sends the image through the send path (which now moves it into the chunk)
/// AND the receive path (which now moves it into the pending buffer) and then reads every key
/// back off the follower that installed it.
///
/// The allocation guard above cannot see this: a path that dropped the image would satisfy it.
#[test]
fn the_state_image_survives_being_moved_through_the_chunk_stream() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < 24 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{index}"),
                value: format!("v{index}").into_bytes(),
            })
            .expect("propose must succeed");
        index += 1;
    }

    let built_image_bytes = {
        let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
        let image = snapshot
            .state_image
            .as_ref()
            .expect("the state-image path must be the one under test");
        image.index_bytes.len() + image.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>()
    };
    // Denominator with a floor: a zero-byte image would make the equality below trivially true.
    println!("built_image_bytes = {built_image_bytes}");
    assert!(
        built_image_bytes > 0,
        "denominator is zero: the fixture built no state image at all"
    );

    let chunks = cluster
        .build_install_snapshot_chunks(3, 8)
        .expect("chunking must succeed");
    let chunk = chunks.into_iter().next().expect("single chunk");
    let carried = chunk
        .state_image
        .as_ref()
        .expect("the chunk must carry the state image after the move");
    let carried_bytes = carried.index_bytes.len()
        + carried.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>();
    assert_eq!(
        carried_bytes, built_image_bytes,
        "the moved image must carry the same bytes the built image had"
    );

    let response = cluster
        .receive_install_snapshot_chunk(chunk)
        .expect("install chunk must be accepted");
    assert!(
        response.snapshot_complete,
        "single-chunk install must complete"
    );

    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let shard_id = inner.shard_id;
    let node3 = inner.nodes.get(&3).expect("follower 3 exists");
    let mut key = 0usize;
    while key < 24 {
        assert_eq!(
            node3
                .engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringGet {
                        key: format!("k{key}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("v{key}").into_bytes())
            },
            "follower must serve k{key} from the image moved through the chunk stream"
        );
        key += 1;
    }
}
