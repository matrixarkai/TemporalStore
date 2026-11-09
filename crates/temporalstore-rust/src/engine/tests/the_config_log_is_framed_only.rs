// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE CONFIG LOG IS FRAMED PROTOBUF ONLY.
//!
//! `shard-<id>.configlog` is where a shard's runtime config is made durable, because that config is
//! not carried in the served-index checkpoint: without it a reload defaults `Config`, which
//! silently resets the flags that change how already-written data is read. Each entry is a
//! length-prefixed, checksummed frame holding a protobuf (a payload may contain a newline, so an
//! entry cannot be a line). There is no JSON arm: the first release carries no installed base, so a
//! JSON line is not an entry, and no test here plants one to prove it still loads. A JSON line is
//! planted only to prove it is NOT read.
#![allow(clippy::all)]
use super::*;
use crate::control::Config;
use crate::engine::{encode_config_log_frame, ConfigLogEntry};
use std::collections::BTreeMap;
use std::io::Write as _;

const SHARD: ShardId = 3;

fn a_loaded_engine() -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    (dir, engine)
}

fn entry(after_seq: u64, config: Config) -> ConfigLogEntry {
    ConfigLogEntry { after_seq, config }
}

fn append(engine: &TemporalEngine, bytes: &[u8]) {
    let path = engine.config_log_path(SHARD);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
}

/// The JSON line this log used to hold. Used only to show it is refused.
fn json_line(entry: &ConfigLogEntry) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(entry).unwrap();
    bytes.push(b'\n');
    assert_eq!(bytes[0], b'{', "the planted line is not the JSON shape");
    bytes
}

/// A config with every field at a value no default would give, so a field the codec drops reads
/// back as its default and the comparison names it.
fn a_config_with_every_field_set(version: u64) -> Config {
    Config {
        version,
        maxmemory_bytes: Some(0),
        // A NEWLINE INSIDE THE PAYLOAD: the reason a binary entry cannot be a line.
        tenant_name: Some("tenant-a\nline-two".to_string()),
        read_qps: Some(11),
        write_qps: None,
        table_read_qps: Some(0),
        table_write_qps: Some(13),
        tenant_read_qps: None,
        tenant_write_qps: Some(15),
        extend_config: BTreeMap::from([
            ("control_rollup".to_string(), "true".to_string()),
            ("zzz_last".to_string(), "\u{e9}tat".to_string()),
        ]),
        feature_max_size: 4242,
        async_storage: true,
    }
}

fn same(a: &ConfigLogEntry, b: &ConfigLogEntry) -> bool {
    a.after_seq == b.after_seq && a.config == b.config
}

#[test]
fn the_config_log_reads_frames_skips_a_json_line_and_a_torn_or_damaged_frame_is_not_misread() {
    let (_dir, engine) = a_loaded_engine();

    // ---- a REAL config change, written by the real writer (a frame).
    let mut first = Config::default();
    first.version = 2;
    first.feature_max_size = 777;
    assert_eq!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: SHARD,
                config: first.clone(),
            })
            .ok,
        true
    );
    let real_frame = std::fs::read(engine.config_log_path(SHARD)).unwrap();
    assert_eq!(
        real_frame[0],
        crate::log_framing::FRAME_MAGIC_V3,
        "the real writer is expected to write a frame"
    );

    // ---- a second frame, planted by hand beside the real writer's output.
    let framed = entry(30, a_config_with_every_field_set(5));
    let frame_bytes = encode_config_log_frame(&framed);
    assert_eq!(frame_bytes[0], crate::log_framing::FRAME_MAGIC_V3);
    assert!(
        frame_bytes.contains(&b'\n'),
        "the frame has no newline inside it, so it does not show why a line cannot carry it"
    );
    append(&engine, &frame_bytes);

    // ---- A JSON LINE BETWEEN TWO FRAMES IS NOT AN ENTRY, AND DOES NOT HIDE THE FRAME AFTER IT.
    let json_entry = entry(40, {
        let mut c = Config::default();
        c.version = 9;
        c.feature_max_size = 31;
        c
    });
    append(&engine, &json_line(&json_entry));
    let after_json = entry(45, {
        let mut c = Config::default();
        c.version = 11;
        c.feature_max_size = 32;
        c
    });
    append(&engine, &encode_config_log_frame(&after_json));

    let read = engine.config_log_entries(SHARD);
    assert_eq!(read.len(), 3, "the JSON line was read, or hid an entry: {read:?}");
    assert_eq!(read[0].config, first, "the real writer's entry read back differently");
    assert!(same(&read[1], &framed), "the framed entry read back differently: {:?}", read[1]);
    assert!(same(&read[2], &after_json), "the frame after the JSON line was lost: {:?}", read[2]);
    assert!(
        read.iter().all(|e| e.config.version != 9),
        "the JSON line was returned as an entry: the JSON arm is back"
    );
    // Present-and-zero is not absent, and neither is a missing option.
    assert_eq!(read[1].config.maxmemory_bytes, Some(0));
    assert_eq!(read[1].config.write_qps, None);

    // ---- a TORN frame: the last 3 bytes of an appended frame are missing. It is dropped, and
    // ---- everything before it is still read.
    let torn_source = entry(50, a_config_with_every_field_set(6));
    let torn = encode_config_log_frame(&torn_source);
    append(&engine, &torn[..torn.len() - 3]);
    let read = engine.config_log_entries(SHARD);
    assert_eq!(read.len(), 3, "a torn tail changed what was read: {read:?}");

    // ---- CONTROL: the same frame whole is read, so the drop above was the missing bytes.
    let path = engine.config_log_path(SHARD);
    let whole_len = std::fs::metadata(&path).unwrap().len() as usize - (torn.len() - 3);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.truncate(whole_len);
    bytes.extend_from_slice(&torn);
    std::fs::write(&path, &bytes).unwrap();
    let read = engine.config_log_entries(SHARD);
    assert_eq!(read.len(), 4);
    assert!(same(&read[3], &torn_source));

    // ---- A DAMAGED frame: one byte INSIDE the tenant name changes. The payload still has its
    // ---- declared length; only the frame's checksum can refuse it. Entries before it are read;
    // ---- the damaged one is NOT returned as a config with a different tenant name.
    let marker = b"tenant-a";
    let at = bytes
        .windows(marker.len())
        .rposition(|w| w == marker)
        .expect("the last frame carries the tenant name");
    let mut damaged = bytes.clone();
    damaged[at + 7] = b'b';
    // A WELL-FORMED FRAME AFTER THE DAMAGED ONE. Its declared length cannot be trusted to step
    // over the frame, so nothing after it is found: reading stops, and this later frame is NOT
    // returned. A reader that resynchronised byte by byte would find it, and would be finding it
    // by luck of where the damage was.
    let later = entry(70, Config::default());
    damaged.extend_from_slice(&encode_config_log_frame(&later));
    std::fs::write(&path, &damaged).unwrap();
    let read = engine.config_log_entries(SHARD);
    assert_eq!(read.len(), 3, "a damaged frame was read, or the log resynchronised past it: {read:?}");
    assert!(
        read.iter().all(|e| e.config.tenant_name.as_deref() != Some("tenant-b\nline-two")),
        "the changed tenant name was returned as a real config"
    );
    // CONTROL: the original bytes, with the same later frame after them, read all five.
    let mut undamaged = bytes.clone();
    undamaged.extend_from_slice(&encode_config_log_frame(&later));
    std::fs::write(&path, &undamaged).unwrap();
    assert_eq!(engine.config_log_entries(SHARD).len(), 5);
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(engine.config_log_entries(SHARD).len(), 4);
}

fn versions(engine: &TemporalEngine) -> Vec<u64> {
    engine
        .config_log_entries(SHARD)
        .iter()
        .map(|entry| entry.config.version)
        .collect()
}

fn set_version(engine: &TemporalEngine, version: u64) {
    let mut config = Config::default();
    config.version = version;
    config.feature_max_size = 100 + version as usize;
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: SHARD,
                config,
            })
            .ok,
        "set_config {version} was refused"
    );
}

/// THE WRITER IS A BINARY FRAME, AND A TORN TAIL DOES NOT SWALLOW THE NEXT ENTRY.
///
/// A frame appended behind a partial FRAME is swallowed by it: the partial frame declares more
/// bytes than it has, so the reader would take the new entry as the rest of the old one, fail its
/// checksum, and stop -- and the entry lost is the NEWEST config. So the writer cuts a torn tail off
/// first; the entry it cuts was never acknowledged, because the caller returns only after the
/// fsync. A complete line of JSON with no terminator is no longer a special case: it is bytes that
/// are not a whole frame, so it is a torn tail like any other and is cut before the next append.
///
/// Driven through the real `set_config` throughout.
#[test]
fn the_config_log_writer_is_binary_and_a_torn_tail_does_not_swallow_the_next_entry() {
    let (_dir, engine) = a_loaded_engine();
    let path = engine.config_log_path(SHARD);
    // The file is named for what it is: it holds frames, not JSON lines.
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some(format!("shard-{SHARD}.configlog").as_str()),
        "the config log is not named for what it holds"
    );

    // ---- the real writer is binary.
    set_version(&engine, 2);
    set_version(&engine, 3);
    let written = std::fs::read(&path).unwrap();
    assert_eq!(
        written[0],
        crate::log_framing::FRAME_MAGIC_V3,
        "the config-log writer did not write a frame"
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&written).is_err(),
        "the config log parses as JSON, so the frame byte proves nothing"
    );
    assert_eq!(versions(&engine), vec![2, 3]);

    // ---- a TORN frame: the last 3 bytes of the version-4 frame are gone. The next write must
    // ---- cut it off, and the entry written AFTER it must be readable.
    set_version(&engine, 4);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.truncate(bytes.len() - 3);
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(versions(&engine), vec![2, 3], "the torn frame was read");
    set_version(&engine, 5);
    assert_eq!(
        versions(&engine),
        vec![2, 3, 5],
        "the entry written after a torn frame was swallowed by it (or the torn frame survived)"
    );

    // ---- a complete JSON line with no terminator: not an entry, and cut like any torn tail.
    let mut open_line = json_line(&entry(0, {
        let mut c = Config::default();
        c.version = 8;
        c
    }));
    open_line.pop();
    append(&engine, &open_line);
    assert_eq!(versions(&engine), vec![2, 3, 5], "an unterminated JSON line was read as an entry");
    set_version(&engine, 6);
    assert_eq!(versions(&engine), vec![2, 3, 5, 6]);
    let after_cut = std::fs::read(&path).unwrap();
    assert!(
        !after_cut.windows(open_line.len()).any(|w| w == open_line.as_slice()),
        "the unterminated JSON line was left in the file instead of being cut"
    );

    // ---- COMMITTED corruption is not cut. The last byte of the version-6 frame changes: the frame
    // ---- is complete, so its checksum is what refuses it, and the writer must leave the file's
    // ---- bytes alone (an operator decides) rather than truncate to it.
    let mut damaged = std::fs::read(&path).unwrap();
    let last = damaged.len() - 1;
    damaged[last] ^= 0x01;
    std::fs::write(&path, &damaged).unwrap();
    assert_eq!(versions(&engine), vec![2, 3, 5], "the damaged frame was read");
    let len_before = damaged.len();
    set_version(&engine, 7);
    let after = std::fs::read(&path).unwrap();
    assert!(
        after.len() > len_before && after.starts_with(&damaged),
        "the writer cut a file that holds committed corruption"
    );
}

/// NO JSON PATH IS LEFT IN THE CONFIG-LOG CODE.
///
/// The tests above read the bytes the writer produced and show a JSON line is not read; this sees
/// an encoder or decoder arm nobody calls YET. It reads the writer, the frame encoder, the payload
/// decoder and the walk with comment lines removed and asserts none names `serde_json`. The CONTROL
/// runs the same scan over a string that does name it, so a scan that could not see the token would
/// fail here instead of passing over a writer.
#[test]
fn no_json_path_is_left_in_the_config_log_code() {
    let source = include_str!("../../engine.rs");
    fn body(source: &str, from: &str, to: &str) -> String {
        let start = source
            .find(from)
            .unwrap_or_else(|| panic!("no declaration {from}"));
        let rest = &source[start..];
        let end = rest[from.len()..]
            .find(to)
            .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
            + from.len();
        rest[..end]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    // control
    assert!(
        body("fn a() {\n// a comment\nlet v = serde_json::to_vec(&x);\n}\nfn b", "fn a(", "fn b")
            .contains("serde_json"),
        "the scan cannot see a serde_json call"
    );

    let writer = body(
        source,
        "pub(super) fn append_config_log_entry(",
        "pub(super) fn config_log_entries(",
    );
    assert!(writer.len() > 200, "the scan found almost nothing");
    assert!(
        writer.contains("encode_config_log_frame("),
        "the writer does not call the frame encoder"
    );
    assert!(
        !writer.contains("serde_json"),
        "the config-log writer still reaches for serde_json: a JSON encode path survives"
    );
    let encoder = body(
        source,
        "pub(super) fn encode_config_log_frame(",
        "\n}\n",
    );
    assert!(!encoder.contains("serde_json"), "the frame encoder names serde_json");
    let decoder = body(
        source,
        "fn decode_config_log_payload(",
        "struct ConfigLogWalk",
    );
    assert!(decoder.len() > 200, "the decoder scan found almost nothing");
    assert!(
        !decoder.contains("serde_json"),
        "the config-log payload decoder still names serde_json: a JSON arm survives"
    );
    let walk = body(source, "fn walk_config_log(", "pub(super) fn encode_config_log_frame(");
    assert!(
        !walk.contains("serde_json") && !walk.contains("unterminated_json"),
        "the config-log walk still special-cases a JSON line"
    );
}
