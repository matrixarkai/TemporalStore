// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::StorageManagerLoopStageSpan;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
struct Counters {
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64,
    logical_read_bytes: u64,
    logical_write_bytes: u64,
}

impl Counters {
    fn parse(source: &str) -> Option<Self> {
        let mut values = [None; 5];
        for line in source.lines() {
            let (key, value) = line.split_once(':')?;
            let index = match key {
                "read_bytes" => 0,
                "write_bytes" => 1,
                "cancelled_write_bytes" => 2,
                "rchar" => 3,
                "wchar" => 4,
                _ => continue,
            };
            if values[index].replace(value.trim().parse::<u64>().ok()?).is_some() {
                return None;
            }
        }
        Some(Self {
            read_bytes: values[0]?, write_bytes: values[1]?,
            cancelled_write_bytes: values[2]?, logical_read_bytes: values[3]?,
            logical_write_bytes: values[4]?,
        })
    }

    fn delta(self, before: Self) -> Option<Self> {
        Some(Self {
            read_bytes: self.read_bytes.checked_sub(before.read_bytes)?,
            write_bytes: self.write_bytes.checked_sub(before.write_bytes)?,
            cancelled_write_bytes: self.cancelled_write_bytes.checked_sub(before.cancelled_write_bytes)?,
            logical_read_bytes: self.logical_read_bytes.checked_sub(before.logical_read_bytes)?,
            logical_write_bytes: self.logical_write_bytes.checked_sub(before.logical_write_bytes)?,
        })
    }
}

fn read() -> Option<Counters> {
    #[cfg(target_os = "linux")]
    { Counters::parse(&std::fs::read_to_string("/proc/self/io").ok()?) }
    #[cfg(not(target_os = "linux"))]
    { None }
}

/// Benchmark-only process accounting. Disabled rounds do not read procfs or allocate rows.
pub(super) struct StageIoProbe {
    previous: Option<Counters>,
    rows: Option<Vec<Option<Counters>>>,
}

impl StageIoProbe {
    pub(super) fn from_environment() -> Self {
        Self::start(std::env::var_os("TS_COST_SCHEDULER_IO").as_deref()
            == Some(std::ffi::OsStr::new("1")))
    }

    fn start(enabled: bool) -> Self {
        Self { previous: enabled.then(read).flatten(), rows: enabled.then(Vec::new) }
    }

    fn record(&mut self, current: Option<Counters>) {
        if let Some(rows) = self.rows.as_mut() {
            rows.push(current.zip(self.previous).and_then(|(now, before)| now.delta(before)));
            self.previous = current;
        }
    }

    pub(super) fn mark(&mut self) {
        if self.rows.is_some() { self.record(read()); }
    }

    pub(super) fn emit(&self, round: u64, shard_id: u64, spans: &[StorageManagerLoopStageSpan]) {
        let Some(rows) = self.rows.as_ref() else { return; };
        if rows.len() != spans.len() { return; }
        let stages = spans.iter().zip(rows).map(|(span, io)| serde_json::json!({
            "stage": span.stage, "executed": span.executed,
            "duration_us": span.duration_us, "io": io,
        })).collect::<Vec<_>>();
        println!("SCHEDULER_STAGE_IO {}", serde_json::json!({
            "round": round, "shard_id": shard_id, "stages": stages,
            "scope": "process kernel accounting, not device traffic; concurrent work and logical probe reads included; cancelled writes separate; timing includes sampling overhead",
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "rchar: 101\nwchar: 202\nsyscr: 3\nsyscw: 4\nread_bytes: 4096\nwrite_bytes: 8192\ncancelled_write_bytes: 16384\n";

    #[test]
    fn parses_all_counters_without_subtracting_cancelled_writes() {
        let row = Counters::parse(SAMPLE).unwrap();
        assert_eq!(row.read_bytes, 4096);
        assert_eq!(row.write_bytes, 8192);
        assert_eq!(row.cancelled_write_bytes, 16384);
        assert_eq!(row.logical_read_bytes, 101);
        assert_eq!(row.logical_write_bytes, 202);
        assert_eq!(row.delta(row).unwrap().write_bytes, 0);
    }

    #[test]
    fn incomplete_duplicate_or_invalid_counters_are_unmeasured() {
        for field in ["rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes"] {
            let missing = SAMPLE.lines().filter(|line| !line.starts_with(&format!("{field}:")))
                .collect::<Vec<_>>().join("\n");
            assert_eq!(Counters::parse(&missing), None, "{field}");
            assert_eq!(Counters::parse(&format!("{SAMPLE}{field}: 1\n")), None, "{field}");
        }
        assert_eq!(Counters::parse(&SAMPLE.replace("8192", "-1")), None);
        assert_eq!(Counters::parse(&SAMPLE.replace("8192", "18446744073709551616")), None);
    }

    #[test]
    fn a_missing_boundary_does_not_charge_two_stages_to_one() {
        let first = Counters::parse(SAMPLE).unwrap();
        let second = Counters { write_bytes: first.write_bytes + 4096, ..first };
        let mut probe = StageIoProbe { previous: Some(first), rows: Some(Vec::new()) };
        probe.record(None);
        probe.record(Some(second));
        probe.record(Some(second));
        let rows = probe.rows.unwrap();
        assert!(rows[0].is_none() && rows[1].is_none());
        assert_eq!(rows[2].unwrap().write_bytes, 0);
        assert_eq!(first.delta(second), None);
    }

    #[test]
    fn a_disabled_probe_retains_no_rows_or_samples() {
        let mut probe = StageIoProbe::start(false);
        probe.mark();
        assert!(probe.previous.is_none() && probe.rows.is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn separate_boundaries_observe_real_file_writes() {
        use std::io::Write;
        let mut file = tempfile::tempfile().unwrap();
        let mut probe = StageIoProbe::start(true);
        for _ in 0..2 {
            file.write_all(&[7; 16384]).unwrap();
            file.sync_data().unwrap();
            probe.mark();
        }
        for row in probe.rows.unwrap() {
            let row = row.expect("Linux process I/O counters must be readable");
            assert!(row.logical_write_bytes >= 16384, "{row:?}");
        }
    }
}
