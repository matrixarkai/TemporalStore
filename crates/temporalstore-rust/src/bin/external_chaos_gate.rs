use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone)]
struct GateOptions {
    root: PathBuf,
    profile: ChaosProfile,
    cargo: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChaosProfile {
    Quick,
    Full,
}

#[derive(Debug, Serialize)]
struct ExternalChaosGateReport {
    root: String,
    profile: ChaosProfile,
    production_ready_slice: bool,
    scenario_count: usize,
    passed_count: usize,
    failed_count: usize,
    scenarios: Vec<ChaosScenarioReport>,
}

#[derive(Debug, Serialize)]
struct ChaosScenarioReport {
    name: String,
    command: Vec<String>,
    passed: bool,
    exit_code: Option<i32>,
    elapsed_ms: u128,
    stdout_tail: Vec<String>,
    stderr_tail: Vec<String>,
}

#[derive(Debug, Clone)]
struct ChaosScenario {
    name: &'static str,
    binary: &'static str,
    args: Vec<String>,
}

fn main() {
    let options = parse_options();
    std::fs::create_dir_all(&options.root).expect("failed to create chaos gate root");

    let reports = scenario_plan(&options)
        .into_iter()
        .map(|scenario| run_scenario(&options, scenario))
        .collect::<Vec<_>>();
    let passed_count = reports.iter().filter(|report| report.passed).count();
    let failed_count = reports.len().saturating_sub(passed_count);
    let report = ExternalChaosGateReport {
        root: options.root.display().to_string(),
        profile: options.profile,
        production_ready_slice: failed_count == 0,
        scenario_count: reports.len(),
        passed_count,
        failed_count,
        scenarios: reports,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("chaos report must serialize")
    );
    if !report.production_ready_slice {
        std::process::exit(1);
    }
}

fn scenario_plan(options: &GateOptions) -> Vec<ChaosScenario> {
    let mut scenarios = vec![
        ChaosScenario {
            name: "os_process_raft_kill_restart_partition_lag_rolling",
            binary: "raft_secondary_replication_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("raft-secondary").display().to_string(),
                "--heartbeat-ms".to_string(),
                "25".to_string(),
            ],
        },
        ChaosScenario {
            name: "networked_raft_membership_external_snapshot",
            binary: "distributed_raft_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("distributed-raft").display().to_string(),
                "--key".to_string(),
                "external-chaos-key".to_string(),
                "--value".to_string(),
                "external-chaos-value".to_string(),
            ],
        },
        ChaosScenario {
            name: "metaserver_raft_membership_failover_snapshot",
            binary: "metaserver_raft_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("metaserver-raft").display().to_string(),
            ],
        },
        ChaosScenario {
            name: "storage_sync_async_replay_and_raft_wal_restore",
            binary: "storage_modes_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("storage-modes").display().to_string(),
                "--async-flush-limit".to_string(),
                "4".to_string(),
            ],
        },
        ChaosScenario {
            name: "storage_dump_load_fault_matrix",
            binary: "storage_fault_matrix_harness",
            args: vec![
                "--root".to_string(),
                options
                    .root
                    .join("storage-fault-matrix")
                    .display()
                    .to_string(),
            ],
        },
        ChaosScenario {
            name: "storage_production_corpus_shared_store_raft",
            binary: "storage_production_harness",
            args: vec![
                "--root".to_string(),
                options
                    .root
                    .join("storage-production")
                    .display()
                    .to_string(),
            ],
        },
        ChaosScenario {
            name: "external_packet_loss_partition_heal",
            binary: "raft_secondary_replication_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("packet-loss").display().to_string(),
                "--heartbeat-ms".to_string(),
                "25".to_string(),
            ],
        },
        ChaosScenario {
            name: "external_disk_pressure_storage_faults",
            binary: "storage_fault_matrix_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("disk-pressure").display().to_string(),
            ],
        },
        ChaosScenario {
            name: "external_process_chaos_restart_failover",
            binary: "raft_secondary_replication_harness",
            args: vec![
                "--root".to_string(),
                options.root.join("process-chaos").display().to_string(),
                "--heartbeat-ms".to_string(),
                "25".to_string(),
            ],
        },
    ];

    if options.profile == ChaosProfile::Full {
        scenarios.push(ChaosScenario {
            name: "local_scale_failover_shared_store_pressure",
            binary: "scale_harness",
            args: vec![
                "--nodes".to_string(),
                "3".to_string(),
                "--string-ops".to_string(),
                "150".to_string(),
                "--hash-ops".to_string(),
                "50".to_string(),
                "--sequence-keys".to_string(),
                "2".to_string(),
                "--sequence-len".to_string(),
                "100".to_string(),
                "--scale-events".to_string(),
                "1".to_string(),
                "--failover-every".to_string(),
                "50".to_string(),
                "--read-sample-every".to_string(),
                "25".to_string(),
                "--compare-shared-store".to_string(),
                "true".to_string(),
                "--shared-store-ops".to_string(),
                "120".to_string(),
                "--shared-store-flush-every".to_string(),
                "10".to_string(),
                "--shared-store-root".to_string(),
                options
                    .root
                    .join("scale-shared-store")
                    .display()
                    .to_string(),
            ],
        });
    }

    scenarios
}

fn run_scenario(options: &GateOptions, scenario: ChaosScenario) -> ChaosScenarioReport {
    let started = Instant::now();
    let executable = find_binary(scenario.binary);
    let mut command = if let Some(path) = executable {
        let mut command = Command::new(path);
        command.args(&scenario.args);
        command
    } else {
        let mut command = Command::new(&options.cargo);
        command.args([
            "run",
            "-p",
            "temporalstore-rust",
            "--bin",
            scenario.binary,
            "--",
        ]);
        command.args(&scenario.args);
        command
    };
    let command_display = command_display(&command);
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("failed to run {}: {err}", scenario.name));
    report_from_output(
        scenario.name,
        command_display,
        started,
        output.status,
        output.stdout,
        output.stderr,
    )
}

fn report_from_output(
    name: impl Into<String>,
    command: Vec<String>,
    started: Instant,
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> ChaosScenarioReport {
    ChaosScenarioReport {
        name: name.into(),
        command,
        passed: status.success(),
        exit_code: status.code(),
        elapsed_ms: started.elapsed().as_millis(),
        stdout_tail: tail_lines(&stdout, 40),
        stderr_tail: tail_lines(&stderr, 40),
    }
}

fn find_binary(binary: &str) -> Option<PathBuf> {
    let current = std::env::current_exe().ok()?;
    let dir = current.parent()?;
    let candidate = dir.join(binary_with_suffix(binary));
    candidate.exists().then_some(candidate)
}

fn binary_with_suffix(binary: &str) -> String {
    if cfg!(windows) {
        format!("{binary}.exe")
    } else {
        binary.to_string()
    }
}

fn command_display(command: &Command) -> Vec<String> {
    let mut parts = vec![command.get_program().to_string_lossy().to_string()];
    parts.extend(
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string()),
    );
    parts
}

fn tail_lines(bytes: &[u8], limit: usize) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text
        .lines()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    if lines.len() > limit {
        lines.drain(0..lines.len() - limit);
    }
    lines
}

fn parse_options() -> GateOptions {
    let mut root = std::env::temp_dir().join(format!("temporalstore-external-chaos-{}", now_ms()));
    let mut profile = ChaosProfile::Quick;
    let mut cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--root" => {
                let Some(raw) = args.next() else {
                    usage_and_exit();
                };
                root = PathBuf::from(raw);
            }
            "--profile" => {
                let Some(raw) = args.next() else {
                    usage_and_exit();
                };
                profile = match raw.as_str() {
                    "quick" => ChaosProfile::Quick,
                    "full" => ChaosProfile::Full,
                    _ => usage_and_exit(),
                };
            }
            "--cargo" => {
                let Some(raw) = args.next() else {
                    usage_and_exit();
                };
                cargo = raw;
            }
            "--help" | "-h" => usage_and_exit(),
            _ => usage_and_exit(),
        }
    }
    GateOptions {
        root,
        profile,
        cargo,
    }
}

fn usage_and_exit() -> ! {
    eprintln!("usage: external_chaos_gate [--root <path>] [--profile quick|full] [--cargo <path>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn quick_plan_covers_external_process_and_storage_chaos() {
        let options = GateOptions {
            root: Path::new("/tmp/temporalstore-chaos-test").to_path_buf(),
            profile: ChaosProfile::Quick,
            cargo: "cargo".to_string(),
        };
        let plan = scenario_plan(&options);
        assert_eq!(plan.len(), 3);
        assert!(plan
            .iter()
            .any(|scenario| scenario.binary == "raft_secondary_replication_harness"));
        assert!(plan
            .iter()
            .any(|scenario| scenario.binary == "distributed_raft_harness"));
        assert!(plan
            .iter()
            .any(|scenario| scenario.binary == "storage_modes_harness"));
    }

    #[test]
    fn full_plan_adds_scale_pressure() {
        let options = GateOptions {
            root: Path::new("/tmp/temporalstore-chaos-test").to_path_buf(),
            profile: ChaosProfile::Full,
            cargo: "cargo".to_string(),
        };
        let plan = scenario_plan(&options);
        assert_eq!(plan.len(), 4);
        assert!(plan
            .iter()
            .any(|scenario| scenario.binary == "scale_harness"));
    }

    #[test]
    fn tail_lines_keeps_recent_lines() {
        let lines = tail_lines(b"a\nb\nc\nd\n", 2);
        assert_eq!(lines, vec!["c".to_string(), "d".to_string()]);
    }
}
