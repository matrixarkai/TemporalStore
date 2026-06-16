use temporalstore_rust::{production_readiness_report, ProductionReadinessReport};

fn main() {
    let options = parse_gate_options(std::env::args().skip(1));
    let report = production_readiness_report();

    if options.list_services {
        for service in report.known_services() {
            println!("{service}");
        }
        return;
    }

    if options.next_service {
        let Some(gate) = report.next_blocked_service() else {
            println!("null");
            return;
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&gate).expect("next service gate report must serialize")
        );
        eprintln!(
            "production readiness gate failed: next blocked service #{} {} owner={} severity={} next_action={}",
            gate.remediation_order,
            gate.service,
            gate.owner,
            gate.severity,
            gate.next_action
        );
        std::process::exit(2);
    }

    if options.service_reports {
        let gates = report.service_gate_reports();
        println!(
            "{}",
            serde_json::to_string_pretty(&gates).expect("service gate reports must serialize")
        );
        if gates.iter().any(|gate| !gate.ready) {
            eprintln!(
                "production readiness gate failed: {} service(s) still blocked",
                gates.iter().filter(|gate| !gate.ready).count()
            );
            for gate in gates.iter().filter(|gate| !gate.ready) {
                let primary_blocker = gate
                    .primary_blocker
                    .as_ref()
                    .map(|blocker| format!("{}: {}", blocker.area, blocker.capability))
                    .unwrap_or_else(|| "none".to_string());
                eprintln!(
                    "- service #{} {}: owner={}, status={}, severity={}, areas=[{}], {} blocker(s), classes=[{}], primary_blocker={}, next_action={}",
                    gate.remediation_order,
                    gate.service,
                    gate.owner,
                    gate.gate_status,
                    gate.severity,
                    gate.areas.join(","),
                    gate.blocker_count,
                    gate.blocker_classes.join(","),
                    primary_blocker,
                    gate.next_action
                );
            }
            std::process::exit(2);
        }
        return;
    }

    if let Some(service) = options.service_report.as_deref() {
        let Some(gate) = report.service_gate_report(service) else {
            print_unknown_service_error(&report, service);
            std::process::exit(2);
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&gate).expect("service gate report must serialize")
        );
        if !gate.ready {
            eprintln!(
                "production readiness gate failed for service {service}: {} blocker(s)",
                gate.blocker_count
            );
            for blocker in gate.failed_capabilities.iter().take(10) {
                eprintln!("- {}: {}", blocker.area, blocker.capability);
            }
            std::process::exit(2);
        }
        return;
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("readiness report must serialize")
    );

    if let Some(service) = options.service_filter.as_deref() {
        let Some(gate) = report.service_gate_report(service) else {
            print_unknown_service_error(&report, service);
            std::process::exit(2);
        };
        if !gate.ready {
            eprintln!(
                "production readiness gate failed for service {service}: {} blocker(s)",
                gate.blocker_count
            );
            for line in service_failure_lines_for_service(&report, service) {
                eprintln!("{line}");
            }
            for blocker in gate.failed_capabilities.iter().take(10) {
                eprintln!("- {}: {}", blocker.area, blocker.capability);
            }
            std::process::exit(2);
        }
    } else if !report.production_ready {
        eprintln!(
            "production readiness gate failed: {} blocker(s) remain across {} area(s)",
            report.blocker_count,
            report.failed_areas.len()
        );
        for line in service_failure_lines(&report) {
            eprintln!("{line}");
        }
        for blocker in report.exact_failed_capabilities().iter().take(10) {
            eprintln!("- {}: {}", blocker.area, blocker.capability);
        }
        std::process::exit(2);
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct GateOptions {
    service_filter: Option<String>,
    service_report: Option<String>,
    service_reports: bool,
    next_service: bool,
    list_services: bool,
}

fn parse_gate_options(args: impl IntoIterator<Item = String>) -> GateOptions {
    let mut options = GateOptions::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--list-services" {
            options.list_services = true;
            continue;
        }
        if arg == "--next-service" {
            options.next_service = true;
            continue;
        }
        if arg == "--service-reports" {
            options.service_reports = true;
            continue;
        }
        if arg == "--service" {
            options.service_filter = args.next();
            continue;
        }
        if let Some(service) = arg.strip_prefix("--service=") {
            options.service_filter = Some(service.to_string());
            continue;
        }
        if arg == "--service-report" {
            options.service_report = args.next();
            continue;
        }
        if let Some(service) = arg.strip_prefix("--service-report=") {
            options.service_report = Some(service.to_string());
        }
    }
    options
}

fn print_unknown_service_error(report: &ProductionReadinessReport, service: &str) {
    eprintln!("production readiness gate failed: unknown service {service}");
    eprintln!("known services: {}", report.known_services().join(", "));
}

fn service_failure_lines(report: &ProductionReadinessReport) -> Vec<String> {
    report
        .blocked_services()
        .into_iter()
        .map(|summary| {
            format!(
                "- service {}: {} blocker(s), classes=[{}], next_action={}",
                summary.service,
                summary.blocker_count,
                summary.blocker_classes.join(","),
                summary.next_action
            )
        })
        .collect()
}

fn service_failure_lines_for_service(
    report: &ProductionReadinessReport,
    service: &str,
) -> Vec<String> {
    service_failure_lines(report)
        .into_iter()
        .filter(|line| line.starts_with(&format!("- service {service}:")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_gate_parses_service_options() {
        assert_eq!(
            parse_gate_options(["--service".to_string(), "proxy".to_string()]),
            GateOptions {
                service_filter: Some("proxy".to_string()),
                service_report: None,
                service_reports: false,
                next_service: false,
                list_services: false,
            }
        );
        assert_eq!(
            parse_gate_options(["--service=metaserver".to_string()]),
            GateOptions {
                service_filter: Some("metaserver".to_string()),
                service_report: None,
                service_reports: false,
                next_service: false,
                list_services: false,
            }
        );
        assert_eq!(
            parse_gate_options(["--service-report".to_string(), "data_node".to_string()]),
            GateOptions {
                service_filter: None,
                service_report: Some("data_node".to_string()),
                service_reports: false,
                next_service: false,
                list_services: false,
            }
        );
        assert_eq!(
            parse_gate_options(["--service-reports".to_string()]),
            GateOptions {
                service_filter: None,
                service_report: None,
                service_reports: true,
                next_service: false,
                list_services: false,
            }
        );
        assert_eq!(
            parse_gate_options(["--next-service".to_string()]),
            GateOptions {
                service_filter: None,
                service_report: None,
                service_reports: false,
                next_service: true,
                list_services: false,
            }
        );
        assert_eq!(
            parse_gate_options(["--list-services".to_string()]),
            GateOptions {
                service_filter: None,
                service_report: None,
                service_reports: false,
                next_service: false,
                list_services: true,
            }
        );
        assert_eq!(
            parse_gate_options(["--json".to_string()]),
            GateOptions::default()
        );
    }

    #[test]
    fn readiness_gate_failure_lines_include_service_next_actions() {
        let report = production_readiness_report();
        let lines = service_failure_lines(&report);
        assert!(lines
            .iter()
            .any(|line| line.contains("service client") && line.contains("tonic/prost")));
        assert!(lines
            .iter()
            .any(|line| line.contains("service proxy") && line.contains("topology-version")));
        assert!(lines
            .iter()
            .any(|line| line.contains("service ingestion") && line.contains("Kafka/Flink")));
        assert!(lines
            .iter()
            .any(|line| line.contains("service data_node") && line.contains("Raft")));
        assert!(lines
            .iter()
            .any(|line| line.contains("service metaserver") && line.contains("scheduler")));
    }

    #[test]
    fn readiness_gate_can_filter_one_service() {
        let report = production_readiness_report();
        let proxy_lines = service_failure_lines_for_service(&report, "proxy");
        assert_eq!(proxy_lines.len(), 1);
        assert!(proxy_lines[0].contains("topology-version"));
        assert!(service_failure_lines_for_service(&report, "client")
            .iter()
            .all(|line| !line.contains("service proxy")));
        assert!(service_failure_lines_for_service(&report, "unknown").is_empty());
    }
}
