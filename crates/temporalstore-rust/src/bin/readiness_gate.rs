use temporalstore_rust::{production_readiness_report, ProductionReadinessReport};

fn main() {
    let service_filter = parse_service_filter(std::env::args().skip(1));
    let report = production_readiness_report();
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("readiness report must serialize")
    );

    if let Some(service) = service_filter.as_deref() {
        let Some(gate) = report.service_gate_report(service) else {
            eprintln!("production readiness gate failed: unknown service {service}");
            eprintln!(
                "known services: {}",
                report
                    .service_summaries
                    .iter()
                    .map(|summary| summary.service.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
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

fn parse_service_filter(args: impl IntoIterator<Item = String>) -> Option<String> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--service" {
            return args.next();
        }
        if let Some(service) = arg.strip_prefix("--service=") {
            return Some(service.to_string());
        }
    }
    None
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
    fn readiness_gate_parses_service_filter() {
        assert_eq!(
            parse_service_filter(["--service".to_string(), "proxy".to_string()]),
            Some("proxy".to_string())
        );
        assert_eq!(
            parse_service_filter(["--service=metaserver".to_string()]),
            Some("metaserver".to_string())
        );
        assert_eq!(parse_service_filter(["--json".to_string()]), None);
    }

    #[test]
    fn readiness_gate_failure_lines_include_service_next_actions() {
        let report = production_readiness_report();
        let lines = service_failure_lines(&report);
        assert!(lines
            .iter()
            .any(|line| line.contains("service client") && line.contains("MetaSyncer")));
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
