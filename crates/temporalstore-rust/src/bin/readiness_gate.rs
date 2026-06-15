use temporalstore_rust::{production_readiness_report, ProductionReadinessReport};

fn main() {
    let report = production_readiness_report();
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("readiness report must serialize")
    );

    if !report.production_ready {
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

fn service_failure_lines(report: &ProductionReadinessReport) -> Vec<String> {
    report
        .service_summaries
        .iter()
        .filter(|summary| !summary.ready || summary.blocker_count > 0)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
