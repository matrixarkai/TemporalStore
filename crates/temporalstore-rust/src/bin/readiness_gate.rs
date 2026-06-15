use temporalstore_rust::production_readiness_report;

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
        for blocker in report.exact_failed_capabilities().iter().take(10) {
            eprintln!("- {}: {}", blocker.area, blocker.capability);
        }
        std::process::exit(2);
    }
}
