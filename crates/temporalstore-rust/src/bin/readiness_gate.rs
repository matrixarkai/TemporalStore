use temporalstore_rust::production_readiness_report;

fn main() {
    let report = production_readiness_report();
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("readiness report must serialize")
    );

    if !report.production_ready {
        eprintln!(
            "production readiness gate failed: {} blocker(s) remain",
            report.missing_count()
        );
        std::process::exit(2);
    }
}
