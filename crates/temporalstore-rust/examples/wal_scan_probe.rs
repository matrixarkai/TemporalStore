// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What a recovered record actually carries: a command to re-run, an outcome naming a page, or
//! the page bytes themselves. With the pages wiped, only the last two of those can rebuild a value.
//!
//!     cargo run --example wal_scan_probe -- <store-root>

use std::path::PathBuf;

use temporalstore_rust::wal::LocalWriteAheadLogStore;

fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("usage: wal_scan_probe <store-root>"));
    let wals = root.join("indexes").join("wals");
    let store = LocalWriteAheadLogStore::new(&wals);

    let records = match store.scan(1, 0, u64::MAX, u64::MAX) {
        Ok(records) => records,
        Err(err) => {
            println!("scan ERROR: {err}");
            return;
        }
    };
    println!("scan returned {} record(s)", records.len());

    let mut with_command = 0usize;
    let mut with_outcomes = 0usize;
    let mut outcome_items = 0usize;
    let mut outcomes_carrying_a_value = 0usize;
    let mut with_staged_pages = 0usize;
    let mut staged_page_bytes = 0usize;
    let mut undecodable = 0usize;

    for (_, bytes) in &records {
        match temporalstore_rust::wal::decode_wal_line(bytes) {
            Ok(record) => {
                if record.command.is_some() {
                    with_command += 1;
                }
                if !record.outcomes.is_empty() {
                    with_outcomes += 1;
                    outcome_items += record.outcomes.len();
                    for item in &record.outcomes {
                        if item.value.as_ref().map(|v| !v.is_empty()).unwrap_or(false) {
                            outcomes_carrying_a_value += 1;
                        }
                    }
                }
                if !record.staged_pages.is_empty() {
                    with_staged_pages += 1;
                    staged_page_bytes += record.staged_pages.iter().map(|p| p.bytes.len()).sum::<usize>();
                }
            }
            Err(_) => undecodable += 1,
        }
    }

    println!("  carrying a COMMAND to re-run          {with_command}");
    println!("  carrying OUTCOMES                     {with_outcomes}  ({outcome_items} items)");
    println!("     of those items, carrying a VALUE   {outcomes_carrying_a_value}");
    println!("  carrying STAGED PAGES                 {with_staged_pages}  ({staged_page_bytes} bytes)");
    println!("  undecodable                           {undecodable}");
    println!();
    println!("With the pages wiped, a value can only come back from a command to re-run, an");
    println!("outcome that carries the value, or a staged page. A record with none of those");
    println!("names an address that no longer exists.");
}
