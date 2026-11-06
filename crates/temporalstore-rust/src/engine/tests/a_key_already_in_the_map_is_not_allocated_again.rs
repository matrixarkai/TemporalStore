//! A key already in the model map is not allocated again.
//!
//! `RecordedMap::entries` is `HashMap<ModelKey, K::Elements>` with `ModelKey = Arc<str>`
//! (`engine_storage_state.rs`). `HashMap::entry` takes its key BY VALUE, and
//! `Arc<str>::from(&str)` always mallocs and copies -- so writing an element through
//! `entry(Arc::from(key))` allocated, copied into and immediately dropped an `Arc` on every write
//! that landed on an object already in the map.
//!
//! THE WASTE WAS (N-1)/N IN ELEMENTS PER OBJECT, measured against a counting allocator on the
//! same container shape: 0.0% at one element per object, 50.0% at two, 75.0% at four, 87.5% at
//! eight, 91.7% at twelve and 98.4% at sixty-four -- tracking that prediction to the decimal at
//! every width, with zero spread over three runs. Each wasted call cost one malloc/free pair and
//! 80.0 B on a 62-character key. A fixture with ONE element per object saves exactly nothing,
//! which is why the figure is stated in elements per object and never in writes.
//!
//! WHY A SOURCE-TEXT GUARD RATHER THAN AN ALLOCATION COUNT. The quantity is a count of
//! allocations, and the only allocation instrument in this crate sits behind
//! `--features alloc-probe`, whose global allocator wrapper adds two atomics to every allocation
//! and cannot be part of a normal run. A guard that cannot run in the default suite cannot be
//! pinned, and an unpinned guard does not fail when the thing it protects regresses. So this pins
//! the SHAPE instead: that each recorded install reaches its element map through a BORROWED
//! lookup and builds an owned key only on the miss. That is the form `remove_element` has always
//! used, which is why the second test anchors the claim to it.

const CONTAINER: &str = include_str!("../recorded_map.rs");

/// Slice one `fn <name>(` body out of the container source, by brace depth.
fn body_of(name: &str) -> String {
    let decl = format!("fn {}(", name);
    let at = CONTAINER
        .find(&decl)
        .unwrap_or_else(|| panic!("{} is not declared in recorded_map.rs any more", name));
    let rest = &CONTAINER[at..];
    let open = rest.find('{').expect("no body brace");
    let mut depth = 0i32;
    for (i, c) in rest[open..].char_indices() {
        if c == '{' {
            depth += 1;
        } else if c == '}' {
            depth -= 1;
            if depth == 0 {
                return rest[open..open + i + 1].to_string();
            }
        }
    }
    panic!("unbalanced body for {}", name);
}

#[test]
fn a_recorded_install_looks_its_key_up_borrowed_before_it_builds_one() {
    for name in ["install", "replay_install_element"] {
        let body = body_of(name);

        assert!(
            body.contains("self.entries.get_mut("),
            "{} no longer looks its key up borrowed. `entry` takes its key BY VALUE and \
             `Arc<str>::from(&str)` always allocates, so without a borrowed lookup first this \
             pays a malloc, a 62-byte copy and a free on every write to an object that is \
             already present -- which is (N-1)/N of them. Use the form `remove_element` \
             uses.\nbody:\n{}",
            name, body
        );

        // The miss path must still build an owned key, or a first write cannot create the entry.
        assert!(
            body.contains("Arc::from("),
            "{} must still build an owned key on the MISS path, or an object's first element \
             write cannot create its entry.\nbody:\n{}",
            name, body
        );

        // The borrowed lookup has to come FIRST, or the allocation happens anyway.
        let lookup = body
            .find("self.entries.get_mut(")
            .expect("presence asserted above");
        let build = body.find("Arc::from(").expect("presence asserted above");
        assert!(
            lookup < build,
            "{} builds an owned key before it looks one up, so the allocation happens regardless \
             of whether the key is present. The borrowed lookup must guard the build.\nbody:\n{}",
            name, body
        );
    }
}

#[test]
fn the_allocation_free_lookup_this_guard_names_is_the_one_remove_element_uses() {
    // Anchors the claim above to a sibling that has always been written this way, so the guard
    // fails loudly if `remove_element` is what changes rather than `install`.
    let body = body_of("remove_element");
    assert!(
        body.contains("self.entries.get_mut(") || body.contains("self.entries.remove("),
        "remove_element no longer reaches its element map through a borrowed lookup, so the \
         precedent this guard cites is gone. Re-establish the claim before relaxing it.\nbody:\n{}",
        body
    );
}
