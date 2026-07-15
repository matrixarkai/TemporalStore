use crate::types::ControlStateFamily;

use super::product_model::control_state_family_key;

pub(super) fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("control_state:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        keys.push(control_state_family_key(family, key));
    }
    keys
}

pub(super) fn visit_associated_record_keys(key: &str, mut visit: impl FnMut(&str)) {
    visit(key);
    if key.starts_with("control_state:") {
        return;
    }
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        visit(&family_key);
    }
}

pub(super) fn any_associated_record_key(
    key: &str,
    mut predicate: impl FnMut(&str) -> bool,
) -> bool {
    if predicate(key) {
        return true;
    }
    if key.starts_with("control_state:") {
        return false;
    }
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        if predicate(&family_key) {
            return true;
        }
    }
    false
}
