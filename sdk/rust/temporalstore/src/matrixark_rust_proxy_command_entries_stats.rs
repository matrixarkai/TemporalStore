use crate::matrixark_rust_proxy_protocol::Command;

pub(crate) fn command_entry_count(command: &Command) -> u64 {
    command
        .entries_compact
        .as_ref()
        .map(|entries| entries.len() as u64)
        .or_else(|| command.entries.as_ref().map(|entries| entries.len() as u64))
        .unwrap_or(0)
}

pub(crate) fn command_entry_stats(command: &Command) -> (u64, u64) {
    if let Some(entries) = &command.entries_compact {
        let bytes = entries.iter().map(|entry| entry[2].len() as u64).sum();
        return (entries.len() as u64, bytes);
    }
    if let Some(entries) = &command.entries {
        let bytes = entries
            .iter()
            .map(|entry| {
                entry
                    .value
                    .as_ref()
                    .map(|value| value.len() as u64)
                    .unwrap_or(0)
            })
            .sum();
        return (entries.len() as u64, bytes);
    }
    (0, 0)
}

pub(crate) fn hash_entry_stats(command: &Command) -> (u64, u64) {
    let mut records = 0_u64;
    let mut bytes = 0_u64;
    if let Some(entries) = &command.entries {
        for entry in entries {
            if let Some(value) = entry.value.as_ref() {
                records += 1;
                bytes += value.len() as u64;
            }
        }
    }
    if let Some(entries) = &command.entries_compact {
        for entry in entries {
            records += 1;
            bytes += entry[2].len() as u64;
        }
    }
    (records, bytes)
}
