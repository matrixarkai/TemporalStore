use crate::matrixark_rust_proxy_protocol::{Command, HashEntryRef};

pub(crate) fn command_entries(command: &Command) -> Result<Vec<HashEntryRef<'_>>, String> {
    if let Some(entries) = &command.entries_compact {
        return Ok(entries
            .iter()
            .map(|entry| HashEntryRef {
                key: entry[0].as_str(),
                field: entry[1].as_str(),
                value: entry[2].as_str(),
            })
            .collect());
    }
    if let Some(entries) = &command.entries {
        return entries
            .iter()
            .map(|entry| {
                Ok(HashEntryRef {
                    key: entry.key.as_str(),
                    field: entry.field.as_str(),
                    value: entry
                        .value
                        .as_deref()
                        .ok_or_else(|| "matrixark batch append entry missing value".to_string())?,
                })
            })
            .collect();
    }
    Ok(Vec::new())
}
