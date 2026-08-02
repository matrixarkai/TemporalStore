use crate::matrixark_rust_proxy_protocol::{Command, HashEntryRef};
use std::borrow::Cow;

pub(crate) fn command_entries(command: &Command) -> Result<Vec<HashEntryRef<'_>>, String> {
    if let Some(entries) = &command.entries_compact {
        return Ok(entries
            .iter()
            .map(|entry| HashEntryRef {
                key: entry[0].as_str(),
                field: entry[1].as_str(),
                value: entry[2].as_str(),
                route_json: Cow::Borrowed("{}"),
            })
            .collect());
    }
    if let Some(entries) = &command.entries {
        return entries
            .iter()
            .map(|entry| {
                let route_json = entry
                    .route_json
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(Cow::Borrowed)
                    .or_else(|| {
                        entry
                            .storage_route
                            .as_ref()
                            .map(|value| Cow::Owned(value.to_string()))
                    })
                    .unwrap_or_else(|| Cow::Borrowed("{}"));
                Ok(HashEntryRef {
                    key: entry.key.as_str(),
                    field: entry.field.as_str(),
                    value: entry
                        .value
                        .as_deref()
                        .ok_or_else(|| "matrixark batch append entry missing value".to_string())?,
                    route_json,
                })
            })
            .collect();
    }
    Ok(Vec::new())
}
