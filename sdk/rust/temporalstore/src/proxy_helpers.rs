use crate::{
    ControlStatePrecision, ControlStateWindow, ControlStateWindowUnit, Error, FeaturePoint, Result,
};

pub(crate) fn json_error(err: serde_json::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

pub(crate) fn json_byte_array_to_string(value: serde_json::Value) -> String {
    let bytes = value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.as_u64().unwrap_or_default() as u8)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub(crate) fn response_hash_entries_to_strings(
    response: serde_json::Value,
) -> Vec<(String, String)> {
    response
        .get("entries")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.as_array()?;
            let field = entry.first()?.as_str()?.to_string();
            let value = json_byte_array_to_string(entry.get(1)?.clone());
            Some((field, value))
        })
        .collect()
}

pub(crate) fn response_feature_points(response: serde_json::Value) -> Result<Vec<FeaturePoint>> {
    serde_json::from_value(
        response
            .get("points")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(json_error)
}

pub(crate) fn proxy_timestamp_ms(occur_time_seconds: u64) -> u64 {
    if occur_time_seconds > 0 {
        return occur_time_seconds.saturating_mul(1000);
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(crate) fn control_state_precision_ms(precision: ControlStatePrecision) -> u64 {
    match precision {
        ControlStatePrecision::OneSecond => 1000,
        ControlStatePrecision::FiveSeconds => 5000,
        ControlStatePrecision::TenSeconds => 10000,
        ControlStatePrecision::OneMinute => 60_000,
        ControlStatePrecision::FiveMinutes => 5 * 60_000,
        ControlStatePrecision::TenMinutes => 10 * 60_000,
        ControlStatePrecision::OneHour => 60 * 60_000,
        ControlStatePrecision::OneDay => 24 * 60 * 60_000,
        ControlStatePrecision::OneMonth => 30 * 24 * 60 * 60_000,
    }
}

pub(crate) fn control_state_window_ms(window: ControlStateWindow) -> (u64, u64) {
    let end = if window.end > 0 {
        window.end as u64
    } else {
        proxy_timestamp_ms(0)
    };
    let start = if window.start >= 0 {
        window.start as u64
    } else {
        end.saturating_sub(control_state_window_unit_ms(window.unit))
    };
    (start, end)
}

pub(crate) fn control_state_window_unit_ms(unit: ControlStateWindowUnit) -> u64 {
    match unit {
        ControlStateWindowUnit::Second => 1000,
        ControlStateWindowUnit::Minute => 60_000,
        ControlStateWindowUnit::Hour => 60 * 60_000,
        ControlStateWindowUnit::Day => 24 * 60 * 60_000,
    }
}

pub(crate) fn io_error(err: std::io::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

pub(crate) fn parse_http_endpoint(endpoint: &str) -> Result<(String, u16, String)> {
    let without_scheme = endpoint.strip_prefix("http://").ok_or_else(|| Error {
        code: 0,
        message: "Rust proxy SDK currently expects an http:// endpoint".to_string(),
    })?;
    let (authority, path) = without_scheme
        .split_once('/')
        .unwrap_or((without_scheme, ""));
    let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
        let port = port.parse::<u16>().map_err(|_| Error {
            code: 0,
            message: "invalid proxy endpoint port".to_string(),
        })?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), 80)
    };
    let base_path = if path.is_empty() {
        String::new()
    } else {
        format!("/{path}")
    };
    Ok((host, port, base_path))
}
