use crate::client::key_is_dropped_by_percent;
use crate::types::{Command, Status};

use super::commands::{proxy_command_is_write, proxy_command_routing_key};
use super::{ProxyOptions, ProxyServingMode};

pub(super) fn proxy_policy_rejection(
    options: &ProxyOptions,
    commands: &[Command],
) -> Option<Status> {
    if matches!(options.serving_mode, ProxyServingMode::NotServing) {
        return Some(Status::error("proxy_not_serving", "proxy is not serving"));
    }
    let has_write = commands.iter().any(proxy_command_is_write);
    if has_write
        && matches!(
            options.serving_mode,
            ProxyServingMode::Readonly | ProxyServingMode::WriteDisabled
        )
    {
        return Some(Status::error(
            "proxy_write_disabled",
            "proxy is not accepting writes",
        ));
    }
    let drop_percent = options.drop_percent.min(100);
    if drop_percent > 0
        && commands
            .iter()
            .filter_map(proxy_command_routing_key)
            .any(|key| key_is_dropped_by_percent(&key, drop_percent))
    {
        return Some(Status::error(
            "proxy_traffic_dropped",
            "request dropped by proxy drop_percent",
        ));
    }
    None
}

pub(super) fn proxy_serving_mode_from_meta(value: &str) -> Option<ProxyServingMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "" => None,
        "serving" => Some(ProxyServingMode::Serving),
        "readonly" | "read_only" => Some(ProxyServingMode::Readonly),
        "write_disabled" => Some(ProxyServingMode::WriteDisabled),
        "degraded" => Some(ProxyServingMode::Degraded),
        "not_serving" | "disabled" | "frozen" | "dropped" => Some(ProxyServingMode::NotServing),
        _ => None,
    }
}

pub(super) fn proxy_serving_mode_label(mode: ProxyServingMode) -> &'static str {
    match mode {
        ProxyServingMode::Serving => "serving",
        ProxyServingMode::Readonly => "readonly",
        ProxyServingMode::WriteDisabled => "write_disabled",
        ProxyServingMode::Degraded => "degraded",
        ProxyServingMode::NotServing => "not_serving",
    }
}
