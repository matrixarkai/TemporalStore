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

    let drop_percent = options.drop_percent.min(100);
    if options.serving_mode == ProxyServingMode::Serving && drop_percent == 0 {
        return None;
    }

    if matches!(
        options.serving_mode,
        ProxyServingMode::Readonly | ProxyServingMode::WriteDisabled
    ) && commands.iter().any(proxy_command_is_write)
    {
        return Some(Status::error(
            "proxy_write_disabled",
            "proxy is not accepting writes",
        ));
    }
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

#[cfg(test)]
mod tests {
    use super::{proxy_policy_rejection, ProxyOptions, ProxyServingMode};
    use crate::types::{Command, Status};

    #[test]
    fn policy_no_drop_serving_fast_path_skips_rejection() {
        let options = ProxyOptions {
            serving_mode: ProxyServingMode::Serving,
            drop_percent: 0,
            ..ProxyOptions::default()
        };
        let mut commands = vec![
            Command::StringGet {
                key: "k".to_string(),
            },
            Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
            Command::StringDelete {
                key: "k3".to_string(),
            },
        ];
        assert_eq!(proxy_policy_rejection(&options, &commands), None);
        commands.push(Command::RiskFamilyQuery {
            family: crate::types::RiskFamily::H,
            key: "risk".to_string(),
            start_ms: 1,
            end_ms: 2,
            aggregator: "avg".to_string(),
        });
        assert_eq!(proxy_policy_rejection(&options, &commands), None);
        assert!(matches!(
            proxy_policy_rejection(
                &ProxyOptions {
                    serving_mode: ProxyServingMode::Readonly,
                    ..ProxyOptions::default()
                },
                &[Command::StringGet {
                    key: "read-only".to_string()
                }]
            ),
            None
        ));
        assert_eq!(
            proxy_policy_rejection(
                &ProxyOptions {
                    serving_mode: ProxyServingMode::Readonly,
                    ..ProxyOptions::default()
                },
                &[Command::StringSet {
                    key: "write-blocked".to_string(),
                    value: b"1".to_vec()
                }]
            ),
            Some(Status::error("proxy_write_disabled", "proxy is not accepting writes"))
        );
        assert_eq!(
            proxy_policy_rejection(
                &ProxyOptions {
                    serving_mode: ProxyServingMode::NotServing,
                    ..ProxyOptions::default()
                },
                &[Command::StringGet {
                    key: "anything".to_string()
                }]
            ),
            Some(Status::error("proxy_not_serving", "proxy is not serving"))
        );
    }
}
