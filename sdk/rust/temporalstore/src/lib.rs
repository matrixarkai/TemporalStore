use std::fmt;

#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

mod types;
pub use types::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow,
    ControlStateWindowUnit, FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy,
    IpsFeatureStat, IpsInstance, IpsLastQuery, SequenceFeatureRow,
};

mod options;
pub use options::Options;

#[cfg(feature = "direct")]
mod direct_client;
#[cfg(feature = "direct")]
mod direct_control_state;
#[cfg(feature = "direct")]
mod direct_features;
#[cfg(feature = "direct")]
mod direct_ffi;
#[cfg(feature = "direct")]
mod direct_ffi_types;
#[cfg(feature = "direct")]
mod direct_helpers;
#[cfg(feature = "direct")]
mod direct_ips;
#[cfg(feature = "direct")]
mod direct_key_value;
#[cfg(feature = "direct")]
mod direct_sequence_features;
#[cfg(feature = "direct")]
pub use direct_client::Client;
#[cfg(feature = "direct")]
pub(crate) use direct_ffi::*;

#[cfg(feature = "proxy")]
mod proxy_client;
#[cfg(feature = "proxy")]
mod proxy_control_state;
#[cfg(feature = "proxy")]
mod proxy_features;
#[cfg(feature = "proxy")]
mod proxy_helpers;
#[cfg(feature = "proxy")]
mod proxy_ips;
#[cfg(feature = "proxy")]
mod proxy_key_value;
#[cfg(feature = "proxy")]
mod proxy_sequence_features;
#[cfg(feature = "proxy")]
mod proxy_transport;
#[cfg(feature = "proxy")]
pub use proxy_client::{ProxyClient, ProxyOptions};

#[cfg(feature = "direct")]
mod rust_c_abi_exports;
#[cfg(feature = "direct")]
mod rust_c_abi_helpers;

#[cfg(test)]
mod tests {
    #[cfg(feature = "direct")]
    use super::Client;
    #[cfg(feature = "proxy")]
    use super::ProxyClient;

    #[cfg(feature = "direct")]
    #[test]
    fn direct_client_exposes_cpp_c_abi_parity_methods() {
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::put_string;
        let _: fn(&Client, &str, &str, u64) -> super::Result<()> = Client::put_string_with_ttl;
        let _: fn(&Client, &str) -> super::Result<String> = Client::get_string;
        let _: fn(&Client, &str, &str, &str) -> super::Result<()> = Client::hset;
        let _: fn(&Client, &str, &str) -> super::Result<String> = Client::hget;
        let _: fn(&Client, &str) -> super::Result<Vec<(String, String)>> = Client::hgetall;
        let _: fn(&Client, &str) -> super::Result<Vec<(String, String)>> = Client::scan_hash;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::hdel;
        let _: fn(&Client, &str) -> super::Result<()> = Client::delete_object;
        let _: fn(&Client, &str, u64) -> super::Result<()> = Client::expire;
        let _: fn(&Client, &str) -> super::Result<u64> = Client::ttl;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::sadd;
        let _: fn(&Client, &str) -> super::Result<Vec<String>> = Client::smembers;
        let _: fn(&Client, &str, &[super::FeaturePoint]) -> super::Result<()> =
            Client::add_feature_points;
        let _: fn(&Client, &str, u64, u64, u64) -> super::Result<Vec<super::FeaturePoint>> =
            Client::query_feature_points;
        let _: fn(
            &Client,
            &str,
            u64,
            u64,
            u64,
            &[super::FeatureFilter],
        ) -> super::Result<Vec<super::FeaturePoint>> = Client::query_feature_points_filtered;
        let _: fn(&Client, &super::IpsInstance) -> super::Result<()> = Client::add_ips_instance;
        let _: fn(&Client, &super::IpsLastQuery) -> super::Result<Vec<super::IpsFeatureStat>> =
            Client::query_ips_last_instances;
        let _: fn(
            &Client,
            &str,
            i64,
            u64,
            super::ControlStatePrecision,
            &str,
            u64,
        ) -> super::Result<()> = Client::control_state_increment;
        let _: fn(
            &Client,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<i64> = Client::control_state_count;
        let _: fn(&Client, &[(&str, &str, &str)], Option<&str>, Option<&str>) -> super::Result<()> =
            Client::matrixark_batch_append_records;
    }

    #[cfg(feature = "proxy")]
    #[test]
    fn proxy_client_exposes_cpp_proxy_parity_methods() {
        let _: fn(&ProxyClient, &str, &[super::FeaturePoint]) -> super::Result<()> =
            ProxyClient::feature_add;
        let _: fn(
            &ProxyClient,
            &str,
            u64,
            u64,
            Option<usize>,
        ) -> super::Result<Vec<super::FeaturePoint>> = ProxyClient::feature_query;
        let _: fn(
            &ProxyClient,
            &str,
            u64,
            u64,
            Option<usize>,
            &[super::FeatureFilter],
        ) -> super::Result<Vec<super::FeaturePoint>> = ProxyClient::feature_query_filtered;
        let _: fn(&ProxyClient, &super::IpsInstance) -> super::Result<()> =
            ProxyClient::add_ips_instance;
        let _: fn(&ProxyClient, &super::IpsLastQuery) -> super::Result<Vec<super::IpsFeatureStat>> =
            ProxyClient::query_ips_last_instances;
        let _: fn(
            &ProxyClient,
            &str,
            i64,
            u64,
            super::ControlStatePrecision,
            &str,
            u64,
        ) -> super::Result<()> = ProxyClient::control_state_increment;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<i64> = ProxyClient::control_state_count;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> = ProxyClient::get;
        let _: fn(&ProxyClient, &str, u64, i64) -> super::Result<()> =
            ProxyClient::control_state_hset;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            u64,
            super::ControlStateHType,
            u64,
            super::ControlStatePrecision,
        ) -> super::Result<()> = ProxyClient::control_state_hset_with_options;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
            &str,
        ) -> super::Result<Vec<i64>> = ProxyClient::control_state_hquery;
        let _: fn(
            &ProxyClient,
            &str,
            &[&str],
            u64,
            u64,
            super::ControlStatePrecision,
            bool,
        ) -> super::Result<()> = ProxyClient::control_state_cpc_set;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<Vec<i64>> = ProxyClient::control_state_cpc_query;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            u64,
            u64,
            super::ControlStateFolType,
        ) -> super::Result<()> = ProxyClient::control_state_fol_set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> =
            ProxyClient::control_state_fol_query;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<(String, String)>> =
            ProxyClient::control_state_manager;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            &[(&str, &str)],
            &str,
            &str,
            bool,
        ) -> super::Result<Vec<(String, String)>> = ProxyClient::control_state_manager_with_options;
        let _: fn(&ProxyClient, &str, &str, &str) -> super::Result<()> = ProxyClient::hset;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<String> = ProxyClient::hget;
        let _: fn(&ProxyClient, &str, &[(&str, &str)]) -> super::Result<()> = ProxyClient::hmset;
        let _: fn(&ProxyClient, &str, &[&str]) -> super::Result<Vec<Option<String>>> =
            ProxyClient::hmget;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<(String, String)>> =
            ProxyClient::hgetall;
        let _: fn(&ProxyClient, &str) -> super::Result<u64> = ProxyClient::hlen;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::hdel;
        let _: fn(&ProxyClient, &str) -> super::Result<()> = ProxyClient::delete_object;
        let _: fn(&ProxyClient, &str, u64) -> super::Result<()> = ProxyClient::expire;
        let _: fn(&ProxyClient, &str) -> super::Result<u64> = ProxyClient::ttl;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::sadd;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<String>> = ProxyClient::smembers;
    }
}
