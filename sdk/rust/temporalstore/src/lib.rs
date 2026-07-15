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
mod client_api_tests;
