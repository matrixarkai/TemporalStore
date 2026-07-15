#[derive(Clone, Debug)]
pub struct Options {
    pub metaserver_addr: String,
    pub namespace_name: String,
    pub table_name: String,
    pub metaserver_consul: String,
    pub idc: String,
    pub host: String,
    pub psm: String,
    pub log_dir: String,
    pub log_level: i32,
    pub io_timeout_ms: i32,
    pub connect_timeout_ms: i32,
    pub request_timeout_ms: i32,
    pub max_read_retries: i32,
    pub max_write_retries: i32,
    pub retry_backoff_ms: i32,
    pub max_feature_points_per_request: i32,
    pub max_feature_query_count: u64,
    pub max_key_bytes: u64,
    pub max_value_bytes: u64,
    pub pin_primary: bool,
}

impl Options {
    pub fn new(
        metaserver_addr: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            metaserver_addr: metaserver_addr.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            metaserver_consul: String::new(),
            idc: "vdc1".to_string(),
            host: "127.0.0.1".to_string(),
            psm: "temporalstore.rust.client".to_string(),
            log_dir: "./".to_string(),
            log_level: 3,
            io_timeout_ms: 1000,
            connect_timeout_ms: 1000,
            request_timeout_ms: 5000,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
            max_feature_points_per_request: 1000,
            max_feature_query_count: 5000,
            max_key_bytes: 4096,
            max_value_bytes: 16 * 1024 * 1024,
            pin_primary: true,
        }
    }
}
