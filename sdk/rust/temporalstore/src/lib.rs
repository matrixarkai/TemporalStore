use std::fmt;

#[cfg(feature = "direct")]
use std::ffi::{CStr, CString};
#[cfg(feature = "proxy")]
use std::io::{Read, Write};
#[cfg(feature = "proxy")]
use std::net::TcpStream;
#[cfg(feature = "direct")]
use std::os::raw::{c_char, c_int};
#[cfg(feature = "direct")]
use std::ptr;

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

#[derive(Clone, Copy, Debug)]
pub enum FeatureFilterOp {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    LessThan = 3,
}

#[derive(Clone, Debug)]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFeatureRow {
    pub timestamp: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct TemporalStoreClientOpaque {
    _private: [u8; 0],
}

#[cfg(feature = "direct")]
#[repr(C)]
struct COptions {
    metaserver_addr: *const c_char,
    metaserver_consul: *const c_char,
    namespace_name: *const c_char,
    table_name: *const c_char,
    idc: *const c_char,
    host: *const c_char,
    psm: *const c_char,
    log_dir: *const c_char,
    log_level: c_int,
    io_timeout_ms: c_int,
    connect_timeout_ms: c_int,
    request_timeout_ms: c_int,
    max_read_retries: c_int,
    max_write_retries: c_int,
    retry_backoff_ms: c_int,
    max_feature_points_per_request: c_int,
    max_feature_query_count: u64,
    max_key_bytes: u64,
    max_value_bytes: u64,
    pin_primary: c_int,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CFeatureFilter {
    field: *const c_char,
    op: c_int,
    value: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CSequenceFeatureRow {
    timestamp: u64,
    gid: u64,
    action_type: u32,
    duration: u32,
    author_id: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CHashEntry {
    key: *const c_char,
    field: *const c_char,
    value: *const c_char,
    route_json: *const c_char,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CStringArray {
    count: usize,
    values: *mut *mut c_char,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CSequenceFeatureRowArray {
    count: usize,
    rows: *mut CSequenceFeatureRow,
}

#[cfg(feature = "direct")]
extern "C" {
    fn temporalstore_options_init(options: *mut COptions);
    fn temporalstore_free_string(value: *mut c_char);
    fn temporalstore_connect(
        options: *const COptions,
        client: *mut *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_close(
        client: *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_put_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_get_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_delete_object(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_expire(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: *mut u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hset(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hget(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hdel(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_sadd(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        member: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_smembers(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        members: *mut CStringArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_string_array_free(array: *mut CStringArray);
    fn temporalstore_matrixark_batch_append_records(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_add_sequence_feature_rows(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        rows: *const CSequenceFeatureRow,
        count: usize,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_query_sequence_feature_rows(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        rows: *mut CSequenceFeatureRowArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_sequence_feature_row_array_free(rows: *mut CSequenceFeatureRowArray);
}

#[cfg(feature = "direct")]
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

#[cfg(feature = "direct")]
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

#[cfg(feature = "direct")]
pub struct Client {
    raw: *mut TemporalStoreClientOpaque,
}

#[cfg(feature = "direct")]
unsafe impl Send for Client {}

#[cfg(feature = "direct")]
impl Client {
    pub fn connect(options: Options) -> Result<Self> {
        let strings = CStringOptions::new(&options)?;
        let mut c_options = unsafe {
            let mut value = std::mem::MaybeUninit::<COptions>::zeroed().assume_init();
            temporalstore_options_init(&mut value);
            value
        };
        strings.apply(&mut c_options, &options);

        let mut raw: *mut TemporalStoreClientOpaque = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_connect(&c_options, &mut raw, &mut error) };
        check(code, error)?;
        Ok(Self { raw })
    }

    pub fn close(&mut self) -> Result<()> {
        if self.raw.is_null() {
            return Ok(());
        }
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_close(self.raw, &mut error) };
        self.raw = ptr::null_mut();
        check(code, error)
    }

    pub fn put_string(&self, key: &str, value: &str) -> Result<()> {
        let key = cstring(key)?;
        let value = cstring(value)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_put_string(self.raw, key.as_ptr(), value.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        let key = cstring(key)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_get_string(self.raw, key.as_ptr(), &mut value, &mut error) };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn delete_object(&self, key: &str) -> Result<()> {
        let key = cstring(key)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_delete_object(self.raw, key.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn expire(&self, key: &str, ttl_ms: u64) -> Result<()> {
        let key = cstring(key)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_expire(self.raw, key.as_ptr(), ttl_ms, &mut error) };
        check(code, error)
    }

    pub fn ttl(&self, key: &str) -> Result<u64> {
        let key = cstring(key)?;
        let mut ttl_ms = 0_u64;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_ttl(self.raw, key.as_ptr(), &mut ttl_ms, &mut error) };
        check(code, error)?;
        Ok(ttl_ms)
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let value = cstring(value)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_hset(
                self.raw,
                key.as_ptr(),
                field.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_hget(
                self.raw,
                key.as_ptr(),
                field.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<()> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_hdel(self.raw, key.as_ptr(), field.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let key = cstring(key)?;
        let member = cstring(member)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_sadd(self.raw, key.as_ptr(), member.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let key = cstring(key)?;
        let mut out = CStringArray {
            count: 0,
            values: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_smembers(self.raw, key.as_ptr(), &mut out, &mut error) };
        check(code, error)?;
        let values = if out.values.is_null() {
            Vec::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(out.values, out.count) };
            slice
                .iter()
                .filter_map(|value| {
                    if value.is_null() {
                        None
                    } else {
                        Some(unsafe { CStr::from_ptr(*value).to_string_lossy().into_owned() })
                    }
                })
                .collect()
        };
        unsafe { temporalstore_string_array_free(&mut out) };
        Ok(values)
    }

    pub fn matrixark_batch_append_records(
        &self,
        entries: &[(&str, &str, &str)],
        count_key: Option<&str>,
        count_value: Option<&str>,
    ) -> Result<()> {
        if entries.is_empty() && count_key.unwrap_or("").is_empty() {
            return Ok(());
        }
        let mut strings: Vec<CString> = Vec::with_capacity(entries.len() * 4 + 2);
        let mut c_entries: Vec<CHashEntry> = Vec::with_capacity(entries.len());
        for (key, field, value) in entries {
            let key_index = strings.len();
            strings.push(cstring(key)?);
            let field_index = strings.len();
            strings.push(cstring(field)?);
            let value_index = strings.len();
            strings.push(cstring(value)?);
            let route_index = strings.len();
            strings.push(cstring("{}")?);
            c_entries.push(CHashEntry {
                key: strings[key_index].as_ptr(),
                field: strings[field_index].as_ptr(),
                value: strings[value_index].as_ptr(),
                route_json: strings[route_index].as_ptr(),
            });
        }
        let count_key_c = match count_key {
            Some(value) if !value.is_empty() => Some(cstring(value)?),
            _ => None,
        };
        let count_value_c = match count_value {
            Some(value) => Some(cstring(value)?),
            None => None,
        };
        let mut error: *mut c_char = ptr::null_mut();
        let entries_ptr = if c_entries.is_empty() {
            ptr::null()
        } else {
            c_entries.as_ptr()
        };
        let code = unsafe {
            temporalstore_matrixark_batch_append_records(
                self.raw,
                entries_ptr,
                c_entries.len(),
                count_key_c
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                count_value_c
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        let key = cstring(key)?;
        let c_rows: Vec<CSequenceFeatureRow> = rows
            .iter()
            .map(|row| CSequenceFeatureRow {
                timestamp: row.timestamp,
                gid: row.gid,
                action_type: row.action_type,
                duration: row.duration,
                author_id: row.author_id,
            })
            .collect();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_add_sequence_feature_rows(
                self.raw,
                key.as_ptr(),
                c_rows.as_ptr(),
                c_rows.len(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let key = cstring(key)?;
        let c_fields: Vec<CString> = filters
            .iter()
            .map(|filter| cstring(&filter.field))
            .collect::<Result<Vec<_>>>()?;
        let c_filters: Vec<CFeatureFilter> = filters
            .iter()
            .zip(c_fields.iter())
            .map(|(filter, field)| CFeatureFilter {
                field: field.as_ptr(),
                op: filter.op as c_int,
                value: filter.value,
            })
            .collect();
        let mut out = CSequenceFeatureRowArray {
            count: 0,
            rows: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_sequence_feature_rows(
                self.raw,
                key.as_ptr(),
                start_ts,
                end_ts,
                count,
                c_filters.as_ptr(),
                c_filters.len(),
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let slice = if out.rows.is_null() {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(out.rows, out.count) }
        };
        let rows = slice
            .iter()
            .map(|row| SequenceFeatureRow {
                timestamp: row.timestamp,
                gid: row.gid,
                action_type: row.action_type,
                duration: row.duration,
                author_id: row.author_id,
            })
            .collect();
        unsafe { temporalstore_sequence_feature_row_array_free(&mut out) };
        Ok(rows)
    }
}

#[cfg(feature = "direct")]
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(feature = "direct")]
struct CStringOptions {
    metaserver_addr: CString,
    metaserver_consul: CString,
    namespace_name: CString,
    table_name: CString,
    idc: CString,
    host: CString,
    psm: CString,
    log_dir: CString,
}

#[cfg(feature = "direct")]
impl CStringOptions {
    fn new(options: &Options) -> Result<Self> {
        Ok(Self {
            metaserver_addr: cstring(&options.metaserver_addr)?,
            metaserver_consul: cstring(&options.metaserver_consul)?,
            namespace_name: cstring(&options.namespace_name)?,
            table_name: cstring(&options.table_name)?,
            idc: cstring(&options.idc)?,
            host: cstring(&options.host)?,
            psm: cstring(&options.psm)?,
            log_dir: cstring(&options.log_dir)?,
        })
    }

    fn apply(&self, c_options: &mut COptions, options: &Options) {
        c_options.metaserver_addr = self.metaserver_addr.as_ptr();
        c_options.metaserver_consul = self.metaserver_consul.as_ptr();
        c_options.namespace_name = self.namespace_name.as_ptr();
        c_options.table_name = self.table_name.as_ptr();
        c_options.idc = self.idc.as_ptr();
        c_options.host = self.host.as_ptr();
        c_options.psm = self.psm.as_ptr();
        c_options.log_dir = self.log_dir.as_ptr();
        c_options.log_level = options.log_level;
        c_options.io_timeout_ms = options.io_timeout_ms;
        c_options.connect_timeout_ms = options.connect_timeout_ms;
        c_options.request_timeout_ms = options.request_timeout_ms;
        c_options.max_read_retries = options.max_read_retries;
        c_options.max_write_retries = options.max_write_retries;
        c_options.retry_backoff_ms = options.retry_backoff_ms;
        c_options.max_feature_points_per_request = options.max_feature_points_per_request;
        c_options.max_feature_query_count = options.max_feature_query_count;
        c_options.max_key_bytes = options.max_key_bytes;
        c_options.max_value_bytes = options.max_value_bytes;
        c_options.pin_primary = if options.pin_primary { 1 } else { 0 };
    }
}

#[cfg(feature = "direct")]
fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error {
        code: 3,
        message: "string contains interior null byte".to_string(),
    })
}

#[cfg(feature = "direct")]
fn check(code: c_int, error: *mut c_char) -> Result<()> {
    if code == 0 {
        return Ok(());
    }
    let message = if error.is_null() {
        "unknown TemporalStore error".to_string()
    } else {
        let message = unsafe { CStr::from_ptr(error).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(error) };
        message
    };
    Err(Error { code, message })
}

#[cfg(feature = "proxy")]
#[derive(Clone, Debug)]
pub struct ProxyOptions {
    pub endpoint: String,
    pub namespace_name: String,
    pub table_name: String,
    pub api_key: String,
}

#[cfg(feature = "proxy")]
impl ProxyOptions {
    pub fn new(
        endpoint: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            api_key: String::new(),
        }
    }
}

#[cfg(feature = "proxy")]
pub struct ProxyClient {
    endpoint: String,
    options: ProxyOptions,
}

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn connect(options: ProxyOptions) -> Self {
        let endpoint = options.endpoint.trim_end_matches('/').to_string();
        Self { endpoint, options }
    }

    pub fn put_string(&self, key: &str, value: &str) -> Result<()> {
        let body = self.key_body(key, &[("value", serde_json::json!(value))]);
        self.post("/v1/string/put", body).map(|_| ())
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        let body = self.key_body(key, &[]);
        let data = self.post("/v1/string/get", body)?;
        Ok(data
            .get("value")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string())
    }

    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        let body = self.key_body(
            key,
            &[("rows", serde_json::to_value(rows).map_err(json_error)?)],
        );
        self.post("/v1/sequence/add", body).map(|_| ())
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let encoded_filters: Vec<serde_json::Value> = filters
            .iter()
            .map(|filter| {
                serde_json::json!({
                    "field": filter.field,
                    "op": filter.op as i32,
                    "value": filter.value,
                })
            })
            .collect();
        let body = self.key_body(
            key,
            &[
                ("start_ts", serde_json::json!(start_ts)),
                ("end_ts", serde_json::json!(end_ts)),
                ("count", serde_json::json!(count)),
                ("filters", serde_json::json!(encoded_filters)),
            ],
        );
        let data = self.post("/v1/sequence/query", body)?;
        serde_json::from_value(
            data.get("rows")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(json_error)
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("field", serde_json::json!(field)),
                ("value", serde_json::json!(value.as_bytes())),
            ],
        );
        self.proxy_service_execute("/ProxyService/HSet", body)
            .map(|_| ())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        let response = self.proxy_service_execute("/ProxyService/HGet", body)?;
        let value = response
            .get("value")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let bytes = value
            .into_iter()
            .map(|item| item.as_u64().unwrap_or_default() as u8)
            .collect::<Vec<_>>();
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        self.proxy_service_execute("/ProxyService/HDel", body)
            .map(|_| ())
    }

    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let body =
            self.proxy_service_body(key, &[("member", serde_json::json!(member.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/SAdd", body)
            .map(|_| ())
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/SMembers", body)?;
        let members = response
            .get("members")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|member| {
                let bytes = member
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|item| item.as_u64().unwrap_or_default() as u8)
                    .collect::<Vec<_>>();
                String::from_utf8_lossy(&bytes).into_owned()
            })
            .collect();
        Ok(members)
    }

    pub fn delete_object(&self, key: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[]);
        self.proxy_service_execute("/ProxyService/Delete", body)
            .map(|_| ())
    }

    pub fn expire(&self, key: &str, ttl_ms: u64) -> Result<()> {
        let body = self.proxy_service_body(key, &[("ttl_ms", serde_json::json!(ttl_ms))]);
        self.proxy_service_execute("/ProxyService/Expire", body)
            .map(|_| ())
    }

    pub fn ttl(&self, key: &str) -> Result<u64> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Ttl", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_u64())
            .unwrap_or_default())
    }

    fn key_body(&self, key: &str, extra: &[(&str, serde_json::Value)]) -> serde_json::Value {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "key": key,
        });
        let object = body.as_object_mut().expect("object body");
        for (name, value) in extra {
            object.insert((*name).to_string(), value.clone());
        }
        body
    }

    fn proxy_service_body(
        &self,
        key: &str,
        extra: &[(&str, serde_json::Value)],
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "key": key,
        });
        let object = body.as_object_mut().expect("object body");
        for (name, value) in extra {
            object.insert((*name).to_string(), value.clone());
        }
        body
    }

    fn proxy_service_execute(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.post_raw(path, body)?;
        let status_ok = response
            .get("status")
            .and_then(|status| status.get("ok"))
            .and_then(|ok| ok.as_bool())
            .unwrap_or(false);
        if !status_ok {
            return Err(Error {
                code: 0,
                message: response
                    .get("status")
                    .and_then(|status| status.get("message"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("proxy service request failed")
                    .to_string(),
            });
        }
        Ok(response
            .get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    fn post(&self, path: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        let envelope = self.post_raw(path, body)?;
        if !envelope
            .get("ok")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
        {
            return Err(Error {
                code: envelope
                    .get("code")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0) as i32,
                message: envelope
                    .get("message")
                    .and_then(|value| value.as_str())
                    .unwrap_or("proxy request failed")
                    .to_string(),
            });
        }
        Ok(envelope
            .get("data")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    fn post_raw(&self, path: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        let (host, port, base_path) = parse_http_endpoint(&self.endpoint)?;
        let request_path = format!("{base_path}{path}");
        let payload = serde_json::to_string(&body).map_err(json_error)?;
        let mut headers = format!(
            "POST {request_path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            payload.len()
        );
        if !self.options.api_key.is_empty() {
            headers.push_str(&format!(
                "Authorization: Bearer {}\r\n",
                self.options.api_key
            ));
        }

        headers.push_str("\r\n");
        let mut stream = TcpStream::connect(format!("{host}:{port}")).map_err(io_error)?;
        stream.write_all(headers.as_bytes()).map_err(io_error)?;
        stream.write_all(payload.as_bytes()).map_err(io_error)?;
        stream.flush().map_err(io_error)?;

        let mut response = String::new();
        stream.read_to_string(&mut response).map_err(io_error)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| Error {
            code: 0,
            message: "invalid proxy HTTP response".to_string(),
        })?;
        if !head.starts_with("HTTP/1.1 2") && !head.starts_with("HTTP/1.0 2") {
            return Err(Error {
                code: 0,
                message: head
                    .lines()
                    .next()
                    .unwrap_or("proxy HTTP request failed")
                    .to_string(),
            });
        }
        serde_json::from_str(body).map_err(json_error)
    }
}

#[cfg(feature = "proxy")]
fn json_error(err: serde_json::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

#[cfg(feature = "proxy")]
fn io_error(err: std::io::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

#[cfg(feature = "proxy")]
fn parse_http_endpoint(endpoint: &str) -> Result<(String, u16, String)> {
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "direct")]
    use super::Client;
    #[cfg(feature = "proxy")]
    use super::ProxyClient;

    #[cfg(feature = "direct")]
    #[test]
    fn direct_client_exposes_cpp_c_abi_parity_methods() {
        let _: fn(&Client, &str, &str, &str) -> super::Result<()> = Client::hset;
        let _: fn(&Client, &str, &str) -> super::Result<String> = Client::hget;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::hdel;
        let _: fn(&Client, &str) -> super::Result<()> = Client::delete_object;
        let _: fn(&Client, &str, u64) -> super::Result<()> = Client::expire;
        let _: fn(&Client, &str) -> super::Result<u64> = Client::ttl;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::sadd;
        let _: fn(&Client, &str) -> super::Result<Vec<String>> = Client::smembers;
        let _: fn(&Client, &[(&str, &str, &str)], Option<&str>, Option<&str>) -> super::Result<()> =
            Client::matrixark_batch_append_records;
    }

    #[cfg(feature = "proxy")]
    #[test]
    fn proxy_client_exposes_cpp_proxy_parity_methods() {
        let _: fn(&ProxyClient, &str, &str, &str) -> super::Result<()> = ProxyClient::hset;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<String> = ProxyClient::hget;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::hdel;
        let _: fn(&ProxyClient, &str) -> super::Result<()> = ProxyClient::delete_object;
        let _: fn(&ProxyClient, &str, u64) -> super::Result<()> = ProxyClient::expire;
        let _: fn(&ProxyClient, &str) -> super::Result<u64> = ProxyClient::ttl;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::sadd;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<String>> = ProxyClient::smembers;
    }
}
