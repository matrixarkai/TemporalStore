include "base.thrift"

namespace cpp bcache2.thrift

struct Status {
    // The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code].
    1: optional i32 code;

    // A developer-facing error message, which should be in English.
    2: optional string message;
}

struct GetRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    255: optional base.Base Base;
}

struct GetResponse {
    1: optional Status status;
    2: optional binary value;
    255: optional base.BaseResp BaseResp;
}

struct SetRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    4: required binary value;
    5: optional i64 ttl_ms;
    255: optional base.Base Base;
}

struct SetResponse {
    1: optional Status status;
    255: optional base.BaseResp BaseResp;
}

enum WritePolicy {
    UPSERT = 0;
    BLOCK = 1;
    FIRST = 2;
    UPDATE = 3;
}

struct Point {
  1: optional i64 ts;
  2: optional binary value; // json for request, pb for response
}

struct FeatureQueryRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  optional i64 start_ts;
    5:  optional i64 end_ts;
    6:  optional i64 count;
    7:  optional string format;  // "json" or "protobuf"
    8:  optional list<string> filters;
    9: optional string fields;
    255: optional base.Base Base;
}

struct FeatureQueryResponse {
    1: optional Status status;
    2: optional list<Point> point_list;
    255: optional base.BaseResp BaseResp;
}

struct FeatureAddRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    4: optional string format;  // "json" or "protobuf"
    5: optional list<Point> point_list;
    6: optional WritePolicy policy;
    255: optional base.Base Base;
}

struct FeatureAddResponse {
    1: optional Status status;
    255: optional base.BaseResp BaseResp;
}

enum ControlStateHType {
    COUNT = 0;
    MIN = 1;
    MAX = 2;
    CHANGE = 3;
}

enum ControlStateFolType {
    FIRST = 0;
    LAST = 1;
}

enum ControlStatePrecision {
    DISABLE = 0;

    OneSecond   = 10;
    FiveSeconds = 14;
    TenSeconds  = 19;
    OneMinute   = 30;
    FiveMinutes = 34;
    TenMinutes  = 39;
    OneHour     = 50;
    OneDay      = 60;
    OneMonth    = 80;
}

enum ControlStateWindowUnit {
    Second = 0;
    Minute = 1;
    Hour = 2;
    Day = 3;
}

struct ControlStateKvPair {
    1:  required string key
    2:  required string value
}

struct ControlStateWindow {
    1: required i64 start_offset;
    2: required i64 end_offset;
    3: required ControlStateWindowUnit unit;
}

struct ControlStateListDetail {
    1: list<string> detail;
}

struct ControlStateResultDetail {
    1: required bool has_result;
    2: required i64 result;
}

enum ControlStateManagerType {
    FULLGC = 0;
    SUBGC = 1;
    QUERY = 2;
    DEL = 3;
    UPDATE = 4;
    FIELD_LIST = 5;
    FIELD_COUNT = 6;
    ALL_DATA_VALUE = 7;
}

struct ControlStateHsetRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required string value;
    5:  required i64 ttl; // 过期时间
    6:  required ControlStateHType htype; // 写入的类型，区分DC，COUNT， MIN，MAX
    7:  required i64 occur_time;
    8:  required ControlStatePrecision precision;
    255: optional base.Base Base;
}

struct ControlStateCommonResponse {
    1: required Status status;
    255: optional base.BaseResp BaseResp;
}

struct ControlStateHqueryRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required list<ControlStateWindow> windows;
    7:  required ControlStatePrecision precision;
    8:  required ControlStateHType htype;
    255: optional base.Base Base;
}

struct ControlStateHqueryResponse {
    1:  required Status status;
    2:  required list<ControlStateResultDetail> result_list;
    255: optional base.BaseResp BaseResp;
}

struct ControlStateCPCSetRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required list<string> values;
    5:  required i64 ttl; // 过期时间
    6:  required i64 occur_time; // 发生时间
    7:  required ControlStatePrecision precision; // 写入的精度
    8:  required bool dont_upgrade_cpc; // 强制使用hash，不提升到cpc实现,用于兼容老的接口，用于dc list的场景
}

struct ControlStateCPCQueryRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required ControlStatePrecision precision;// 精度，对于0-0d这种，不需要
    5:  required list<ControlStateWindow> windows;
    6:  required bool with_detail; // 是否返回详情，默认为false
}

struct ControlStateCPCQueryResponse {
    1: required Status status;
    2: required list<i64> count_list;
    3: required list<ControlStateListDetail> detail_lists;
    255: optional base.BaseResp BaseResp;
}

struct ControlStateFolSetRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required string value;
    5:  required i64 occur_time; // 发生时间
    6:  required i64 ttl; // 过期时间
    7:  required ControlStateFolType fol_type;
    255: optional base.Base Base;
}

struct ControlStateFolQueryRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    255: optional base.Base Base;
}

struct ControlStateFolQueryResponse {
    1: required Status status;
    2: required string result;
    255: optional base.BaseResp BaseResp;
}

struct ControlStateManagerRequest {
    1:  required string namespace_name;
    2:  required string table_name;
    3:  required binary key;
    4:  required ControlStateManagerType op_type;
    5:  optional list<ControlStateKvPair> field_list;
    6:  required string start_offset;
    7:  required string end_offset;
    8:  optional bool is_cpc;
}

struct ControlStateManagerResponse {
    1: required Status status;
    2: required list<ControlStateKvPair> result;
    255: optional base.BaseResp BaseResp;
}

struct HMSetRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    4: optional list<binary> fields;
    5: optional list<binary> values;
    255: optional base.Base Base;
}

struct HMSetResponse {
    1: optional Status status;
    255: optional base.BaseResp BaseResp;
}

struct HMGetRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    4: optional list<binary> fields;
    255: optional base.Base Base;
}

struct HMGetResponse {
    1: optional Status status;
    2: optional list<binary> values;
    3: optional list<bool> exists;   // Indicates whether each field exists
    255: optional base.BaseResp BaseResp;
}

struct HGetAllRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    255: optional base.Base Base;
}

struct HGetAllResponse {
    1: optional Status status;
    2: optional list<binary> fields;
    3: optional list<binary> values;
    255: optional base.BaseResp BaseResp;
}

struct HLenRequest {
    1: required string namespace_name;
    2: required string table_name;
    3: required binary key;
    255: optional base.Base Base;
}

struct HLenResponse {
    1: optional Status status;
    2: optional i64 len;
    255: optional base.BaseResp BaseResp;
}

service Bcache2ThriftService {
    // feature module
    FeatureQueryResponse  FeatureQuery(1:FeatureQueryRequest request);
    FeatureAddResponse    FeatureAdd(1:FeatureAddRequest request);

    // string module
    GetResponse  Get(1:GetRequest request);
    SetResponse  Set(1:SetRequest request);

    // control_state module
    ControlStateCommonResponse ControlStateHset(1:ControlStateHsetRequest request);
    ControlStateHqueryResponse ControlStateHquery(1:ControlStateHqueryRequest request);
    ControlStateCommonResponse ControlStateFolSet(1:ControlStateFolSetRequest request);
    ControlStateFolQueryResponse ControlStateFolQuery(1:ControlStateFolQueryRequest request);
    ControlStateCommonResponse ControlStateCPCSet(1:ControlStateCPCSetRequest request);
    ControlStateCPCQueryResponse ControlStateCPCQuery(1:ControlStateCPCQueryRequest request);
    ControlStateManagerResponse ControlStateManager(1:ControlStateManagerRequest request)

    // hash module
    HMGetResponse HMGet(1:HMGetRequest request);
    HMSetResponse HMSet(1:HMSetRequest request);
    HGetAllResponse HGetAll(1:HGetAllRequest request);
    HLenResponse HLen(1:HLenRequest request);
}
