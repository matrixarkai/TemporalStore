#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct temporalstore_client temporalstore_client_t;

typedef enum temporalstore_risk_precision {
    TEMPORALSTORE_RISK_ONE_SECOND = 0,
    TEMPORALSTORE_RISK_FIVE_SECONDS = 1,
    TEMPORALSTORE_RISK_TEN_SECONDS = 2,
    TEMPORALSTORE_RISK_ONE_MINUTE = 3,
    TEMPORALSTORE_RISK_FIVE_MINUTES = 4,
    TEMPORALSTORE_RISK_TEN_MINUTES = 5,
    TEMPORALSTORE_RISK_ONE_HOUR = 6,
    TEMPORALSTORE_RISK_ONE_DAY = 7,
    TEMPORALSTORE_RISK_ONE_MONTH = 8,
} temporalstore_risk_precision_t;

typedef enum temporalstore_window_unit {
    TEMPORALSTORE_WINDOW_SECOND = 0,
    TEMPORALSTORE_WINDOW_MINUTE = 1,
    TEMPORALSTORE_WINDOW_HOUR = 2,
    TEMPORALSTORE_WINDOW_DAY = 3,
} temporalstore_window_unit_t;

typedef enum temporalstore_feature_filter_op {
    TEMPORALSTORE_FEATURE_FILTER_EQUAL = 0,
    TEMPORALSTORE_FEATURE_FILTER_NOT_EQUAL = 1,
    TEMPORALSTORE_FEATURE_FILTER_GREATER_THAN = 2,
    TEMPORALSTORE_FEATURE_FILTER_LESS_THAN = 3,
} temporalstore_feature_filter_op_t;

typedef struct temporalstore_options {
    const char* metaserver_addr;
    const char* metaserver_consul;
    const char* namespace_name;
    const char* table_name;
    const char* idc;
    const char* host;
    const char* psm;
    const char* log_dir;
    int log_level;
    int io_timeout_ms;
    int connect_timeout_ms;
    int request_timeout_ms;
    int max_read_retries;
    int max_write_retries;
    int retry_backoff_ms;
    int max_feature_points_per_request;
    uint64_t max_feature_query_count;
    uint64_t max_key_bytes;
    uint64_t max_value_bytes;
    int pin_primary;
} temporalstore_options_t;

typedef struct temporalstore_hash_entry {
    const char* key;
    const char* field;
    const char* value;
    const char* route_json;
} temporalstore_hash_entry_t;

typedef struct temporalstore_string_array {
    size_t count;
    char** values;
} temporalstore_string_array_t;

typedef struct temporalstore_feature_point {
    uint64_t timestamp;
    const char* value;
} temporalstore_feature_point_t;

typedef struct temporalstore_feature_point_array {
    size_t count;
    temporalstore_feature_point_t* points;
} temporalstore_feature_point_array_t;

typedef struct temporalstore_feature_filter {
    const char* field;
    temporalstore_feature_filter_op_t op;
    uint64_t value;
} temporalstore_feature_filter_t;

typedef struct temporalstore_sequence_feature_row {
    uint64_t timestamp;
    uint64_t gid;
    uint32_t action_type;
    uint32_t duration;
    uint64_t author_id;
} temporalstore_sequence_feature_row_t;

typedef struct temporalstore_sequence_feature_row_array {
    size_t count;
    temporalstore_sequence_feature_row_t* rows;
} temporalstore_sequence_feature_row_array_t;

typedef struct temporalstore_ips_feature_stat {
    int64_t id;
    int32_t slot;
    int has_slot;
    int32_t type;
    int32_t v1;
    int32_t v2;
} temporalstore_ips_feature_stat_t;

typedef struct temporalstore_ips_feature_array {
    size_t count;
    temporalstore_ips_feature_stat_t* features;
} temporalstore_ips_feature_array_t;

void temporalstore_options_init(temporalstore_options_t* options);
void temporalstore_free_string(char* value);
void temporalstore_string_array_free(temporalstore_string_array_t* array);
void temporalstore_feature_point_array_free(temporalstore_feature_point_array_t* array);
void temporalstore_sequence_feature_row_array_free(
    temporalstore_sequence_feature_row_array_t* array);
void temporalstore_ips_feature_array_free(temporalstore_ips_feature_array_t* array);

int temporalstore_connect(const temporalstore_options_t* options, temporalstore_client_t** client,
                          char** error_message);
int temporalstore_close(temporalstore_client_t* client, char** error_message);

int temporalstore_put_string(temporalstore_client_t* client, const char* key, const char* value,
                             char** error_message);
int temporalstore_put_string_with_ttl(temporalstore_client_t* client, const char* key,
                                      const char* value, uint64_t ttl_ms,
                                      char** error_message);
int temporalstore_get_string(temporalstore_client_t* client, const char* key, char** value,
                             char** error_message);
int temporalstore_delete_object(temporalstore_client_t* client, const char* key,
                                char** error_message);
int temporalstore_expire(temporalstore_client_t* client, const char* key, uint64_t ttl_ms,
                         char** error_message);
int temporalstore_ttl(temporalstore_client_t* client, const char* key, uint64_t* ttl_ms,
                      char** error_message);
int temporalstore_hset(temporalstore_client_t* client, const char* key, const char* field,
                       const char* value, char** error_message);
int temporalstore_hget(temporalstore_client_t* client, const char* key, const char* field,
                       char** value, char** error_message);
int temporalstore_hdel(temporalstore_client_t* client, const char* key, const char* field,
                       char** error_message);
int temporalstore_sadd(temporalstore_client_t* client, const char* key, const char* member,
                       char** error_message);
int temporalstore_smembers(temporalstore_client_t* client, const char* key,
                           temporalstore_string_array_t* members, char** error_message);

int temporalstore_matrixark_batch_append_records(temporalstore_client_t* client,
                                                 const temporalstore_hash_entry_t* entries,
                                                 size_t entry_count, const char* count_key,
                                                 const char* count_value,
                                                 char** error_message);
int temporalstore_matrixark_batch_append_records_v2(temporalstore_client_t* client,
                                                    const temporalstore_hash_entry_t* entries,
                                                    size_t entry_count, const char* count_key,
                                                    const char* count_value,
                                                    const char* append_options_json,
                                                    char** error_message);
int temporalstore_matrixark_retrieve_context_pack(temporalstore_client_t* client,
                                                  const char* request_json,
                                                  char** response_json,
                                                  char** error_message);

int temporalstore_add_feature_points(temporalstore_client_t* client, const char* key,
                                     const temporalstore_feature_point_t* points, size_t count,
                                     char** error_message);
int temporalstore_query_feature_points(temporalstore_client_t* client, const char* key,
                                       uint64_t start_ts, uint64_t end_ts, uint64_t count,
                                       temporalstore_feature_point_array_t* points,
                                       char** error_message);
int temporalstore_query_feature_points_with_filters(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_feature_point_array_t* points, char** error_message);
int temporalstore_add_sequence_feature_rows(temporalstore_client_t* client, const char* key,
                                            const temporalstore_sequence_feature_row_t* rows,
                                            size_t count, char** error_message);
int temporalstore_query_sequence_feature_rows(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_sequence_feature_row_array_t* rows, char** error_message);

int temporalstore_add_ips_instance(temporalstore_client_t* client, const char* table, int64_t uid,
                                   int64_t timestamp_us, int32_t action_type,
                                   int32_t logical_table,
                                   const temporalstore_ips_feature_stat_t* features,
                                   size_t feature_count, char** error_message);
int temporalstore_query_ips_last_instances(temporalstore_client_t* client, const char* table,
                                           int64_t uid, int32_t action_type,
                                           int32_t logical_table, int32_t slot, int32_t top_k,
                                           int64_t last_instances,
                                           temporalstore_ips_feature_array_t* features,
                                           char** error_message);

int temporalstore_risk_increment(temporalstore_client_t* client, const char* key, int64_t amount,
                                 uint64_t ttl_seconds,
                                 temporalstore_risk_precision_t precision, const char* uuid,
                                 uint64_t occur_time_seconds, char** error_message);
int temporalstore_risk_count(temporalstore_client_t* client, const char* key,
                             temporalstore_risk_precision_t precision, int64_t window_start,
                             int64_t window_end, temporalstore_window_unit_t window_unit,
                             int64_t* count, char** error_message);

#ifdef __cplusplus
}
#endif
