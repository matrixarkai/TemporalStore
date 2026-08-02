// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#ifndef BCACHE2_BCACHE2_H_  // NOLINT(build/header_guard)
#define BCACHE2_BCACHE2_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdlib.h>
#include <sys/uio.h>

typedef struct bcache2_table bcache2_table_t;

typedef struct data {
    const void* data;
    size_t size;
} data_t;

typedef enum bcache2_status {
    BCACHE2_OK = 0,
    BCACHE2_CANCELLED = 1,
    BCACHE2_UNKNOWN = 2,
    BCACHE2_INVALID_ARGUMENT = 3,
    BCACHE2_DEADLINE_EXCEEDED = 4,
    BCACHE2_NOT_FOUND = 5,
    BCACHE2_ALREADY_EXISTS = 6,
    BCACHE2_PERMISSION_DENIED = 7,
    BCACHE2_RESOURCE_EXHAUSTED = 8,
    BCACHE2_FAILED_PRECONDITION = 9,
    BCACHE2_ABORTED = 10,
    BCACHE2_OUT_OF_RANGE = 11,
    BCACHE2_UNIMPLEMENTED = 12,
    BCACHE2_INTERNAL = 13,
    BCACHE2_UNAVAILABLE = 14,
    BCACHE2_DATA_LOSS = 15,
    BCACHE2_UNAUTHENTICATED = 16,
} bcache2_status_t;

typedef void (*bcache2_callback_t)(void* args);

// client options
typedef struct bcache2_options bcache2_options_t;
bcache2_options_t* bcache2_options_init();
void bcache2_options_destory(bcache2_options_t* options);
void bcache2_options_set(bcache2_options_t* options, const char* name, const char* value);

// table options
typedef struct bcache2_table_options bcache2_table_options_t;
bcache2_table_options_t* bcache2_tableoptions_init();
void bcache2_tableoptions_destory(bcache2_table_options_t* options);
void bcache2_tableoptions_set(bcache2_table_options_t* options, const char* name,
                              const char* value);

// execution
typedef struct bcache2_execution bcache2_execution_t;
bcache2_execution_t* bcache2_execution_init(int64_t trace_id, int64_t timeout);
void bcache2_execution_destory(bcache2_execution_t* execution);
void bcache2_execution_add_request(bcache2_execution_t* execution, uint32_t cmd,
                                   data_t partition_key, data_t request);
int bcache2_execution_get_status(bcache2_execution_t* execution, int request_index);
const char* bcache2_execution_get_message(bcache2_execution_t* execution, int request_index);
data_t bcache2_execution_get_response(bcache2_execution_t* execution, int request_index);

void bcache2_init(bcache2_options_t* options);

void bcache2_destory();

int bcache2_open(const char* uri, bcache2_table_options_t* options, bcache2_table_t** table);

void bcache2_close(bcache2_table_t* table);

void bcache2_execute(bcache2_table_t* table, bcache2_execution_t* execution,
                     bcache2_callback_t callback, void* callback_args);

#ifdef __cplusplus
}
#endif

#endif  // BCACHE2_BCACHE2_H_
