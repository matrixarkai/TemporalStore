// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <matrixobjectstore/matrixobjectstore.h>

// override matrixobjectstore APIs to inject hang, failure and crash if FIU is enabled
// otherwise, these are just zero-overhead macro aliases
#ifdef FIU_ENABLE

#include <cstring>
#include <thread>

#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/logging.h"
#include "libfiu/libfiu/fiu.h"

DECLARE_int32(store_fiu_hang_interval_ms);

#define MATRIXOBJECTSTORE_FAULT_INJECT_HANG(path, api)                         \
    fiu_do_on(                                                         \
        path, do {                                                     \
            LOG_INFO("Inject hang").put("Api", api).put("Path", path); \
            bcache2::CoSleep(FLAGS_store_fiu_hang_interval_ms * 1000); \
        } while (false))

#define MATRIXOBJECTSTORE_FAULT_INJECT_FAILURE(path, api, error_result)           \
    fiu_do_on(                                                            \
        path, do {                                                        \
            LOG_INFO("Inject failure").put("Api", api).put("Path", path); \
            message->status_ = STATUS_UNKNOWN_ERR;                        \
            return error_result;                                          \
        } while (false))

#define MATRIXOBJECTSTORE_FAULT_INJECT_MARK_FAILURE(path, api, error_result)      \
    fiu_do_on(                                                            \
        path, do {                                                        \
            LOG_INFO("Inject failure").put("Api", api).put("Path", path); \
            message->status_ = error_result;                              \
        } while (false))

#define MATRIXOBJECTSTORE_FAULT_INJECT_CRASH(path, api)                            \
    fiu_do_on(                                                             \
        path, do {                                                         \
            LOG_WARNING("Inject crash").put("Api", api).put("Path", path); \
            LOG_FLUSH();                                                   \
            _Exit(0);                                                      \
        } while (false))

// inject hang, failure and crash all together
#define MATRIXOBJECTSTORE_FAULT_INJECT(path, api, error_result)                     \
    do {                                                                    \
        MATRIXOBJECTSTORE_FAULT_INJECT_HANG(path "/hang", api);                     \
        MATRIXOBJECTSTORE_FAULT_INJECT_FAILURE(path "/failure", api, error_result); \
        MATRIXOBJECTSTORE_FAULT_INJECT_CRASH(path "/crash", api);                   \
    } while (false)

#define MATRIXOBJECTSTORE_FAULT_INJECT_DISTORT(path, api, buffer, size)            \
    fiu_do_on(                                                             \
        path, do {                                                         \
            LOG_DEBUG("Inject failure").put("Api", api).put("Path", path); \
            char* tmp = new char[size];                                    \
            memcpy(buffer, tmp, size);                                     \
            delete[] tmp;                                                  \
        } while (false))

typedef void (*bcache2_matrixobjectstore_rw_callback)(ssize_t size_written,
                                              struct matrixobjectstore_message* message, void* args);

struct Bcache2MatrixObjectStoreContext {
    bcache2_matrixobjectstore_rw_callback cb;
    void* args;
    const void* buffer;
};

#define MATRIXOBJECTSTORE_FAULT_INJECT_ASYNC(path, api, error_result, callback, args, buffer)     \
    do {                                                                                  \
        args = new Bcache2MatrixObjectStoreContext{callback, args, buffer};                       \
        callback = +[](ssize_t size, struct matrixobjectstore_message* message, void* args) {     \
            auto ctx = reinterpret_cast<Bcache2MatrixObjectStoreContext*>(args);                  \
            MATRIXOBJECTSTORE_FAULT_INJECT_HANG(path "/hang", api);                               \
            MATRIXOBJECTSTORE_FAULT_INJECT_MARK_FAILURE(path "/failure", api, error_result);      \
            MATRIXOBJECTSTORE_FAULT_INJECT_CRASH(path "/crash", api);                             \
            void* user_buffer = const_cast<void*>(ctx->buffer);                           \
            MATRIXOBJECTSTORE_FAULT_INJECT_DISTORT(path "/data_distort", api, user_buffer, size); \
            ctx->cb(size, message, ctx->args);                                            \
            delete ctx;                                                                   \
        };                                                                                \
    } while (false)

inline void MATRIXOBJECTSTORE_ASYNC_WRITE(struct matrixobjectstore_blob* blob, const void* buffer, size_t length,
                                  struct matrixobjectstore_io_options* options,
                                  struct matrixobjectstore_message* message,
                                  matrixobjectstore_write_callback callback, void* args) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/io/write", "matrixobjectstore_async_write", /* void */);
    MATRIXOBJECTSTORE_FAULT_INJECT_ASYNC("store/matrixobjectstore/io/async_write", "matrixobjectstore_async_write",
                                 STATUS_IO_ERROR, callback, args, buffer);
    return matrixobjectstore_async_write(blob, buffer, length, options, message, callback, args);
}  // NOLINT(whitespace/indent)

inline void MATRIXOBJECTSTORE_ASYNC_PREAD(struct matrixobjectstore_blob* blob, void* buffer, size_t length,
                                  size_t offset, struct matrixobjectstore_io_options* options,
                                  struct matrixobjectstore_message* message,
                                  matrixobjectstore_read_callback callback, void* args) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/io/read", "matrixobjectstore_async_pread", /* void */);
    MATRIXOBJECTSTORE_FAULT_INJECT_ASYNC("store/matrixobjectstore/io/async_read", "matrixobjectstore_async_pread",
                                 STATUS_IO_ERROR, callback, args, buffer);
    return matrixobjectstore_async_pread(blob, buffer, length, offset, options, message, callback, args);
}

inline struct matrixobjectstore_blob* MATRIXOBJECTSTORE_OPEN(const char* blob_name, int open_mode,
                                             struct matrixobjectstore_open_options* options,
                                             struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/open", "matrixobjectstore_open", nullptr);
    return matrixobjectstore_open(blob_name, open_mode, options, message);
}

inline void MATRIXOBJECTSTORE_CLOSE(struct matrixobjectstore_blob* blob, struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/close", "matrixobjectstore_close", /* void */);
    return matrixobjectstore_close(blob, message);
}

inline bool MATRIXOBJECTSTORE_STAT(const char* blob_name, struct matrixobjectstore_stat_t* stat,
                           struct matrixobjectstore_stat_options* options,
                           struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/stat", "matrixobjectstore_stat", false);
    return matrixobjectstore_stat(blob_name, stat, options, message);
}

inline bool MATRIXOBJECTSTORE_DELETE(const char* blob_name, struct matrixobjectstore_delete_options* options,
                             struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/delete", "matrixobjectstore_delete", false);
    return matrixobjectstore_delete(blob_name, options, message);
}

inline bool MATRIXOBJECTSTORE_RENAME(const char* src_blob_name, const char* target_blob_name,
                             struct matrixobjectstore_rename_options* options,
                             struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/rename", "matrixobjectstore_rename", false);
    return matrixobjectstore_rename(src_blob_name, target_blob_name, options, message);
}
inline bool MATRIXOBJECTSTORE_CREATE_INLINE_BLOB(const char* blob_name,
                                         struct matrixobjectstore_create_inline_blob_options* options,
                                         struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/create_inline_blob",
                           "matrixobjectstore_creat_inline_blob", false);
    return matrixobjectstore_create_inline_blob(blob_name, options, message);
}

inline bool MATRIXOBJECTSTORE_UPDATE_INLINE_BLOB(const char* blob_name,
                                         const struct matrixobjectstore_inline_blob_stat* new_stat,
                                         struct matrixobjectstore_update_inline_blob_options* options,
                                         struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/update_inline_blob",
                           "matrixobjectstore_update_inline_blob", false);
    return matrixobjectstore_update_inline_blob(blob_name, new_stat, options, message);
}

inline bool MATRIXOBJECTSTORE_STAT_INLINE_BLOB(const char* blob_name,
                                       struct matrixobjectstore_inline_blob_stat* curr_stat,
                                       struct matrixobjectstore_stat_inline_blob_options* options,
                                       struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/stat_inline_blob", "matrixobjectstore_stat_inline_blob",
                           false);
    return matrixobjectstore_stat_inline_blob(blob_name, curr_stat, options, message);
}  // NOLINT(whitespace/indent)

inline struct matrixobjectstore_pool* MATRIXOBJECTSTORE_OPEN_POOL(const char* pool_name,
                                                  struct matrixobjectstore_open_pool_options* options,
                                                  struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/open_pool", "matrixobjectstore_open_pool", nullptr);
    return matrixobjectstore_open_pool(pool_name, options, message);
}

inline bool MATRIXOBJECTSTORE_TRAVERSE_POOL(struct matrixobjectstore_pool* pool, struct matrixobjectstore_entry* entry,
                                    struct matrixobjectstore_traverse_options* options,
                                    struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/traverse_pool", "matrixobjectstore_traverse_pool", false);
    return matrixobjectstore_traverse_pool(pool, entry, options, message);
}

inline void MATRIXOBJECTSTORE_CLOSE_POOL(struct matrixobjectstore_pool* pool, struct matrixobjectstore_message* message) {
    MATRIXOBJECTSTORE_FAULT_INJECT("store/matrixobjectstore/ioctl/close_pool", "matrixobjectstore_close_pool", /* void */);
    matrixobjectstore_close_pool(pool, message);
}

#else

#define MATRIXOBJECTSTORE_ASYNC_WRITE matrixobjectstore_async_write
#define MATRIXOBJECTSTORE_ASYNC_PREAD matrixobjectstore_async_pread
#define MATRIXOBJECTSTORE_OPEN matrixobjectstore_open
#define MATRIXOBJECTSTORE_CLOSE matrixobjectstore_close
#define MATRIXOBJECTSTORE_STAT matrixobjectstore_stat
#define MATRIXOBJECTSTORE_DELETE matrixobjectstore_delete
#define MATRIXOBJECTSTORE_RENAME matrixobjectstore_rename
#define MATRIXOBJECTSTORE_CREATE_INLINE_BLOB matrixobjectstore_create_inline_blob
#define MATRIXOBJECTSTORE_UPDATE_INLINE_BLOB matrixobjectstore_update_inline_blob
#define MATRIXOBJECTSTORE_STAT_INLINE_BLOB matrixobjectstore_stat_inline_blob
#define MATRIXOBJECTSTORE_OPEN_POOL matrixobjectstore_open_pool
#define MATRIXOBJECTSTORE_TRAVERSE_POOL matrixobjectstore_traverse_pool
#define MATRIXOBJECTSTORE_CLOSE_POOL matrixobjectstore_close_pool

#endif
