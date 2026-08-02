// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <bytestore/bytestore.h>

// override bytestore APIs to inject hang, failure and crash if FIU is enabled
// otherwise, these are just zero-overhead macro aliases
#ifdef FIU_ENABLE

#include <cstring>
#include <thread>

#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/logging.h"
#include "libfiu/libfiu/fiu.h"

DECLARE_int32(store_fiu_hang_interval_ms);

#define BYTESTORE_FAULT_INJECT_HANG(path, api)                         \
    fiu_do_on(                                                         \
        path, do {                                                     \
            LOG_INFO("Inject hang").put("Api", api).put("Path", path); \
            bcache2::CoSleep(FLAGS_store_fiu_hang_interval_ms * 1000); \
        } while (false))

#define BYTESTORE_FAULT_INJECT_FAILURE(path, api, error_result)           \
    fiu_do_on(                                                            \
        path, do {                                                        \
            LOG_INFO("Inject failure").put("Api", api).put("Path", path); \
            message->status_ = STATUS_UNKNOWN_ERR;                        \
            return error_result;                                          \
        } while (false))

#define BYTESTORE_FAULT_INJECT_MARK_FAILURE(path, api, error_result)      \
    fiu_do_on(                                                            \
        path, do {                                                        \
            LOG_INFO("Inject failure").put("Api", api).put("Path", path); \
            message->status_ = error_result;                              \
        } while (false))

#define BYTESTORE_FAULT_INJECT_CRASH(path, api)                            \
    fiu_do_on(                                                             \
        path, do {                                                         \
            LOG_WARNING("Inject crash").put("Api", api).put("Path", path); \
            LOG_FLUSH();                                                   \
            _Exit(0);                                                      \
        } while (false))

// inject hang, failure and crash all together
#define BYTESTORE_FAULT_INJECT(path, api, error_result)                     \
    do {                                                                    \
        BYTESTORE_FAULT_INJECT_HANG(path "/hang", api);                     \
        BYTESTORE_FAULT_INJECT_FAILURE(path "/failure", api, error_result); \
        BYTESTORE_FAULT_INJECT_CRASH(path "/crash", api);                   \
    } while (false)

#define BYTESTORE_FAULT_INJECT_DISTORT(path, api, buffer, size)            \
    fiu_do_on(                                                             \
        path, do {                                                         \
            LOG_DEBUG("Inject failure").put("Api", api).put("Path", path); \
            char* tmp = new char[size];                                    \
            memcpy(buffer, tmp, size);                                     \
            delete[] tmp;                                                  \
        } while (false))

typedef void (*bcache2_bytestore_rw_callback)(ssize_t size_written,
                                              struct bytestore_message* message, void* args);

struct Bcache2BytestoreContext {
    bcache2_bytestore_rw_callback cb;
    void* args;
    const void* buffer;
};

#define BYTESTORE_FAULT_INJECT_ASYNC(path, api, error_result, callback, args, buffer)     \
    do {                                                                                  \
        args = new Bcache2BytestoreContext{callback, args, buffer};                       \
        callback = +[](ssize_t size, struct bytestore_message* message, void* args) {     \
            auto ctx = reinterpret_cast<Bcache2BytestoreContext*>(args);                  \
            BYTESTORE_FAULT_INJECT_HANG(path "/hang", api);                               \
            BYTESTORE_FAULT_INJECT_MARK_FAILURE(path "/failure", api, error_result);      \
            BYTESTORE_FAULT_INJECT_CRASH(path "/crash", api);                             \
            void* user_buffer = const_cast<void*>(ctx->buffer);                           \
            BYTESTORE_FAULT_INJECT_DISTORT(path "/data_distort", api, user_buffer, size); \
            ctx->cb(size, message, ctx->args);                                            \
            delete ctx;                                                                   \
        };                                                                                \
    } while (false)

inline void BYTESTORE_ASYNC_WRITE(struct bytestore_blob* blob, const void* buffer, size_t length,
                                  struct bytestore_io_options* options,
                                  struct bytestore_message* message,
                                  bytestore_write_callback callback, void* args) {
    BYTESTORE_FAULT_INJECT("store/bytestore/io/write", "bytestore_async_write", /* void */);
    BYTESTORE_FAULT_INJECT_ASYNC("store/bytestore/io/async_write", "bytestore_async_write",
                                 STATUS_IO_ERROR, callback, args, buffer);
    return bytestore_async_write(blob, buffer, length, options, message, callback, args);
}  // NOLINT(whitespace/indent)

inline void BYTESTORE_ASYNC_PREAD(struct bytestore_blob* blob, void* buffer, size_t length,
                                  size_t offset, struct bytestore_io_options* options,
                                  struct bytestore_message* message,
                                  bytestore_read_callback callback, void* args) {
    BYTESTORE_FAULT_INJECT("store/bytestore/io/read", "bytestore_async_pread", /* void */);
    BYTESTORE_FAULT_INJECT_ASYNC("store/bytestore/io/async_read", "bytestore_async_pread",
                                 STATUS_IO_ERROR, callback, args, buffer);
    return bytestore_async_pread(blob, buffer, length, offset, options, message, callback, args);
}

inline struct bytestore_blob* BYTESTORE_OPEN(const char* blob_name, int open_mode,
                                             struct bytestore_open_options* options,
                                             struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/open", "bytestore_open", nullptr);
    return bytestore_open(blob_name, open_mode, options, message);
}

inline void BYTESTORE_CLOSE(struct bytestore_blob* blob, struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/close", "bytestore_close", /* void */);
    return bytestore_close(blob, message);
}

inline bool BYTESTORE_STAT(const char* blob_name, struct bytestore_stat_t* stat,
                           struct bytestore_stat_options* options,
                           struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/stat", "bytestore_stat", false);
    return bytestore_stat(blob_name, stat, options, message);
}

inline bool BYTESTORE_DELETE(const char* blob_name, struct bytestore_delete_options* options,
                             struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/delete", "bytestore_delete", false);
    return bytestore_delete(blob_name, options, message);
}

inline bool BYTESTORE_RENAME(const char* src_blob_name, const char* target_blob_name,
                             struct bytestore_rename_options* options,
                             struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/rename", "bytestore_rename", false);
    return bytestore_rename(src_blob_name, target_blob_name, options, message);
}
inline bool BYTESTORE_CREATE_INLINE_BLOB(const char* blob_name,
                                         struct bytestore_create_inline_blob_options* options,
                                         struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/create_inline_blob",
                           "bytestore_creat_inline_blob", false);
    return bytestore_create_inline_blob(blob_name, options, message);
}

inline bool BYTESTORE_UPDATE_INLINE_BLOB(const char* blob_name,
                                         const struct bytestore_inline_blob_stat* new_stat,
                                         struct bytestore_update_inline_blob_options* options,
                                         struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/update_inline_blob",
                           "bytestore_update_inline_blob", false);
    return bytestore_update_inline_blob(blob_name, new_stat, options, message);
}

inline bool BYTESTORE_STAT_INLINE_BLOB(const char* blob_name,
                                       struct bytestore_inline_blob_stat* curr_stat,
                                       struct bytestore_stat_inline_blob_options* options,
                                       struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/stat_inline_blob", "bytestore_stat_inline_blob",
                           false);
    return bytestore_stat_inline_blob(blob_name, curr_stat, options, message);
}  // NOLINT(whitespace/indent)

inline struct bytestore_pool* BYTESTORE_OPEN_POOL(const char* pool_name,
                                                  struct bytestore_open_pool_options* options,
                                                  struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/open_pool", "bytestore_open_pool", nullptr);
    return bytestore_open_pool(pool_name, options, message);
}

inline bool BYTESTORE_TRAVERSE_POOL(struct bytestore_pool* pool, struct bytestore_entry* entry,
                                    struct bytestore_traverse_options* options,
                                    struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/traverse_pool", "bytestore_traverse_pool", false);
    return bytestore_traverse_pool(pool, entry, options, message);
}

inline void BYTESTORE_CLOSE_POOL(struct bytestore_pool* pool, struct bytestore_message* message) {
    BYTESTORE_FAULT_INJECT("store/bytestore/ioctl/close_pool", "bytestore_close_pool", /* void */);
    bytestore_close_pool(pool, message);
}

#else

#define BYTESTORE_ASYNC_WRITE bytestore_async_write
#define BYTESTORE_ASYNC_PREAD bytestore_async_pread
#define BYTESTORE_OPEN bytestore_open
#define BYTESTORE_CLOSE bytestore_close
#define BYTESTORE_STAT bytestore_stat
#define BYTESTORE_DELETE bytestore_delete
#define BYTESTORE_RENAME bytestore_rename
#define BYTESTORE_CREATE_INLINE_BLOB bytestore_create_inline_blob
#define BYTESTORE_UPDATE_INLINE_BLOB bytestore_update_inline_blob
#define BYTESTORE_STAT_INLINE_BLOB bytestore_stat_inline_blob
#define BYTESTORE_OPEN_POOL bytestore_open_pool
#define BYTESTORE_TRAVERSE_POOL bytestore_traverse_pool
#define BYTESTORE_CLOSE_POOL bytestore_close_pool

#endif
