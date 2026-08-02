// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/store/local_file_store.h"

#include <absl/strings/match.h>
#include <byte/base/closure.h>
#include <byte/include/assert.h>
#include <byte/io/file_path.h>
#include <byte/io/file_util.h>
#include <common/logging.h>
#include <dirent.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <sys/file.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#if defined(__CYGWIN__)
extern "C" {
ssize_t pread(int fd, void* buf, size_t count, off_t offset);
ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset);
int ftruncate(int fd, off_t length);
}
#endif

#include "common/controller.h"
#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/function_closure.h"
#include "common/scoped_invoker.h"
#include "common/string_utils.h"
#include "stream/store/store.h"

DECLARE_int32(store_fiu_hang_interval_ms);

namespace bcache2 {
namespace stream {

#ifndef FIU_ENABLE

#define FILE_STORE_INJECT_ACTION(name, api, path)
#define FILE_STORE_INJECT_ACTION_WITH_TYPE(name, api, path, type)
#define FILE_STORE_FAULT_INJECT(name, api, path)

#else

#define FILE_STORE_INJECT_ACTION(name, api, path)                             \
    do {                                                                      \
        if (std::string(name).find("fail") != std::string::npos) {            \
            LOG_DEBUG("Inject fail").put("Api", api).put("Path", path);       \
            ctrl->set_status(Status::StoreInternal(""));                      \
            return;                                                           \
        } else if (std::string(name).find("hang") != std::string::npos) {     \
            LOG_DEBUG("Inject hang").put("Api", api).put("Path", path);       \
            std::this_thread::sleep_for(                                      \
                std::chrono::milliseconds(FLAGS_store_fiu_hang_interval_ms)); \
        }                                                                     \
    } while (false);

#define FILE_STORE_INJECT_ACTION_WITH_TYPE(name, api, path, type) \
    do {                                                          \
        if (std::string(path).find(type) != std::string::npos) {  \
            FILE_STORE_INJECT_ACTION(name, api, path);            \
        }                                                         \
    } while (false);

#define FILE_STORE_FAULT_INJECT(name, api, path)                                 \
    do {                                                                         \
        fiu_do_on(name, FILE_STORE_INJECT_ACTION(name, api, path));              \
        fiu_do_on((std::string("index/") + name).data(),                         \
                  FILE_STORE_INJECT_ACTION_WITH_TYPE(name, api, path, "index")); \
        fiu_do_on((std::string("page/") + name).data(),                          \
                  FILE_STORE_INJECT_ACTION_WITH_TYPE(name, api, path, "page"));  \
        fiu_do_on((std::string("oplog/") + name).data(),                         \
                  FILE_STORE_INJECT_ACTION_WITH_TYPE(name, api, path, "oplog")); \
    } while (false);

#endif /* FIU_ENABLE */

namespace {

std::string GetFile(const std::string& uri) {
    const std::string kPrefix = "file://";
    if (!absl::StartsWith(uri, kPrefix)) {
        return "";
    }
    return uri.substr(kPrefix.size());
}

Status PrepareDir(const std::string& dir) {
    if (byte::DirectoryExists(dir)) {
        return Status::OK();
    }
    byte::Status status = byte::CreateDirectoryRecursive(dir);
    if (!status.ok()) {
        LOG_ERROR("Create dir failed").put("Dir", dir).put("Error", status.ToString());
        return Status::StoreInternal(status.ToString());
    }
    LOG_INFO("Create dir ok").put("Dir", dir);
    return Status::OK();
}

}  // namespace

FileCondition::FileCondition(const Store::Condition& condition)
    : uri_(condition.name), data_(condition.data) {}

FileCondition::~FileCondition() {
    if (fd_ >= 0) {
        Unlock();
    }
}

Status FileCondition::Lock() {
    if (uri_.empty()) {
        return Status::OK();
    }

    std::string file = GetFile(uri_);
    if (file.empty()) {
        LOG_ERROR("Uri format error").put("Uri", uri_);
        return Status::StoreInternal("Uri format error");
    }

    fd_ = open(file.c_str(), O_RDONLY | O_SYNC, 0444);
    if (fd_ < 0) {
        if (errno == ENOENT) {
            return Status::StoreNotFound("File not found");
        }
        LOG_ERROR("Open file failed").put("File", file).put("Error", strerror(errno));
        return Status::StoreInternal(strerror(errno));
    }

    int res = flock(fd_, LOCK_EX);
    if (res != 0) {
        LOG_ERROR("Flock file failed").put("File", file).put("Error", strerror(errno));
        return Status::StoreInternal(strerror(errno));
    }

    struct stat s;
    res = stat(file.c_str(), &s);
    if (res != 0) {
        LOG_ERROR("Stat file failed").put("File", file).put("Error", strerror(errno));
        return Status::StoreInternal(strerror(errno));
    }

    std::string data;
    data.resize(s.st_size);
    ssize_t io_size = pread(fd_, &data[0], s.st_size, 0);
    if (io_size != s.st_size) {
        LOG_ERROR("Pwrite file failed").put("File", file).put("Error", strerror(errno));
        return Status::StoreInternal(strerror(errno));
    }

    if (data != "" && data != std::string(data_.data(), data_.size())) {
        LOG_WARNING("Data mismatch")
            .put("File", file)
            .put("RealData", DebugRawString(data))
            .put("ExpectedData", DebugRawString(data_.data(), data_.size()));
        return Status::StoreConditionFailed("Data mismatch");
    }

    return Status::OK();
}

void FileCondition::Unlock() {
    if (fd_ < 0) {
        LOG_WARNING("No need unlock").put("File", uri_);
        return;
    }

    int res = flock(fd_, LOCK_UN);
    if (res != 0) {
        LOG_ERROR("Flock file failed").put("File", uri_).put("Error", strerror(errno));
    }

    res = close(fd_);
    if (res != 0) {
        LOG_WARNING("Close failed").put("File", uri_).put("Fd", fd_).put("Error", strerror(errno));
    }
}

void File::Close() {
    int res = close(fd_);
    if (res != 0) {
        LOG_WARNING("Close failed")
            .put("File", pathname_)
            .put("Fd", fd_)
            .put("Error", strerror(errno));
    }
}

void File::Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) {
    ScopedInvoker done(callback);
    FILE_STORE_FAULT_INJECT("store/file/io/fail/append", "Append", pathname_);

    done.Release();
    byte::AsyncThread* thread = IsCoContext() ? byte::GetCurrentThread() : nullptr;
    auto func = [this, ctrl, data, size, callback, thread] {
        ScopedInvoker done(callback, thread);
        FILE_STORE_FAULT_INJECT("store/file/io/hang/append", "Append", pathname_);

        metrics_.append_qps->get()->Increment();
        metrics_.append_throughput->get()->Add(size);
        ScopedLatency latency(metrics_.append_latency->get());

        struct stat s;
        int res = stat(pathname_.c_str(), &s);
        if (res != 0) {
            LOG_ERROR("Stat file failed").put("File", pathname_).put("Error", strerror(errno));
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
            return;
        }

        if ((s.st_mode & S_IWUSR) == 0) {
            LOG_ERROR("File is read-only mode").put("File", pathname_);
            ctrl->set_status(Status::StoreInternal("File is read-only mode"));
            return;
        }

        ssize_t io_size = pwrite(fd_, data, size, length_);
        if (io_size != static_cast<ssize_t>(size)) {
            LOG_ERROR("Pwrite failed")
                .put("File", pathname_)
                .put("Fd", fd_)
                .put("Offset", length_)
                .put("Size", size);
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
            return;
        }
        length_ += io_size;
        ctrl->set_status(Status::OK());
    };
    bg_thread_pool_.PushTask(NewFuncClosure(func));
}

void File::Read(Controller* ctrl, size_t offset, void* data, size_t size, Closure<void>* callback) {
    ScopedInvoker done(callback);
    FILE_STORE_FAULT_INJECT("store/file/io/fail/read", "Read", pathname_);

    done.Release();
    byte::AsyncThread* thread = IsCoContext() ? byte::GetCurrentThread() : nullptr;
    auto func = [this, ctrl, offset, data, size, callback, thread] {
        ScopedInvoker done(callback, thread);
        FILE_STORE_FAULT_INJECT("store/file/io/hang/read", "Read", pathname_);

        metrics_.read_qps->get()->Increment();
        metrics_.read_throughput->get()->Add(size);
        ScopedLatency latency(metrics_.read_latency->get());

        ssize_t io_size = pread(fd_, data, size, offset);
        if (io_size != static_cast<ssize_t>(size)) {
            LOG_ERROR("Pread failed")
                .put("File", pathname_)
                .put("Fd", fd_)
                .put("Offset", offset)
                .put("Size", size)
                .put("Length", length_);
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
            return;
        }
        ctrl->set_status(Status::OK());
    };
    bg_thread_pool_.PushTask(NewFuncClosure(func));
}

#define CONDITION_GUARD(condition)                                                      \
    std::unique_ptr<FileCondition> condition_guard(new FileCondition(condition));       \
    Status _status = condition_guard->Lock();                                           \
    if (_status.IsNotFound() || _status.IsStoreConditionFailed()) { \
        LOG_ERROR("Condition error").put("Error", _status.ToString());                  \
        ctrl->set_status(Status::StoreConditionFailed("Condition error"));              \
        return;                                                                         \
    }                                                                                   \
    if (!_status.ok()) {                                                                \
        LOG_ERROR("Condition failed").put("Error", _status.ToString());                 \
        ctrl->set_status(_status);                                                      \
        return;                                                                         \
    }

#define GET_PATHNAME(uri, file)                                 \
    std::string file = GetFile(uri);                            \
    if (file.empty()) {                                         \
        LOG_ERROR("Uri format failed").put("Uri", uri);         \
        ctrl->set_status(Status::StoreInternal("Uri error")); \
        return;                                                 \
    }

void LocalFileStore::SetCondition(Controller* ctrl, const std::string& uri,
                                  const ConditionData& data, const SetConditionOptions& options) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/set_condition", "SetCondition", uri);

    GET_PATHNAME(uri, file);
    CONDITION_GUARD(options.condition);

    Status status = PrepareDir(byte::FilePath::GetFileDir(file));
    if (!status.ok()) {
        LOG_ERROR("Prepare dir failed").put("File", file).put("Error", status.ToString());
        ctrl->set_status(status);
        return;
    }

    int fd = open(file.c_str(), O_CREAT | O_RDWR | O_SYNC, 0644);
    if (fd < 0) {
        LOG_ERROR("Open file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }

    int res = ftruncate(fd, 0);
    if (res != 0) {
        LOG_ERROR("Ftruncate file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }

    ssize_t io_size = pwrite(fd, data.data(), data.size(), 0);
    if (io_size != static_cast<ssize_t>(data.size())) {
        LOG_ERROR("Pwrite file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }

    res = close(fd);
    if (res != 0) {
        LOG_WARNING("Close failed").put("File", file).put("Fd", fd).put("Error", strerror(errno));
    }

    ctrl->set_status(Status::OK());
}

void LocalFileStore::StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/stat_condition", "StatCondition", uri);
    GET_PATHNAME(uri, file);

    int fd = open(file.c_str(), O_RDONLY | O_SYNC, 0644);
    if (fd < 0) {
        if (errno == ENOENT) {
            ctrl->set_status(Status::StoreNotFound("File not found"));
        } else {
            LOG_ERROR("Open file failed").put("File", file).put("Error", strerror(errno));
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
        }
        return;
    }

    struct stat s;
    int res = stat(file.c_str(), &s);
    if (res != 0) {
        LOG_ERROR("Stat file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }
    if (static_cast<size_t>(s.st_size) != data->size()) {
        LOG_ERROR("File size mismatch").put("File", file).put("Size", s.st_size);
        ctrl->set_status(Status::StoreInternal("File size mismatch"));
        return;
    }

    ssize_t io_size = pread(fd, data->data(), s.st_size, 0);
    if (io_size != s.st_size) {
        LOG_ERROR("Pwrite file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }

    res = close(fd);
    if (res != 0) {
        LOG_WARNING("Close failed").put("File", uri).put("Fd", fd).put("Error", strerror(errno));
    }

    ctrl->set_status(Status::OK());
}

void LocalFileStore::List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/list", "List", path);

    GET_PATHNAME(path, dir);
    Status status = PrepareDir(dir);
    if (!status.ok()) {
        LOG_ERROR("Prepare dir failed").put("Dir", dir).put("Error", status.ToString());
        ctrl->set_status(status);
        return;
    }

    DIR* dentry = opendir(dir.c_str());
    if (dentry == nullptr) {
        LOG_ERROR("Open dir failed").put("Dir", dir).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }
    struct dirent* entry = nullptr;
    while ((entry = readdir(dentry)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        BlobInfo blob;
        blob.name = entry->d_name;
        files->push_back(blob);
    }

    closedir(dentry);
    ctrl->set_status(Status::OK());
}

void LocalFileStore::Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
                          Blob** blob) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/open", "Open", uri);

    GET_PATHNAME(uri, file);
    Status status = PrepareDir(byte::FilePath::GetFileDir(file));
    if (!status.ok()) {
        LOG_ERROR("Prepare dir failed").put("File", file).put("Error", status.ToString());
        ctrl->set_status(status);
        return;
    }

    int flags = 0;
    if (options.mode == OpenMode::kRead) {
        flags = O_RDONLY | O_SYNC;
    } else if (options.mode == OpenMode::kWrite) {
        flags = O_CREAT | O_RDWR | O_APPEND | O_SYNC;
    }

    CONDITION_GUARD(options.condition);
    int fd = open(file.c_str(), flags, 0644);
    if (fd < 0) {
        LOG_ERROR("Open file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }

    *blob = new File(file, fd, options.metrics_manager);
    ctrl->set_status(Status::OK());
}

void LocalFileStore::Delete(Controller* ctrl, const std::string& uri,
                            const DeleteOptions& options) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/delete", "Delete", uri);
    GET_PATHNAME(uri, file);

    CONDITION_GUARD(options.condition);
    int res = unlink(file.c_str());
    if (res != 0) {
        if (errno == ENOENT) {
            ctrl->set_status(Status::StoreNotFound("File not found"));
        } else {
            LOG_ERROR("Unlink file failed").put("File", file).put("Error", strerror(errno));
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
        }
        return;
    }
    ctrl->set_status(Status::OK());
}

void LocalFileStore::Freeze(Controller* ctrl, const std::string& uri,
                            const FreezeOptions& options) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/freeze", "Freeze", uri);
    GET_PATHNAME(uri, file);

    CONDITION_GUARD(options.condition);
    int res = chmod(file.c_str(), S_IRUSR | S_IRGRP | S_IROTH);
    if (res != 0) {
        LOG_ERROR("Chmod file failed").put("File", file).put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }
    ctrl->set_status(Status::OK());
}

void LocalFileStore::Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
                          BlobStat* stat_info) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/stat", "Stat", uri);
    GET_PATHNAME(uri, file);

    struct stat s;
    int res = stat(file.c_str(), &s);
    if (res != 0) {
        if (errno == ENOENT) {
            ctrl->set_status(Status::StoreNotFound("File not found"));
        } else {
            LOG_ERROR("Stat file failed").put("File", file).put("Error", strerror(errno));
            ctrl->set_status(Status::StoreInternal(strerror(errno)));
        }
        return;
    }
    stat_info->size = s.st_size;
    ctrl->set_status(Status::OK());
}

void LocalFileStore::Rename(Controller* ctrl, const std::string& src_uri,
                            const std::string& dst_uri, const RenameOptions& options) {
    FILE_STORE_FAULT_INJECT("store/file/ioctl/fail/rename", "Rename", dst_uri);
    GET_PATHNAME(src_uri, src_file);
    GET_PATHNAME(dst_uri, dst_file);

    CONDITION_GUARD(options.condition);
    int res = rename(src_file.c_str(), dst_file.c_str());
    if (res != 0) {
        LOG_ERROR("Rename file failed")
            .put("SrcFile", src_file)
            .put("DstFile", dst_file)
            .put("Error", strerror(errno));
        ctrl->set_status(Status::StoreInternal(strerror(errno)));
        return;
    }
    ctrl->set_status(Status::OK());
}

}  // namespace stream
}  // namespace bcache2
