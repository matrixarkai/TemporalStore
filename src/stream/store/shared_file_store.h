#pragma once

#include <byte/base/closure.h>
#include <byte/include/assert.h>
#include <byte/thread/async_thread.h>
#include <common/logging.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "common/controller.h"
#include "stream/store/store.h"

namespace bcache2 {
namespace stream {

class SharedFileCondition {
 public:
    explicit SharedFileCondition(const Store::Condition& condition);
    virtual ~SharedFileCondition() {}

    Status Check();

 private:
    std::string uri_;
    Env::ConditionData data_;

    DISALLOW_COPY_AND_ASSIGN(SharedFileCondition);
};

class SharedFile : public Blob {
 public:
    SharedFile(const std::string& pathname, int fd, MetricsManager* metrics_manager)
        : pathname_(pathname), fd_(fd) {
        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "shared_file_store";
        BYTE_ASSERT(bg_thread_pool_.Init(tp_options));
        BYTE_ASSERT(bg_thread_pool_.Start());
        metrics_.Init(metrics_manager, pathname);
    }

    void Close() override;
    void Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) override;
    void Read(Controller* ctrl, size_t offset, void* data, size_t size,
              Closure<void>* callback) override;

 private:
    byte::AsyncThreadPool bg_thread_pool_;
    std::string pathname_;
    int fd_ = 0;
    size_t length_ = 0;
    BlobMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(SharedFile);
};

class SharedFileStore : public Store {
 public:
    SharedFileStore() {}
    virtual ~SharedFileStore() {}

    void SetCondition(Controller* ctrl, const std::string& uri, const ConditionData& data,
                      const SetConditionOptions& options) override;

    void StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) override;

    void List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) override;

    void Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
              Blob** blob) override;

    void Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) override;

    void Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) override;

    void Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
              BlobStat* stat_info) override;

    void Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                const RenameOptions& options) override;

 private:
    DISALLOW_COPY_AND_ASSIGN(SharedFileStore);
};

}  // namespace stream
}  // namespace bcache2
