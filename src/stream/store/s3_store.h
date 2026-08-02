#pragma once

#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <vector>

#include "stream/store/store.h"

namespace bcache2 {
namespace stream {

class S3Store : public Store {
 public:
    explicit S3Store(std::string backend_name);
    ~S3Store() override = default;

    void SetCondition(Controller* ctrl, const std::string& uri, const ConditionData& data,
                      const SetConditionOptions& options) override;

    void StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) override;

    void List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) override;

    void Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
              Blob** blob) override;

    void Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) override;

    void Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) override;

    void Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
              BlobStat* stat) override;

    void Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                const RenameOptions& options) override;

    Status PutObject(const std::string& uri, const std::string& body);
    Status GetObject(const std::string& uri, std::string* body);
    Status GetObjectRange(const std::string& uri, size_t offset, size_t size, std::string* body);

 private:
    Status CheckCondition(const Condition& condition);
    Status HeadObject(const std::string& uri, BlobStat* stat);
    Status DeleteObject(const std::string& uri);
    Status CopyObject(const std::string& src_uri, const std::string& dst_uri);

    std::string backend_name_;

    DISALLOW_COPY_AND_ASSIGN(S3Store);
};

}  // namespace stream
}  // namespace bcache2
