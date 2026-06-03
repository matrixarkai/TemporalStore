#pragma once

#include <string>
#include <utility>
#include <vector>

#include "stream/store/store.h"

namespace bcache2 {
namespace stream {

class UnsupportedObjectStore : public Store {
 public:
    UnsupportedObjectStore(std::string backend_name, std::string build_hint)
        : backend_name_(std::move(backend_name)), build_hint_(std::move(build_hint)) {}
    ~UnsupportedObjectStore() override = default;

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

 private:
    void SetUnsupported(Controller* ctrl, const std::string& api, const std::string& uri) const;

    std::string backend_name_;
    std::string build_hint_;
};

}  // namespace stream
}  // namespace bcache2
