#include "stream/store/unsupported_object_store.h"

#include <common/logging.h>

#include <string>

#include "common/controller.h"

namespace bcache2 {
namespace stream {

void UnsupportedObjectStore::SetUnsupported(Controller* ctrl, const std::string& api,
                                            const std::string& uri) const {
    std::string message = backend_name_ + " object-store backend is not linked: " + build_hint_;
    LOG_ERROR("Object store backend is not linked")
        .put("Backend", backend_name_)
        .put("Api", api)
        .put("Uri", uri)
        .put("Hint", build_hint_);
    ctrl->set_status(Status::Unimplemented(message));
}

void UnsupportedObjectStore::SetCondition(Controller* ctrl, const std::string& uri,
                                          const ConditionData& data,
                                          const SetConditionOptions& options) {
    SetUnsupported(ctrl, "SetCondition", uri);
}

void UnsupportedObjectStore::StatCondition(Controller* ctrl, const std::string& uri,
                                           ConditionData* data) {
    SetUnsupported(ctrl, "StatCondition", uri);
}

void UnsupportedObjectStore::List(Controller* ctrl, const std::string& path,
                                  std::vector<BlobInfo>* files) {
    SetUnsupported(ctrl, "List", path);
}

void UnsupportedObjectStore::Open(Controller* ctrl, const std::string& uri,
                                  const OpenOptions& options, Blob** blob) {
    if (blob != nullptr) {
        *blob = nullptr;
    }
    SetUnsupported(ctrl, "Open", uri);
}

void UnsupportedObjectStore::Delete(Controller* ctrl, const std::string& uri,
                                    const DeleteOptions& options) {
    SetUnsupported(ctrl, "Delete", uri);
}

void UnsupportedObjectStore::Freeze(Controller* ctrl, const std::string& uri,
                                    const FreezeOptions& options) {
    SetUnsupported(ctrl, "Freeze", uri);
}

void UnsupportedObjectStore::Stat(Controller* ctrl, const std::string& uri,
                                  const StatOptions& options, BlobStat* stat) {
    SetUnsupported(ctrl, "Stat", uri);
}

void UnsupportedObjectStore::Rename(Controller* ctrl, const std::string& src_uri,
                                    const std::string& dst_uri,
                                    const RenameOptions& options) {
    SetUnsupported(ctrl, "Rename", src_uri + " -> " + dst_uri);
}

}  // namespace stream
}  // namespace bcache2
