// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "model/ips_model.h"

#include <utility>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "model/ips/ips_interface.h"
#include "model/orset_model.h"
#include "extension/ips/interface.pb.h"

namespace bcache2 {
namespace model {
IpsOrSet::IpsOrSet(PersistentMap<std::string, std::string>* data) {
    data_ = data;
    user_meta_str = ips::IPSInterface::GenEmptyTreeMeta();
}
}  // namespace model
}  // namespace bcache2
