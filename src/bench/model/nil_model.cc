// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/model/nil_model.h"

#include "bench/model/hash_model.h"
#include "bench/model/string_model.h"
#include "bench/model/ips_model.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"

namespace bcache2 {
namespace bench {

Status NilModel::Apply(const ApplyOptions& opts, const Operation& op,
                       std::vector<std::unique_ptr<Model>>* next_states) const {
    switch (op.module_id()) {
    case Module::COMMON: {
        switch (op.code()) {
        case kOK: {
            return Status::Internal("Model is nil but request success");
        }

        case kNotFound: {
            next_states->clear();
            next_states->emplace_back(std::unique_ptr<Model>(new NilModel()));
            return Status::OK();
        }

        default: {
            // request failure
            next_states->clear();
            next_states->emplace_back(std::unique_ptr<Model>(new NilModel()));
            return Status::OK();
        }
        }
    }
    case Module::STRING: {
        return StringModel().Apply(opts, op, next_states);
    }
    case Module::HASH: {
        return HashModel().Apply(opts, op, next_states);
    }
    case Module::IPS: {
        return IpsModel().Apply(opts, op, next_states);
    }
    }
    return Status::FailedPrecondition("Module not support");
}

}  // namespace bench
}  // namespace bcache2
