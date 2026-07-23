// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>

#include "model/feature_model.h"
#include "model/hash_model.h"

namespace bcache2 {
namespace model {

class ContextNodeOrSet : public HashOrSet<std::string, std::string> {
 public:
    explicit ContextNodeOrSet(PersistentMap<std::string, std::string>* data)
            : HashOrSet<std::string, std::string>(data) {}
};

class ContextEventOrSet : public FeatureOrSet {
 public:
    explicit ContextEventOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextIndexOrSet : public FeatureOrSet {
 public:
    explicit ContextIndexOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextAuditOrSet : public FeatureOrSet {
 public:
    explicit ContextAuditOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextDirtyOrSet : public FeatureOrSet {
 public:
    explicit ContextDirtyOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextChildOrSet : public FeatureOrSet {
 public:
    explicit ContextChildOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextEmbeddingOrSet : public HashOrSet<std::string, std::string> {
 public:
    explicit ContextEmbeddingOrSet(PersistentMap<std::string, std::string>* data)
            : HashOrSet<std::string, std::string>(data) {}
};

class ContextEntityOrSet : public HashOrSet<std::string, std::string> {
 public:
    explicit ContextEntityOrSet(PersistentMap<std::string, std::string>* data)
            : HashOrSet<std::string, std::string>(data) {}
};

class ContextSummaryOrSet : public FeatureOrSet {
 public:
    explicit ContextSummaryOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

class ContextCompressionOrSet : public FeatureOrSet {
 public:
    explicit ContextCompressionOrSet(PersistentMap<uint64_t, std::string>* data) : FeatureOrSet(data) {}
};

using ContextNodeModel =
        OrSetModel<std::string, std::string, ContextNodeOrSet>;
using ContextEventModel =
        OrSetModel<uint64_t, std::string, ContextEventOrSet>;
using ContextIndexModel =
        OrSetModel<uint64_t, std::string, ContextIndexOrSet>;
using ContextAuditModel =
        OrSetModel<uint64_t, std::string, ContextAuditOrSet>;
using ContextDirtyModel =
        OrSetModel<uint64_t, std::string, ContextDirtyOrSet>;
using ContextChildModel =
        OrSetModel<uint64_t, std::string, ContextChildOrSet>;
using ContextEmbeddingModel =
        OrSetModel<std::string, std::string, ContextEmbeddingOrSet>;
using ContextEntityModel =
        OrSetModel<std::string, std::string, ContextEntityOrSet>;
using ContextSummaryModel =
        OrSetModel<uint64_t, std::string, ContextSummaryOrSet>;
using ContextCompressionModel =
        OrSetModel<uint64_t, std::string, ContextCompressionOrSet>;

}  // namespace model
}  // namespace bcache2
