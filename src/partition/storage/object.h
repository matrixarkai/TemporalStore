// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>

#include "common/align.h"
#include "common/macros.h"
#include "model/model_manager.h"

namespace bcache2 {
namespace partition {

// ┌────────────┬──────────────────┬─────────┬─────────────┐
// │ flags (1B) │   key len (4B)   │  rawkay │    model    │
// └─────┬──────┴──────────────────┴─────────┴─────────────┘
//       │     ┌────────────────┬───────────────┐
//       └────>│  trivial (1b)  │ model id (7b) │
//             └────────────────┴───────────────┘
// TODO(wangtai.10): varint and align optimize
class Object {
 public:
    using KeyLenType = uint32_t;
    using FlagsType = uint8_t;
    using ModelIdType = uint8_t;

    Object() {}
    Object(uint8_t object_id, uint8_t* raw_object_buf)
        : object_id_(object_id), raw_object_buf_(raw_object_buf) {}
    ~Object() = default;

    explicit operator bool() const { return raw_object_buf_ != nullptr; }

    static void Construct(uint8_t* raw_object_buf);
    static void ConstructWithValues(uint8_t* raw_object_buf, ModelIdType model_id,
                                    absl::string_view key);
    static void ConstructFrom(uint8_t* raw_object_buf, Object robj);
    static void Destroy(uint8_t* raw_object_buf);

    KeyLenType KeyLen() const { return KeyLenRef(); }
    absl::string_view Key() const {
        return absl::string_view(reinterpret_cast<char*>(raw_object_buf_ + KeyOffset()), KeyLen());
    }
    ModelIdType ModelId() const { return FlagsRef() & kModelIdsMask; }
    uint8_t ObjectId() const { return object_id_; }
    bool Trivial() const { return FlagsRef() & kTrivialMask; }
    size_t RawSize() const { return ComputeRawObjectSize(KeyLen(), ModelId()); }
    uint8_t* RawBuf() const { return raw_object_buf_; }

    template <typename ModelType>
    ModelType* Model() const {
        BYTE_ASSERT(ModelId() == model::ModelManager::GetModelId<ModelType>())
            << "model_id is " << static_cast<int>(ModelId()) << ", need "
            << static_cast<int>(model::ModelManager::GetModelId<ModelType>());
        return reinterpret_cast<ModelType*>(raw_object_buf_ + ModelOffset());
    }
    uint8_t* RawModelBuf() const { return raw_object_buf_ + ModelOffset(); }

    static size_t ComputeRawObjectSize(KeyLenType key_len, ModelIdType model_id) {
        uint8_t* ptr = 0x0;

        // flags
        ptr = Align(alignof(FlagsType), ptr) + sizeof(FlagsType);

        // key len
        ptr = Align(alignof(KeyLenType), ptr) + sizeof(KeyLenType);

        // key
        ptr = Align(alignof(char), ptr) + key_len;

        // model
        ptr = Align(model::ModelManager::GetModelAlignment(model_id), ptr) +
              model::ModelManager::GetModelSize(model_id);

        // we don't known object start address, so reserve alignof(std::max_align_t)
        // to force align alignof(std::max_align_t) bytes
        return reinterpret_cast<uintptr_t>(ptr) + alignof(std::max_align_t);
    }

 private:
    size_t FlagsOffset() const {
        return Align(alignof(std::max_align_t), raw_object_buf_) - raw_object_buf_;
    }
    size_t KeyLenOffset() const {
        return Align(alignof(KeyLenType), raw_object_buf_ + FlagsOffset() + sizeof(FlagsType)) -
               raw_object_buf_;
    }
    size_t KeyOffset() const {
        return Align(alignof(char), raw_object_buf_ + KeyLenOffset() + sizeof(KeyLenType)) -
               raw_object_buf_;
    }
    size_t ModelOffset() const {
        return Align(model::ModelManager::GetModelAlignment(ModelId()),
                     raw_object_buf_ + KeyOffset() + KeyLen()) -
               raw_object_buf_;
    }
    FlagsType& FlagsRef() const {
        return *reinterpret_cast<FlagsType*>(raw_object_buf_ + FlagsOffset());
    }
    KeyLenType& KeyLenRef() const {
        KeyLenType* ptr = reinterpret_cast<KeyLenType*>(raw_object_buf_ + KeyLenOffset());
        return *ptr;
    }

    uint8_t object_id_ = 0;
    uint8_t* raw_object_buf_ = nullptr;

    static constexpr uint8_t kModelIdsMask = 0x7f;  // 01111111
    static constexpr uint8_t kTrivialMask = 0x80;   // 10000000

    ALLOW_COPY_AND_ASSIGN(Object);
};

inline void Object::Construct(uint8_t* raw_object_buf) {
    // set trivial to 1
    Object obj(0, raw_object_buf);
    obj.FlagsRef() = 1 << 7;
}

inline void Object::ConstructWithValues(uint8_t* raw_object_buf, ModelIdType model_id,
                                        absl::string_view key) {
    BYTE_ASSERT(key.size() <= std::numeric_limits<KeyLenType>::max()) << "key too large";
    BYTE_ASSERT(model_id <= kModelIdsMask) << "model id too large";

    // flags, key len, key
    Object obj(0, raw_object_buf);
    obj.FlagsRef() = model_id;  // non trivial
    obj.KeyLenRef() = key.size();
    memcpy(obj.raw_object_buf_ + obj.KeyOffset(), key.data(), key.size());

    // model
    model::ModelManager::Construct(model_id, obj.RawModelBuf());

    BYTE_ASSERT(!obj.Trivial());
}

inline void Object::ConstructFrom(uint8_t* raw_object_buf, Object robj) {
    BYTE_ASSERT(!robj.Trivial()) << "construct from a trivial object";

    Object obj(0, raw_object_buf);

    // flags, key len, key
    obj.FlagsRef() = robj.FlagsRef();
    obj.KeyLenRef() = robj.KeyLenRef();
    memcpy(obj.raw_object_buf_ + obj.KeyOffset(), robj.raw_object_buf_ + robj.KeyOffset(),
           robj.KeyLen());

    // model
    model::ModelManager::MoveConstruct(obj.ModelId(), obj.raw_object_buf_ + obj.ModelOffset(),
                                       robj.raw_object_buf_ + robj.ModelOffset());
}

inline void Object::Destroy(uint8_t* raw_object_buf) {
    Object obj(0, raw_object_buf);
    model::ModelManager::Destruct(obj.ModelId(), obj.RawModelBuf());
    // set trivial to 1
    obj.FlagsRef() = 1 << 7;
}

}  // namespace partition
}  // namespace bcache2
