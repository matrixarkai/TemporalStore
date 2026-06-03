// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>

#include "common/macros.h"
#include "common/slice.h"
#include "protocol/base.pb.h"

namespace bcache2 {

enum Code {
#define DEFINE_CODE(code_name, code_value) k##code_name = code_value,

#include "common/code.inc"
#undef DEFINE_CODE
};

class Status {
 public:
    // Create a success status.
    Status() : state_(nullptr) {}
    ~Status() { delete[] state_; }

    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
    Status& operator=(Status&& rhs) noexcept;

    Status(Code code, const Slice& msg);

    // Return a success status.
    static Status OK() { return Status(); }

    // Return error status of an appropriate type.

#define DEFINE_CODE(code_name, code_value)                                          \
    static Status code_name(const Slice& msg) { return Status(k##code_name, msg); } \
    bool Is##code_name() const { return code() == k##code_name; }

#include "common/code.inc"  // NOLINT
#undef DEFINE_CODE

    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == nullptr); }

    int errorcode() const { return static_cast<int>(code()); }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const;
    RpcStatus ToRpcStatus() const;
    static Status FromRpcStatus(RpcStatus rpc_status) {
        if (rpc_status.code() == Code::kOK) {
            return Status::OK();
        }
        return Status(static_cast<Code>(rpc_status.code()), rpc_status.message());
    }

 private:
    using LengthType = uint32_t;

    Code code() const { return (state_ == nullptr) ? Code::kOK : static_cast<Code>(state_[4]); }

    static const char* CopyState(const char* s);

    static LengthType Length(const char* state) {
        if (state == nullptr) {
            return 0;
        }
        return *reinterpret_cast<const LengthType*>(state + kLengthOffset);
    }

    static bool SetLength(char* state, LengthType size) {
        if (state == nullptr) {
            return false;
        }
        *reinterpret_cast<LengthType*>(state + kLengthOffset) = size;
        return true;
    }

    static const char* Data(const char* state) {
        if (state == nullptr) {
            return nullptr;
        }
        return state + kDataOffset;
    }

    // OK status has a null state_.  Otherwise, state_ is a new[] array
    // of the following form:
    //    state_[0..3] == length of message
    //    state_[4]    == code
    //    state_[5..]  == message
    static const uint32_t kLengthOffset = 0;
    static const uint32_t kCodeOffset = kLengthOffset + sizeof(LengthType);
    static const uint32_t kDataOffset = kCodeOffset + 1;
    const char* state_ = nullptr;
};

inline Status::Status(const Status& rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}
inline Status& Status::operator=(const Status& rhs) {
    // The following condition catches both aliasing (when this == &rhs),
    // and the common case where both rhs and *this are ok.
    if (state_ != rhs.state_) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}
inline Status& Status::operator=(Status&& rhs) noexcept {
    std::swap(state_, rhs.state_);
    return *this;
}

inline const char* Status::CopyState(const char* state) {
    char* result = new char[Length(state) + kDataOffset];
    std::memcpy(result, state, Length(state) + kDataOffset);
    return result;
}

inline Status::Status(Code code, const Slice& msg) {
    if (code == Code::kOK) {
        return;
    }

    const LengthType size = static_cast<LengthType>(msg.size());
    char* result = new char[size + kDataOffset];
    *reinterpret_cast<LengthType*>(result) = size;
    result[kCodeOffset] = static_cast<char>(code);
    std::memcpy(result + kDataOffset, msg.data(), size);
    state_ = result;
}

inline std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    } else {
        char tmp[30];
        const char* type;
        switch (code()) {
#define DEFINE_CODE(code_name, code_value) \
    case k##code_name:                     \
        type = #code_name;                 \
        break;

#include "common/code.inc"  // NOLINT
#undef DEFINE_CODE
        default:
            std::snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
            type = tmp;
            break;
        }

        std::string result(type);
        result.append(": ");
        result.append(Data(state_), Length(state_));
        return result;
    }
}

inline std::ostream& operator<<(std::ostream& os, const Status& status) {
    return os << status.ToString();
}

inline RpcStatus Status::ToRpcStatus() const {
    RpcStatus rpc_status;
    if (state_ == nullptr) {
        return rpc_status;
    }
    rpc_status.set_code(code());
    rpc_status.mutable_message()->append(Data(state_), Length(state_));
    return rpc_status;
}

#define RETURN_IF_STATUS_ERROR(stmt) \
    do {                             \
        const Status& __s = (stmt);  \
        if (UNLIKELY(!__s.ok())) {   \
            return __s;              \
        }                            \
    } while (false)

}  // namespace bcache2
