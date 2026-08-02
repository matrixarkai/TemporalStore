// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <arpa/inet.h>
#include <protocol/info.pb.h>

#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "common/macros.h"
#include "stream/stream.h"

namespace bcache2 {
namespace partition {

enum ConditionVersion : uint8_t {
    kEmpty = 0x00,
    kV1 = 0x1f,
};

enum AddressFamily : uint8_t {
    kIpv4,
    kIpv6,
};

class ConditionInfoBase {
 public:
    ConditionInfoBase() = default;
    virtual ~ConditionInfoBase() {}

    virtual uint8_t RemoteFamily() const = 0;
    virtual uint8_t Version() const = 0;
    virtual uint64_t PartitionId() const = 0;
    virtual uint32_t LoadVersion() const = 0;
    virtual std::string RemoteIpStr() const = 0;
    virtual uint16_t RemotePort() const = 0;

    virtual ConditionInfo TransformProtocol() const {
        ConditionInfo info;
        info.set_version(Version());
        info.set_partition_id(PartitionId());
        info.set_load_version(LoadVersion());
        info.set_remote_ip(RemoteIpStr());
        info.set_remote_port(RemotePort());
        return info;
    }
    virtual std::string ToString() const { return TransformProtocol().ShortDebugString(); }
};

class ConditionInfoObsolete : public ConditionInfoBase {
 public:
    struct Structure {
        uint64_t partition_id = 0;
        uint64_t load_version = 0;
        uint64_t last_load_version = 0;
        uint64_t reserverd = 0;
    };
    static_assert(sizeof(Structure) == stream::Env::kInlineBlobSize);

    explicit ConditionInfoObsolete(const char* buf)
        : structure_(reinterpret_cast<const Structure*>(buf)) {}
    ~ConditionInfoObsolete() {}

    uint8_t RemoteFamily() const override { return 0; }
    uint8_t Version() const override { return 0xff; }
    uint64_t PartitionId() const override { return structure_->partition_id; }
    uint32_t LoadVersion() const override { return structure_->load_version; }
    std::string RemoteIpStr() const override { return ""; }
    uint16_t RemotePort() const override { return 0; }

 private:
    const Structure* structure_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ConditionInfoObsolete);
};

class ConditionInfoEmpty : public ConditionInfoBase {
 public:
    uint8_t RemoteFamily() const override { return 0; }
    uint8_t Version() const override { return 0; }
    uint64_t PartitionId() const override { return 0; }
    uint32_t LoadVersion() const override { return 0; }
    std::string RemoteIpStr() const override { return ""; }
    uint16_t RemotePort() const override { return 0; }
};

class ConditionInfoV1 : public ConditionInfoBase {
 public:
    struct Structure {
        uint8_t version = ConditionVersion::kV1;
        uint64_t partition_id = 0;
        uint32_t load_version = 0;
        uint8_t remote_family = AddressFamily::kIpv4;
        char remote_ip[16] = {};
        uint16_t remote_port = 0;
    } __attribute__((__packed__));
    static_assert(sizeof(Structure) == stream::Env::kInlineBlobSize);

    explicit ConditionInfoV1(const char* buf)
        : structure_(reinterpret_cast<const Structure*>(buf)) {}
    ~ConditionInfoV1() {}

    uint8_t RemoteFamily() const override { return structure_->remote_family; }
    uint8_t Version() const override { return structure_->version; }
    uint64_t PartitionId() const override { return structure_->partition_id; }
    uint32_t LoadVersion() const override { return structure_->load_version; }
    std::string RemoteIpStr() const override {
        char buf[INET6_ADDRSTRLEN];
        if (structure_->remote_family == AddressFamily::kIpv4) {
            inet_ntop(AF_INET, structure_->remote_ip, buf, sizeof(buf));
        } else {
            inet_ntop(AF_INET6, structure_->remote_ip, buf, sizeof(buf));
        }
        return buf;
    }
    uint16_t RemotePort() const override { return structure_->remote_port; }

 private:
    const Structure* structure_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ConditionInfoV1);
};

// normally, ConditionInfoObserver is a local variable
class ConditionInfoObserver {
 public:
    explicit ConditionInfoObserver(const char* buf) {
        if (buf[0] == 0x00 && memcmp(buf, buf + 1, stream::Env::kInlineBlobSize - 1) == 0) {
            condition_info_.reset(new ConditionInfoEmpty());
            return;
        }

        uint8_t version = *reinterpret_cast<const uint8_t*>(buf);
        switch (version) {
        case ConditionVersion::kV1:
            condition_info_.reset(new ConditionInfoV1(buf));
            break;
        default:
            BYTE_ASSERT(false) << "invalid condition version " << static_cast<int>(version);
        }
    }
    ~ConditionInfoObserver() {}

    uint8_t Version() const { return condition_info_->Version(); }
    uint64_t PartitionId() const { return condition_info_->PartitionId(); }
    uint32_t LoadVersion() const { return condition_info_->LoadVersion(); }
    std::string RemoteIpStr() const { return condition_info_->RemoteIpStr(); }
    uint16_t RemotePort() const { return condition_info_->RemotePort(); }
    ConditionInfo TransformProtocol() const { return condition_info_->TransformProtocol(); }
    std::string ToString() const { return condition_info_->ToString(); }

 private:
    std::unique_ptr<ConditionInfoBase> condition_info_;

    DISALLOW_COPY_AND_ASSIGN(ConditionInfoObserver);
};

inline std::ostream& operator<<(std::ostream& os, const ConditionInfoObserver& condition_os) {
    return os << condition_os.ToString();
}

}  // namespace partition
}  // namespace bcache2
