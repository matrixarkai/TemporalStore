// Compatibility objects for prebuilt BRPC archives that export thrift headers
// but were linked without the thrift service implementation.

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <thrift/TProcessor.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "brpc/details/method_status.h"
#include "brpc/log.h"
#include "brpc/policy/thrift_protocol.h"
#include "brpc/protocol.h"
#include "brpc/thrift_message.h"
#include "brpc/thrift_service.h"
#include "butil/class_name.h"

namespace brpc {

ThriftService::ThriftService() : _status(new MethodStatus) {}

ThriftService::~ThriftService() {
    delete _status;
    _status = nullptr;
}

void ThriftService::Describe(std::ostream& os, const DescribeOptions&) const {
    os << butil::class_name_str(*this);
}

void ThriftService::Expose(const butil::StringPiece& prefix) {
    if (_status == nullptr) {
        return;
    }
    std::string name;
    const std::string& class_name = butil::class_name_str(*this);
    name.reserve(prefix.size() + 1 + class_name.size());
    name.append(prefix.data(), prefix.size());
    name.push_back('_');
    name.append(class_name);
    _status->Expose(name);
}

namespace policy {

#ifndef BCACHE2_PROXY_HAS_BRPC_THRIFT_PROTOCOL_SOURCE
bool ReadThriftStruct(const butil::IOBuf& body, ThriftMessageBase* raw_msg,
                      int16_t expected_fid) {
    if (raw_msg == nullptr) {
        return false;
    }

    const size_t body_len = body.size();
    std::vector<uint8_t> thrift_buffer(body_len);
    if (body_len > 0) {
        body.copy_to(thrift_buffer.data(), body_len);
    }

    auto in_buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
        thrift_buffer.data(), static_cast<uint32_t>(body_len),
        ::apache::thrift::transport::TMemoryBuffer::OBSERVE);
    apache::thrift::protocol::TBinaryProtocolT<apache::thrift::transport::TMemoryBuffer> iprot(
        in_buffer);

    try {
        std::string fname;
        uint32_t xfer = 0;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid = 0;
        xfer += iprot.readStructBegin(fname);
        while (true) {
            xfer += iprot.readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            if (fid == expected_fid && ftype == ::apache::thrift::protocol::T_STRUCT) {
                xfer += raw_msg->Read(&iprot);
                xfer += iprot.readFieldEnd();
                xfer += iprot.readStructEnd();
                (void)xfer;
                iprot.getTransport()->readEnd();
                return true;
            }
            xfer += iprot.skip(ftype);
            xfer += iprot.readFieldEnd();
        }
        xfer += iprot.readStructEnd();
        (void)xfer;
        iprot.getTransport()->readEnd();
    } catch (const std::exception& e) {
        LOG(WARNING) << "caught thrift exception while reading framed body: " << e.what();
    } catch (...) {
        LOG(WARNING) << "caught unknown thrift exception while reading framed body";
    }
    return false;
}
#endif

}  // namespace policy
}  // namespace brpc

namespace bcache2 {
namespace proxy {

bool EnsureBrpcThriftProtocolRegistered() {
    if (brpc::FindProtocol(brpc::PROTOCOL_THRIFT) != nullptr) {
        return true;
    }
#ifdef BCACHE2_PROXY_HAS_BRPC_THRIFT_PROTOCOL_SOURCE
    brpc::Protocol thrift_binary_protocol = {
        brpc::policy::ParseThriftMessage,
        brpc::policy::SerializeThriftRequest,
        brpc::policy::PackThriftRequest,
        brpc::policy::ProcessThriftRequest,
        brpc::policy::ProcessThriftResponse,
        brpc::policy::VerifyThriftRequest,
        nullptr,
        nullptr,
        brpc::CONNECTION_TYPE_POOLED_AND_SHORT,
        "thrift"};
    return brpc::RegisterProtocol(brpc::PROTOCOL_THRIFT, thrift_binary_protocol) == 0 ||
           brpc::FindProtocol(brpc::PROTOCOL_THRIFT) != nullptr;
#else
    return false;
#endif
}

}  // namespace proxy
}  // namespace bcache2
