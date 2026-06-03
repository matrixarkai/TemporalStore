// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "proxy/service.h"

#include <memory>
#include <vector>

#include "client/client_impl.h"
#include "common/logging.h"
#include "proxy/thrift_utils.h"
#include "thrift/TApplicationException.h"
#include "thrift/TProcessor.h"
#include "thrift/Thrift.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#define PARSE_AND_CONTINUE(RequestType, method)                 \
    if (method_name == method) {                                \
        RequestType request;                                    \
        *body_len += static_cast<size_t>(request.read(&iprot)); \
        LOG_DEBUG("Parse request")                              \
            .put("MethodName", method_name)                     \
            .put("BodyLen", *body_len)                          \
            .put("req", request);                               \
        *body_len += iprot.readFieldEnd();                      \
        continue;                                               \
    }

#define REGISTER_HANDLER(method_name, request_type, response_type)                             \
    do {                                                                                       \
        if (ctrl->thrift_method_name() == method_name) {                                       \
            return CommonHandler(ctrl, *req->Cast<request_type>(), res->Cast<response_type>(), \
                                 done_guard.release());                                        \
        }                                                                                      \
    } while (false)

namespace bcache2 {
namespace proxy {

bool Bcache2ThriftService::ParseBaseThrift(butil::IOBuf* source, const std::string& method_name,
                                           uint32_t* body_len) {
    // parse buffered thrift msg
    *body_len = 0;
    std::unique_ptr<uint8_t[]> buf(new uint8_t[source->size()]);
    source->copy_to(buf.get(), source->size());

    // skip header
    size_t shift = 12 + method_name.size();
    uint8_t* ob_buf = buf.get() + shift;
    auto in_buffer = apache::thrift::stdcxx::make_shared<apache::thrift::transport::TMemoryBuffer>(
        ob_buf, source->size() - shift, ::apache::thrift::transport::TMemoryBuffer::OBSERVE);
    apache::thrift::protocol::TBinaryProtocolT<apache::thrift::transport::TMemoryBuffer> iprot(
        in_buffer);
    try {
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        while (true) {
            *body_len += iprot.readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            PARSE_AND_CONTINUE(thrift::GetRequest, "Get");
            PARSE_AND_CONTINUE(thrift::SetRequest, "Set");
            PARSE_AND_CONTINUE(thrift::RiskHsetRequest, "RiskHset");
            PARSE_AND_CONTINUE(thrift::RiskHqueryRequest, "RiskHquery");
            PARSE_AND_CONTINUE(thrift::RiskFolSetRequest, "RiskFolSet");
            PARSE_AND_CONTINUE(thrift::RiskCPCQueryRequest, "RiskCPCQuery");
            PARSE_AND_CONTINUE(thrift::RiskCPCSetRequest, "RiskCPCSet");
            PARSE_AND_CONTINUE(thrift::RiskFolQueryRequest, "RiskFolQuery");
            PARSE_AND_CONTINUE(thrift::RiskManagerRequest, "RiskManager");
            PARSE_AND_CONTINUE(thrift::HMGetRequest, "HMGet");
            PARSE_AND_CONTINUE(thrift::HMSetRequest, "HMSet");
            PARSE_AND_CONTINUE(thrift::HGetAllRequest, "HGetAll");
            PARSE_AND_CONTINUE(thrift::FeatureAddRequest, "FeatureAdd");
            PARSE_AND_CONTINUE(thrift::FeatureQueryRequest, "FeatureQuery");
            LOG_WARNING("Unknown method")
                .put("method_name", method_name)
                .put("length", source->length());
            return false;
        }
        return true;
    } catch (apache::thrift::transport::TTransportException& e) {
        LOG_DEBUG("Fail to parse request").put("Method", method_name).put("What", e.what());
        throw e;
    } catch (apache::thrift::TException& e) {
        LOG_DEBUG("Fail to parse request").put("Method", method_name).put("What", e.what());
        throw apache::thrift::transport::TTransportException(e.what());
    }
}

void Bcache2ThriftService::ProcessThriftFramedRequest(brpc::Controller* ctrl,
                                                      brpc::ThriftFramedMessage* req,
                                                      brpc::ThriftFramedMessage* res,
                                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    try {
        // Dispatch calls to different methods
        REGISTER_HANDLER("Get", thrift::GetRequest, thrift::GetResponse);
        REGISTER_HANDLER("Set", thrift::SetRequest, thrift::SetResponse);
        REGISTER_HANDLER("FeatureAdd", thrift::FeatureAddRequest, thrift::FeatureAddResponse);
        REGISTER_HANDLER("FeatureQuery", thrift::FeatureQueryRequest, thrift::FeatureQueryResponse);
        REGISTER_HANDLER("RiskHset", thrift::RiskHsetRequest, thrift::RiskCommonResponse);
        REGISTER_HANDLER("RiskHquery", thrift::RiskHqueryRequest, thrift::RiskHqueryResponse);
        REGISTER_HANDLER("RiskFolSet", thrift::RiskFolSetRequest, thrift::RiskCommonResponse);
        REGISTER_HANDLER("RiskFolQuery", thrift::RiskFolQueryRequest, thrift::RiskFolQueryResponse);
        REGISTER_HANDLER("RiskCPCSet", thrift::RiskCPCSetRequest, thrift::RiskCommonResponse);
        REGISTER_HANDLER("RiskCPCQuery", thrift::RiskCPCQueryRequest, thrift::RiskCPCQueryResponse);
        REGISTER_HANDLER("RiskManager", thrift::RiskManagerRequest, thrift::RiskManagerResponse);
        REGISTER_HANDLER("HMGet", thrift::HMGetRequest, thrift::HMGetResponse);
        REGISTER_HANDLER("HMSet", thrift::HMSetRequest, thrift::HMSetResponse);
        REGISTER_HANDLER("HGetAll", thrift::HGetAllRequest, thrift::HGetAllResponse);
        REGISTER_HANDLER("HLen", thrift::HLenRequest, thrift::HLenResponse);
    } catch (apache::thrift::TException& e) {
        LOG_WARNING("Fail to parse request")
            .put("Remote", ctrl->remote_side())
            .put("method", ctrl->thrift_method_name())
            .put("What", e.what());
        ctrl->SetFailed(brpc::EREQUEST, "Fail to parse request, %s", e.what());
        return;
    }

    LOG_WARNING("Invalid method")
        .put("Remote", ctrl->remote_side())
        .put("method", ctrl->thrift_method_name());
    ctrl->SetFailed(brpc::ENOMETHOD, "Invalid method=%s", ctrl->thrift_method_name().c_str());
}

}  // namespace proxy
}  // namespace bcache2
