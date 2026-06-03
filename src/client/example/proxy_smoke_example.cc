#include <brpc/channel.h>
#include <brpc/thrift_message.h>

#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "thrift/server_types.h"

namespace {

bool CheckStatus(const char* op, const bcache2::thrift::Status& status) {
    if (status.code != bcache2::kOK) {
        std::cerr << op << " failed: code=" << status.code
                  << " message=" << status.message << std::endl;
        return false;
    }
    return true;
}

bool CheckController(const char* op, const brpc::Controller& ctrl) {
    if (ctrl.Failed()) {
        std::cerr << op << " rpc failed: " << ctrl.ErrorText() << std::endl;
        return false;
    }
    return true;
}

template <typename Request, typename Response>
bool Call(brpc::Channel* channel, const char* method, const Request& request,
          Response* response) {
    brpc::ThriftStub stub(channel);
    brpc::Controller ctrl;
    ctrl.set_timeout_ms(5000);
    stub.CallMethod(method, &ctrl, &request, response, nullptr);
    return CheckController(method, ctrl) && CheckStatus(method, response->status);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: " << argv[0]
                  << " <proxy_host:port> <namespace> <table> <key_prefix>" << std::endl;
        return 2;
    }

    const std::string proxy_endpoint = argv[1];
    const std::string namespace_name = argv[2];
    const std::string table_name = argv[3];
    const std::string prefix = std::string(argv[4]) + "_" + std::to_string(std::time(nullptr)) + "_" +
                               std::to_string(static_cast<unsigned long long>(std::rand()));

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_THRIFT;
    options.timeout_ms = 5000;

    brpc::Channel channel;
    if (channel.Init(proxy_endpoint.c_str(), "", &options) != 0) {
        std::cerr << "failed to init proxy channel: " << proxy_endpoint << std::endl;
        return 1;
    }

    {
        bcache2::thrift::SetRequest request;
        bcache2::thrift::SetResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":string");
        request.__set_value("proxy-value");
        if (!Call(&channel, "Set", request, &response)) {
            return 1;
        }
    }

    {
        bcache2::thrift::GetRequest request;
        bcache2::thrift::GetResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":string");
        if (!Call(&channel, "Get", request, &response)) {
            return 1;
        }
        if (response.value != "proxy-value") {
            std::cerr << "Get returned wrong value: " << response.value << std::endl;
            return 1;
        }
        std::cout << "PASS proxy STRING Set/Get" << std::endl;
    }

    {
        bcache2::thrift::HMSetRequest request;
        bcache2::thrift::HMSetResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":hash");
        request.__set_fields(std::vector<std::string>{"f1", "f2"});
        request.__set_values(std::vector<std::string>{"v1", "v2"});
        if (!Call(&channel, "HMSet", request, &response)) {
            return 1;
        }
    }

    {
        bcache2::thrift::HMGetRequest request;
        bcache2::thrift::HMGetResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":hash");
        request.__set_fields(std::vector<std::string>{"f1", "f2"});
        if (!Call(&channel, "HMGet", request, &response)) {
            return 1;
        }
        if (response.values.size() != 2 || response.values[0] != "v1" ||
            response.values[1] != "v2") {
            std::cerr << "HMGet returned unexpected values" << std::endl;
            return 1;
        }
        std::cout << "PASS proxy HASH HMSet/HMGet" << std::endl;
    }

    {
        bcache2::thrift::FeatureAddRequest request;
        bcache2::thrift::FeatureAddResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":feature");
        request.__set_format("json");
        request.__set_policy(bcache2::thrift::WritePolicy::UPSERT);
        bcache2::thrift::Point point;
        point.__set_ts(1000);
        point.__set_value(R"({"action_type":3,"duration":180,"gid":1001})");
        request.__set_point_list(std::vector<bcache2::thrift::Point>{point});
        if (!Call(&channel, "FeatureAdd", request, &response)) {
            return 1;
        }
    }

    {
        bcache2::thrift::FeatureQueryRequest request;
        bcache2::thrift::FeatureQueryResponse response;
        request.__set_namespace_name(namespace_name);
        request.__set_table_name(table_name);
        request.__set_key(prefix + ":feature");
        request.__set_format("json");
        request.__set_start_ts(900);
        request.__set_end_ts(1100);
        request.__set_count(10);
        if (!Call(&channel, "FeatureQuery", request, &response)) {
            return 1;
        }
        if (response.point_list.size() != 1) {
            std::cerr << "FeatureQuery returned " << response.point_list.size()
                      << " points, expected 1" << std::endl;
            return 1;
        }
        std::cout << "PASS proxy FEATURE Add/Query" << std::endl;
    }

    std::cout << "PASS proxy thrift smoke" << std::endl;
    return 0;
}
