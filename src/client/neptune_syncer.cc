// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/neptune_syncer.h"
#include <butil/rand_util.h>

#include <memory>

#include "bthread/bthread.h"
#include "client/client_impl.h"
#include "client/router.h"
#include "client/server_pool.h"
#include "common/bthread_closure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

static Status HttpRequest(brpc::HttpMethod method, const std::string& url,
                          rapidjson::Document* response) {
    brpc::ChannelOptions opts;
    opts.protocol = brpc::PROTOCOL_HTTP;
    brpc::Channel channel;
    if (channel.Init(url.c_str(), &opts) != 0) {
        LOG_WARNING("Init channel failed").put("Url", url);
        return Status::FailedPrecondition("Init channel failed");
    }

    brpc::Controller ctrl;
    ctrl.http_request().uri() = url;
    ctrl.http_request().set_method(method);

    channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
    if (ctrl.Failed()) {
        LOG_WARNING("Http request failed").put("Url", url).put("Error", ctrl.ErrorText());
        return Status::Internal(ctrl.ErrorText());
    }

    if (response) {
        response->Parse<0>(ctrl.response_attachment().to_string().c_str());
    }

    if (response->HasParseError()) {
        LOG_WARNING("Parse response failed");
        return Status::Internal("Parse response failed");
    }

    if (!response->HasMember("StatusCode") ||
        !(*response)["StatusCode"].IsInt() ||
        !response->HasMember("Data") ||
        !(*response)["Data"].IsArray()) {
        LOG_ERROR("Response is not ok");
        return Status::FailedPrecondition("Response is not ok");
    }

    auto status_code = (*response)["StatusCode"].GetInt();
    if (status_code != 0) {
        LOG_ERROR("Response status not ok");
        return Status::FailedPrecondition("Response status not ok");
    }

    LOG_DEBUG("Http finish")
        .put("Url", url)
        .put("Request", ctrl.request_attachment().to_string())
        .put("Response", ctrl.response_attachment().to_string());
    return Status::OK();
}

Status NeptuneSyncer::Init() {
    bthread::TimerThreadOptions timer_thread_options;
    int ret = timer_thread_.start(&timer_thread_options);
    if (ret != 0) {
        LOG_ERROR("Neptune syncer time thread start fail").put("Ret:", ret);
        return Status::Internal("Timer thread start failed ");
    }
    NeptuneSyncSchedule();
    return Status::OK();
}

void NeptuneSyncer::NeptuneSyncSchedule() {
    LOG_CALL_DEBUG();

    auto func = [this] {
        auto fn = [](void* data) {
            auto ptr = static_cast<NeptuneSyncer*>(data);
            ptr->NeptuneSyncSchedule();
        };

        uint64_t task_id = timer_thread_.schedule(
            fn, reinterpret_cast<void*>(this),
            butil::milliseconds_to_timespec(butil::gettimeofday_ms() +
                    options_.timer_interval_ms));
        BYTE_ASSERT(task_id != 0);
    };
    ScopedCallback done(NewFuncClosure(std::move(func)));

    const auto& server_addr = RefreshNeptuneServer();
    if (server_addr.compare("") == 0) {
        LOG_WARNING("RefreshNeptuneServer can not get server_addr");
        return;
    }
    Refresh(server_addr);
}

bool NeptuneSyncer::check_table_valid(
    const rapidjson::Value& data) {
    if (!data.HasMember("rpc_meta") ||
        !data["rpc_meta"].HasMember("callee") ||
        !data["rpc_meta"]["callee"].IsString()) {
        return false;
    }
    std::string tmp_psm(data["rpc_meta"]["callee"].GetString());
    if (tmp_psm.compare(client_opts_->bcache2_psm) != 0) {
        return false;
    }
    if (!data.HasMember("value") || data["value"].IsNull()) {
        return false;
    }
    return true;
}

void NeptuneSyncer::Refresh(const std::string& addr) {
    const std::string& caller_psm = client_opts_->psm;
    const std::string& caller_cluster = client_opts_->cluster;
    const std::string& callee_psm = client_opts_->bcache2_psm;

    std::string url, url2;
    url = "http://" + addr +
            "/api/v2/govern/cached_list?caller=" +
            caller_psm + "&caller_cluster=" + caller_cluster +
            "&category=IDCTraffic";

    rapidjson::Document response, response2;
    std::map<std::string, std::vector<std::pair<uint64_t, std::string>>> local_dc_weight_map;

    Status status = HttpRequest(brpc::HTTP_METHOD_GET, url, &response);
    if (!status.ok()) {
        LOG_WARNING("HttpRequest Failed").put("Url", url).put("Status", status);
        return;
    }

    const rapidjson::Value& data_array = response["Data"];
    for (rapidjson::SizeType i = 0; i < data_array.Size(); i++) {
        if (!check_table_valid(data_array[i])) {
            continue;
        }

        if (!data_array[i]["value"].HasMember("idc_weights") ||
            !data_array[i]["value"]["idc_weights"].IsArray()) {
                continue;
        }

        const rapidjson::Value& weight_array = data_array[i]["value"]["idc_weights"];
        for (rapidjson::SizeType j = 0; j < weight_array.Size(); j++) {
            if (!weight_array[j].HasMember("caller_idc") ||
                !weight_array[j].HasMember("callee_idc") ||
                !weight_array[j].HasMember("weight") ||
                !weight_array[j]["caller_idc"].IsString() ||
                !weight_array[j]["callee_idc"].IsString() ||
                !weight_array[j]["weight"].IsUint64()) {
                    continue;
            }
            std::string caller_dc(weight_array[j]["caller_idc"].GetString());
            std::string callee_dc(weight_array[j]["callee_idc"].GetString());
            const auto& weight = weight_array[j]["weight"].GetUint64();

            local_dc_weight_map[caller_dc].push_back(std::make_pair(weight, callee_dc));
        }
    }

    auto fn = [&](std::map<std::string,
        std::unique_ptr<RatioDice<std::string>>,
        IgnoreCaseLess>& dc_weight) {
        for (const auto& value : local_dc_weight_map) {
            const auto& caller_dc = value.first;
            const auto& local_rd = new(RatioDice<std::string>);
            for (const auto& elem : value.second) {
                if (elem.first > 0) {
                    LOG_INFO("Update map")
                    .put("Caller psm", caller_psm)
                    .put("Caller cluster", caller_cluster)
                    .put("Callee psm", callee_psm)
                    .put("Callee dc", caller_dc)
                    .put("Callee dc", elem.second)
                    .put("Weight", elem.first);
                    local_rd->AddProperty(elem.first, elem.second);
                }
            }
            dc_weight[caller_dc].reset(local_rd);
        }
        return true;
    };

    size_t rc = dc_weight_.Modify(fn);
    if (!rc) {
        LOG_WARNING("Update dc_weight failed");
    } else {
        LOG_INFO("Update dc_weight success");
    }

    url = "http://" + addr +
            "/api/v2/govern/cached_list?caller=" +
            caller_psm + "&caller_cluster=" + caller_cluster +
            "&category=DropPct";

    status = HttpRequest(brpc::HTTP_METHOD_GET, url, &response2);
    if (!status.ok()) {
        LOG_WARNING("HttpRequest Failed").put("Url", url).put("Status", status);
        return;
    }

    const rapidjson::Value& data_array2 = response2["Data"];
    for (rapidjson::SizeType i = 0; i < data_array2.Size(); i++) {
        if (!check_table_valid(data_array2[i])) {
            continue;
        }

        if (!data_array2[i]["value"].HasMember("drop_percentage") ||
            !data_array2[i]["value"]["drop_percentage"].IsUint64()) {
            continue;
        }

        drop_pct_ = data_array2[i]["value"]["drop_percentage"].GetUint64();
        if (drop_pct_ > 0) {
            LOG_INFO("Drop Pct change")
            .put("Caller psm", caller_psm)
            .put("Caller cluster", caller_cluster)
            .put("Callee psm", callee_psm)
            .put("Drop Pct", drop_pct_);
        }
    }
}

std::string NeptuneSyncer::RefreshNeptuneServer() {
    std::string server_addr = "";
    std::vector<service_discovery::Endpoint> master_endpoints;
    Status status = consul_.LookupWithCluster(
        neptune_consul_name_, neptune_consul_cluster_,
        service_discovery::Consul::AddrFamily::Auto, &master_endpoints);
    if (!status.ok()) {
        LOG_WARNING("Failed to lookup")
        .put("Name", neptune_consul_name_)
        .put("Cluster", neptune_consul_cluster_)
        .put("Status", status);
        return "";
    }
    auto addr_cnt = static_cast<int>(master_endpoints.size());
    if (addr_cnt == 0) {
        LOG_WARNING("Failed to lookup")
        .put("Cluster", neptune_consul_cluster_)
        .put("Consul", neptune_consul_name_);

        master_endpoints.clear();
        consul_.LookupWithCluster(
            neptune_consul_name_, neptune_consul_default_cluster_,
            service_discovery::Consul::AddrFamily::Auto, &master_endpoints);
        addr_cnt = static_cast<int>(master_endpoints.size());
        if (!status.ok() || (addr_cnt == 0)) {
            LOG_WARNING("Failed to lookup")
            .put("Name", neptune_consul_name_)
            .put("Cluster", neptune_consul_default_cluster_)
            .put("Status", status);
            return "";
        }
    }
    LOG_DEBUG("Refresh succeed")
        .put("consul server cnt", addr_cnt)
        .put("Server_addr", server_addr);
    if (addr_cnt >= 1) {
        const auto& pos = butil::RandInt(0, (addr_cnt-1));
        server_addr = master_endpoints[pos].host + ":"
                    + std::to_string(master_endpoints[pos].port);
        master_endpoints.clear();
        return server_addr;
    } else {
        return "";
    }
}

}  // namespace client
}  // namespace bcache2
