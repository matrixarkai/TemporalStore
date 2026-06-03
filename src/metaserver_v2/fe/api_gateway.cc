// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/fe/api_gateway.h"

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "brpc/http_header.h"
#include "brpc/server.h"
#include "brpc/uri.h"
#include "butil/file_util.h"
#include "butil/iobuf.h"
#include "butil/strings/string_util.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/proto_enhance.h"

namespace bcache2 {
namespace metaserver {

Status ClusterMap::Init() { return Load(&cluster_uri_map_); }

Status ClusterMap::Load(std::map<std::string, std::string>* m) {
    std::string file_data;
    butil::FilePath fp = butil::FilePath(path_);
    if (butil::PathExists(fp)) {
        if (!butil::ReadFileToString(fp, &file_data)) {
            return Status::Internal("read file failed");
        }
    }
    if (file_data.empty()) {
        return Status::OK();
    }
    LOG_INFO("loading clusters from file");
    std::vector<std::string> lines;
    Tokenize(file_data, "\n", &lines);
    for (size_t i = 0; i < lines.size(); i += 2) {
        if (i + 1 == lines.size()) {
            break;
        }
        LOG_INFO("loading cluster from file").put("cluster", lines[i]).put("uri", lines[i + 1]);
        (*m)[lines[i]] = lines[i + 1];
    }

    return Status::OK();
}

Status ClusterMap::Save(const std::map<std::string, std::string>& m) {
    if (m.empty()) {
        return Status::OK();
    }
    std::vector<std::string> lines;
    for (auto pair : m) {
        lines.push_back(pair.first);
        lines.push_back(pair.second);
    }
    std::string file_data = JoinString(lines, "\n");
    butil::FilePath tmp_fp = butil::FilePath(path_ + ".tmp");
    int rc = butil::WriteFile(tmp_fp, file_data.data(), file_data.size());
    if (rc < 0) {
        return Status::Internal("write file failed");
    }
    butil::FilePath fp = butil::FilePath(path_);
    if (!butil::Move(tmp_fp, fp)) {
        return Status::Internal("rename file failed");
    }
    return Status::OK();
}

void ClusterMap::Add(const std::string& cluster, const std::string& uri) {
    LOG_INFO("add cluster").put("cluster", cluster).put("uri", uri);
    std::lock_guard<bthread::Mutex> _(mu_);
    backend_map_.erase(cluster);
    cluster_uri_map_[cluster] = uri;
    std::map<std::string, std::string> m = cluster_uri_map_;
    Save(m);
}

std::string ClusterMap::GetUri(const std::string& cluster) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = cluster_uri_map_.find(cluster);
    if (iter == cluster_uri_map_.end()) {
        return "-";
    }
    return iter->second;
}

butil::EndPoint ClusterMap::GetLeader(const std::string& cluster) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = backend_map_.find(cluster);
    if (iter == backend_map_.end()) {
        return butil::EndPoint();
    }
    return iter->second.leader;
}

std::map<std::string, std::string> ClusterMap::GetAllUri() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return cluster_uri_map_;
}

Status ClusterMap::GetOrCreateChannel(const std::string& cluster, brpc::Channel** channel) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = backend_map_.find(cluster);
    if (iter == backend_map_.end()) {
        auto iter2 = cluster_uri_map_.find(cluster);
        if (iter2 == cluster_uri_map_.end()) {
            return Status::NotFound("uri not found");
        }
        Backend bk(cluster, iter2->second);
        Status status = bk.tracker->Start();
        if (!status.ok()) {
            return status;
        }
        iter = backend_map_.emplace(std::make_pair(cluster, std::move(bk))).first;
    }
    Backend& bk = iter->second;
    auto& tracker = bk.tracker;
    if (bk.leader.port > 0 && tracker->IsLeader(bk.leader)) {
        *channel = &bk.channel;
        return Status::OK();
    }
    Status status = tracker->GetLeaderEndpoint(&bk.leader);
    if (!status.ok()) {
        return status;
    }
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    int rc = bk.channel.Init(bk.leader, &opts);
    if (rc != 0) {
        bk.leader.port = 0;
        return Status::Internal("init channel failed");
    }
    *channel = &bk.channel;
    return Status::OK();
}

//////////

template <typename Message>
Status pb2json(const Message& m, butil::rapidjson::Document* data) {
    butil::IOBuf buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
    std::string err;
    json2pb::Pb2JsonOptions opt;
    if (!json2pb::ProtoMessageToJson(m, &wrapper, opt, &err)) {
        return Status::Internal("pb2json failed: " + err);
    }
    data->Parse<0>(buf.to_string().c_str());
    return Status::OK();
}

static void PackStatus(const Status& status, butil::IOBuf* output) {
    butil::rapidjson::StringBuffer sb;
    butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("status");
    writer.AddInt(status.errorcode());
    writer.Key("msg");
    const std::string msg = status.ToString();
    writer.String(msg.data(), msg.size());
    writer.EndObject();
    output->append(sb.GetString());
}

static void PackData(butil::rapidjson::Value data_field, butil::IOBuf* output) {
    butil::rapidjson::Document root;
    root.SetObject();
    root.AddMember("status", butil::rapidjson::Value(0), root.GetAllocator());
    root.AddMember("msg", butil::rapidjson::Value(""), root.GetAllocator());
    root.AddMember("data", data_field, root.GetAllocator());
    butil::rapidjson::StringBuffer sb;
    butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(sb);
    root.Accept(writer);
    output->append(sb.GetString());
}

void HttpApiServiceImpl::AddCluster(google::protobuf::RpcController* controller,
                                    const HttpRequest* request, HttpResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    const butil::IOBuf& buf = cntl->request_attachment();
    butil::rapidjson::Document doc;
    doc.Parse<0>(buf.to_string().c_str());

    std::string cluster;
    std::string uri;
    if (doc.HasMember("cluster")) {
        cluster = doc["cluster"].GetString();
    }
    if (doc.HasMember("uri")) {
        uri = doc["uri"].GetString();
    }
    Status status = Status::OK();
    if (cluster.empty() || uri.empty()) {
        status = Status::FailedPrecondition("invalid param");
    } else {
        cluster_map_->Add(cluster, uri);
    }
    cntl->http_response().set_content_type("application/json");
    PackStatus(status, &(cntl->response_attachment()));
}

void HttpApiServiceImpl::Query(google::protobuf::RpcController* controller,
                               const HttpRequest* request, HttpResponse* response,
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    cntl->http_response().set_content_type("application/json");
    const std::string& unresolved_path = cntl->http_request().unresolved_path();
    Status status;
    if (!strcasecmp(unresolved_path.data(), "list_server")) {
        status = ListServer(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "list_table")) {
        status = ListTable(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "list_partition")) {
        status = ListPartition(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "list_server_partition")) {
        status = ListServerPartition(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "info")) {
        status = QueryInfo(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "list_cluster")) {
        status = ListCluster(cntl->http_request(), &(cntl->response_attachment()));
    } else if (!strcasecmp(unresolved_path.data(), "listx")) {
        status = ListX(cntl->http_request(), &(cntl->response_attachment()));
    } else {
        status =
            Status::FailedPrecondition(fmt::format("unknown method {}", unresolved_path.data()));
    }
    if (!status.ok()) {
        LOG_WARNING("failed to do query").put("path", unresolved_path).put("status", status);
        PackStatus(status, &(cntl->response_attachment()));
    }
}

Status HttpApiServiceImpl::QueryInfo(const brpc::HttpHeader& http_request,
                                     butil::IOBuf* http_response) {
    butil::rapidjson::Document data;
    data.SetObject();
    auto& alloc = data.GetAllocator();
    butil::rapidjson::Document items;
    items.SetArray();
    for (auto& pair : cluster_map_->GetAllUri()) {
        const std::string& cluster = pair.first;
        const std::string& ms_uri = pair.second;

        butil::rapidjson::Document item(&alloc);
        item.SetObject();
        item.AddMember("cluster", butil::rapidjson::Value(cluster.data(), alloc), alloc);
        item.AddMember("uri", butil::rapidjson::Value(ms_uri.data(), alloc), alloc);
        const butil::EndPoint leader_ep = cluster_map_->GetLeader(cluster);
        item.AddMember("leader",
                       butil::rapidjson::Value(butil::endpoint2str(leader_ep).c_str(), alloc),
                       alloc);
        ManagementInfo pb_info;
        Status status = QueryManageInfo(cluster, &pb_info);
        if (status.ok()) {
            butil::rapidjson::Document info(&alloc);
            status = pb2json(pb_info, &info);
            if (status.ok()) {
                item.AddMember("info", info, alloc);
            } else {
                LOG_WARNING("failed to convert manage info").put("result", status);
            }
        } else {
            LOG_WARNING("failed to query manage info").put("result", status);
        }
        items.PushBack(item, alloc);
    }
    data.AddMember("items", items, alloc);
    PackData(std::move(data), http_response);
    return Status::OK();
}

Status HttpApiServiceImpl::QueryManageInfo(const std::string& cluster, ManagementInfo* info) {
    brpc::Channel* channel = nullptr;
    Status status = cluster_map_->GetOrCreateChannel(cluster, &channel);
    if (!status.ok()) {
        return status;
    }

    brpc::Controller bk_cntl;
    bk_cntl.set_log_id(butil::fast_rand());
    bk_cntl.set_timeout_ms(30'000);
    QueryService_Stub stub(channel);
    EmptyRequest bk_request;
    QueryManageInfoResponse bk_response;
    stub.QueryManageInfo(&bk_cntl, &bk_request, &bk_response, nullptr);
    if (bk_cntl.Failed()) {
        return Status::Internal("rpc failed " + bk_cntl.ErrorText());
    }
    status = Status::FromRpcStatus(bk_response.status());
    if (!status.ok()) {
        return status;
    }
    info->CopyFrom(bk_response.info());
    return Status::OK();
}

Status HttpApiServiceImpl::ListCluster(const brpc::HttpHeader& http_request,
                                       butil::IOBuf* http_response) {
    butil::rapidjson::Document data;
    data.SetObject();
    auto& alloc = data.GetAllocator();
    butil::rapidjson::Document items;
    items.SetArray();
    for (auto& pair : cluster_map_->GetAllUri()) {
        const std::string& cluster = pair.first;

        butil::rapidjson::Document item(&alloc);
        item.SetObject();
        item.AddMember("value", butil::rapidjson::Value(cluster.data(), alloc), alloc);
        item.AddMember("label", butil::rapidjson::Value(cluster.data(), alloc), alloc);
        items.PushBack(item, alloc);
    }
    data.AddMember("options", items, alloc);
    PackData(std::move(data), http_response);
    return Status::OK();
}

Status HttpApiServiceImpl::ListX(const brpc::HttpHeader& http_request,
                                 butil::IOBuf* http_response) {
    std::string action;
    const brpc::URI& uri = http_request.uri();
    for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); iter++) {
        const std::string& k = iter->first;
        const std::string& v = iter->second;
        if (!strcasecmp(k.data(), "action")) {
            action = v;
            break;
        }
    }
    Status status;
    if (!strcasecmp(action.data(), "list_server")) {
        status = ListServer(http_request, http_response);
    } else if (!strcasecmp(action.data(), "list_table")) {
        status = ListTable(http_request, http_response);
    } else if (!strcasecmp(action.data(), "list_partition")) {
        status = ListPartition(http_request, http_response);
    } else if (!strcasecmp(action.data(), "list_server_partition")) {
        status = ListServerPartition(http_request, http_response);
    } else if (!strcasecmp(action.data(), "info")) {
        status = QueryInfo(http_request, http_response);
    } else if (!strcasecmp(action.data(), "list_cluster")) {
        status = ListCluster(http_request, http_response);
    } else {
        status = Status::FailedPrecondition(fmt::format("unknown action {}", action));
    }
    return status;
}

Status HttpApiServiceImpl::ListServer(const brpc::HttpHeader& http_request,
                                      butil::IOBuf* http_response) {
    std::string cluster;
    ListServerRequest bk_request;
    const brpc::URI& uri = http_request.uri();
    for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); iter++) {
        const std::string& k = iter->first;
        const std::string& v = iter->second;
        if (!strcasecmp(k.data(), "cluster")) {
            cluster = v;
        } else if (!strcasecmp(k.data(), "vregion")) {
            bk_request.mutable_location()->set_vregion(v);
        } else if (!strcasecmp(k.data(), "vdc")) {
            bk_request.mutable_location()->set_vdc(v);
        } else if (!strcasecmp(k.data(), "vau")) {
            bk_request.mutable_location()->set_vau(v);
        } else if (!strcasecmp(k.data(), "tag")) {
            bk_request.mutable_location()->set_tag(v);
        } else if (!strcasecmp(k.data(), "prefix")) {
            bk_request.set_ip_substr(v);
        }
    }
    if (cluster.empty()) {
        return Status::FailedPrecondition("missing cluster field");
    }
    brpc::Channel* channel = nullptr;
    Status status = cluster_map_->GetOrCreateChannel(cluster, &channel);
    if (!status.ok()) {
        return status;
    }

    brpc::Controller bk_cntl;
    bk_cntl.set_log_id(butil::fast_rand());
    bk_cntl.set_timeout_ms(30'000);
    QueryService_Stub stub(channel);
    ListServerResponse bk_response;
    stub.ListServer(&bk_cntl, &bk_request, &bk_response, nullptr);
    if (bk_cntl.Failed()) {
        return Status::Internal("rpc failed " + bk_cntl.ErrorText());
    }
    status = Status::FromRpcStatus(bk_response.status());
    if (!status.ok()) {
        return status;
    }
    butil::rapidjson::Document data;
    status = pb2json(bk_response, &data);
    if (!status.ok()) {
        return status;
    }

    if (!data.HasMember("servers")) {
        return Status::NotFound("no server registered");
    }

    butil::rapidjson::Value items(butil::rapidjson::kObjectType);
    items.AddMember("items", data["servers"], data.GetAllocator());
    PackData(std::move(items), http_response);
    return Status::OK();
}

Status HttpApiServiceImpl::ListTable(const brpc::HttpHeader& http_request,
                                     butil::IOBuf* http_response) {
    std::string cluster;
    ListTableRequest bk_request;
    const brpc::URI& uri = http_request.uri();
    for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); iter++) {
        const std::string& k = iter->first;
        const std::string& v = iter->second;
        if (!strcasecmp(k.data(), "cluster")) {
            cluster = v;
        } else if (!strcasecmp(k.data(), "namespace")) {
            bk_request.set_namespace_name(v);
        } else if (!strcasecmp(k.data(), "table")) {
            bk_request.set_table_name(v);
        }
    }
    if (cluster.empty()) {
        return Status::FailedPrecondition("missing cluster field");
    }
    brpc::Channel* channel = nullptr;
    Status status = cluster_map_->GetOrCreateChannel(cluster, &channel);
    if (!status.ok()) {
        return status;
    }

    brpc::Controller bk_cntl;
    bk_cntl.set_log_id(butil::fast_rand());
    bk_cntl.set_timeout_ms(30'000);
    QueryService_Stub stub(channel);
    ListTableResponse bk_response;
    stub.ListTable(&bk_cntl, &bk_request, &bk_response, nullptr);
    if (bk_cntl.Failed()) {
        return Status::Internal("rpc failed " + bk_cntl.ErrorText());
    }
    status = Status::FromRpcStatus(bk_response.status());
    if (!status.ok()) {
        return status;
    }
    butil::rapidjson::Document data;
    status = pb2json(bk_response, &data);
    if (!status.ok()) {
        return status;
    }

    if (!data.HasMember("tables")) {
        return Status::NotFound("no table found");
    }
    butil::rapidjson::Value items(butil::rapidjson::kObjectType);
    items.AddMember("items", data["tables"], data.GetAllocator());
    PackData(std::move(items), http_response);
    return Status::OK();
}

Status HttpApiServiceImpl::ListPartition(const brpc::HttpHeader& http_request,
                                         butil::IOBuf* http_response) {
    std::string cluster;
    ListPartitionRequest bk_request;
    const brpc::URI& uri = http_request.uri();
    for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); iter++) {
        const std::string& k = iter->first;
        const std::string& v = iter->second;
        if (!strcasecmp(k.data(), "cluster")) {
            cluster = v;
        } else if (!strcasecmp(k.data(), "namespace")) {
            bk_request.set_namespace_name(v);
        } else if (!strcasecmp(k.data(), "table")) {
            bk_request.set_table_name(v);
        } else if (!strcasecmp(k.data(), "partition_id")) {
            const uint64_t pid = strtoull(v.data(), nullptr, 10);
            bk_request.set_partition_id(pid);
        }
    }
    if (cluster.empty()) {
        return Status::FailedPrecondition("missing cluster field");
    }
    brpc::Channel* channel = nullptr;
    Status status = cluster_map_->GetOrCreateChannel(cluster, &channel);
    if (!status.ok()) {
        return status;
    }

    brpc::Controller bk_cntl;
    bk_cntl.set_log_id(butil::fast_rand());
    bk_cntl.set_timeout_ms(30'000);
    QueryService_Stub stub(channel);
    ListPartitionResponse bk_response;
    stub.ListPartition(&bk_cntl, &bk_request, &bk_response, nullptr);
    if (bk_cntl.Failed()) {
        return Status::Internal("rpc failed " + bk_cntl.ErrorText());
    }
    status = Status::FromRpcStatus(bk_response.status());
    if (!status.ok()) {
        return status;
    }
    butil::rapidjson::Document data;
    status = pb2json(bk_response, &data);
    if (!status.ok()) {
        return status;
    }

    if (!data.HasMember("info")) {
        return Status::NotFound("no found");
    }
    butil::rapidjson::Value items(butil::rapidjson::kObjectType);
    items.AddMember("items", data["info"], data.GetAllocator());
    PackData(std::move(items), http_response);
    return Status::OK();
}

Status HttpApiServiceImpl::ListServerPartition(const brpc::HttpHeader& http_request,
                                               butil::IOBuf* http_response) {
    std::string cluster;
    ListServerPartitionRequest bk_request;
    const brpc::URI& uri = http_request.uri();
    for (auto iter = uri.QueryBegin(); iter != uri.QueryEnd(); iter++) {
        const std::string& k = iter->first;
        const std::string& v = iter->second;
        if (!strcasecmp(k.data(), "cluster")) {
            cluster = v;
        } else if (!strcasecmp(k.data(), "server_id")) {
            const uint32_t server_id = strtoul(v.data(), nullptr, 10);
            bk_request.set_server_id(server_id);
        }
    }
    if (cluster.empty()) {
        return Status::FailedPrecondition("missing cluster field");
    }
    brpc::Channel* channel = nullptr;
    Status status = cluster_map_->GetOrCreateChannel(cluster, &channel);
    if (!status.ok()) {
        return status;
    }

    brpc::Controller bk_cntl;
    bk_cntl.set_log_id(butil::fast_rand());
    bk_cntl.set_timeout_ms(30'000);
    QueryService_Stub stub(channel);
    ListServerPartitionResponse bk_response;
    stub.ListServerPartition(&bk_cntl, &bk_request, &bk_response, nullptr);
    if (bk_cntl.Failed()) {
        return Status::Internal("rpc failed " + bk_cntl.ErrorText());
    }
    status = Status::FromRpcStatus(bk_response.status());
    if (!status.ok()) {
        return status;
    }
    butil::rapidjson::Document data;
    status = pb2json(bk_response, &data);
    if (!status.ok()) {
        return status;
    }

    if (!data.HasMember("node_partitions")) {
        return Status::NotFound("no found");
    }
    butil::rapidjson::Value items(butil::rapidjson::kObjectType);
    items.AddMember("items", data["node_partitions"], data.GetAllocator());
    PackData(std::move(items), http_response);
    return Status::OK();
}

void HttpApiServiceImpl::Manage(google::protobuf::RpcController* controller,
                                const HttpRequest* request, HttpResponse* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    const butil::IOBuf& buf = cntl->request_attachment();
    butil::rapidjson::Document doc;
    LOG_INFO("add table data:").put("raw", buf.to_string().c_str());
    doc.Parse<0>(buf.to_string().c_str());

    std::string cluster;
    std::string method;
    std::string data;
    Status status = Status::OK();
    do {
        if (!doc.HasMember("cluster")) {
            status = Status::FailedPrecondition("missing cluster");
            break;
        }
        cluster = doc["cluster"].GetString();
        if (!doc.HasMember("method")) {
            status = Status::FailedPrecondition("missing method");
            break;
        }
        method = doc["method"].GetString();
        if (!doc.HasMember("data")) {
            status = Status::FailedPrecondition("missing data");
            break;
        }
        status = ConvertManageForm(method, &doc, &data);
    } while (false);
    cntl->http_response().set_content_type("application/json");
    if (!status.ok()) {
        PackStatus(status, &(cntl->response_attachment()));
        return;
    }
    LOG_INFO("received manage request")
        .put("cluster", cluster)
        .put("method", method)
        .put("data", data);
    butil::rapidjson::Document bk_response;
    status =
        SendManageRequest(std::move(cluster), std::move(method), std::move(data), &bk_response);
    if (!status.ok()) {
        PackStatus(status, &(cntl->response_attachment()));
        return;
    }
    if (!bk_response.HasMember("status") ||          //
        !bk_response["status"].HasMember("code") ||  //
        bk_response["status"]["code"].GetInt() == 0) {
        PackData(std::move(bk_response), &(cntl->response_attachment()));
        return;
    }
    const int code = bk_response["status"]["code"].GetInt();
    const std::string msg = bk_response["status"].HasMember("message")
                                ? bk_response["status"]["message"].GetString()
                                : "";
    status = Status(static_cast<Code>(code), msg);
    PackStatus(status, &(cntl->response_attachment()));
}

Status HttpApiServiceImpl::ConvertManageForm(const std::string& method,
                                             butil::rapidjson::Document* doc,
                                             std::string* data) {
    butil::rapidjson::Value& form = (*doc)["data"];
    butil::rapidjson::StringBuffer sb;
    butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(sb);
    form.Accept(writer);
    std::string json_str = sb.GetString();
    LOG_INFO("add table config:").put("json", json_str);
    if (method == "AddTable") {
        AddTableRequest request;
        std::string err_msg;
        if (!json2pb::JsonToProtoMessage(json_str, &request, &err_msg)) {
            return Status::FailedPrecondition(err_msg);
        }
        for (int i = 0; i < request.partition_units_size(); i++) {
            auto unit = request.mutable_partition_units(i);
            size_t pnum = unit->partition_num();
            size_t loc_count = unit->placement_set_size();
            if (loc_count == 0) {
                return Status::FailedPrecondition("no placement found");
            }
            // TODO(wuzhenyu) 这里比较tricky，受限于SCM平台
            if (pnum > 1 && loc_count == pnum) {
                continue;
            }
            const Location tpl = unit->placement_set(0);
            unit->clear_placement_set();
            std::vector<std::string> vdcs;
            Tokenize(tpl.vdc(), ",", &vdcs);
            for (size_t j = 0; j < pnum; j++) {
                std::string vdc = vdcs[j % vdcs.size()];
                std::vector<std::string> vdc_with_tag;
                Tokenize(vdc, "-", &vdc_with_tag);
                Location* loc = unit->add_placement_set();
                Status status = loc_map_->Get(vdc_with_tag[0], loc);
                if (!status.ok()) {
                    return Status::FailedPrecondition("no location found");
                }
                if (vdc_with_tag.size() > 1) {
                    loc->set_tag(vdc_with_tag[1]);
                }
            }
        }  // for partition unit
        LOG_INFO("convert add table form").put("result", request.DebugString());
        butil::IOBuf buf;
        butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
        std::string err;
        json2pb::Pb2JsonOptions opt;
        if (!json2pb::ProtoMessageToJson(request, &wrapper, opt, &err)) {
            return Status::Internal("pb2json failed: " + err);
        }
        *data = buf.to_string();
    } else {
        *data = std::move(json_str);
    }
    return Status::OK();
}

Status HttpApiServiceImpl::SendManageRequest(std::string cluster, std::string method,
                                             std::string data,
                                             butil::rapidjson::Document* response) {
    butil::EndPoint leader = cluster_map_->GetLeader(cluster);
    if (leader.ip == butil::IP_ANY) {
        return Status::NotFound("leader not found");
    }
    std::string ep_str = butil::endpoint2str(leader).c_str();
    std::string uri = fmt::format("http://{}/ManageService/{}", ep_str, method);

    brpc::ChannelOptions opts;
    opts.protocol = brpc::PROTOCOL_HTTP;
    brpc::Channel channel;
    channel.Init(uri.c_str(), &opts);

    brpc::Controller cntl;
    cntl.http_request().uri() = uri;
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.request_attachment() = data;
    cntl.http_request().set_content_type("application/json");
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
        return Status::Internal("rpc failed " + cntl.ErrorText());
    }

    response->Parse<0>(cntl.response_attachment().to_string().c_str());
    return Status::OK();
}

LocationMap::LocationMap(std::string path) : path_(std::move(path)) {}

Status LocationMap::Load() {
    std::string file_data;
    butil::FilePath fp = butil::FilePath(path_);
    if (butil::PathExists(fp)) {
        if (!butil::ReadFileToString(fp, &file_data)) {
            return Status::Internal("read file failed");
        }
    }
    if (file_data.empty()) {
        return Status::OK();
    }
    LOG_INFO("loading location from file");
    std::vector<std::string> lines;
    Tokenize(file_data, "\n", &lines);
    for (size_t i = 0; i < lines.size(); i += 2) {
        if (i + 1 == lines.size()) {
            break;
        }
        Location loc;
        {
            std::string o;
            butil::TrimString(lines[i], " ", &o);
            loc.set_vregion(o);
        }
        {
            std::string o;
            butil::TrimString(lines[i + 1], " ", &o);
            loc.set_vdc(o);
        }

        LOG_INFO("loading location from file").put("loc", loc.ShortDebugString());
        Add(std::move(loc));
    }
    return Status::OK();
}

void LocationMap::Add(Location loc) {
    if (!loc.vdc().empty() && ValidateFuzzily(loc)) {
        vdc_to_loc_[loc.vdc()] = loc;
    }
}

Status LocationMap::Get(const std::string& vdc, Location* loc) {
    auto iter = vdc_to_loc_.find(vdc);
    if (iter == vdc_to_loc_.end()) {
        return Status::NotFound("vdc not found");
    }
    *loc = iter->second;
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

