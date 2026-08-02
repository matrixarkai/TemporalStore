// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <map>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "bthread/mutex.h"
#include "butil/endpoint.h"
#include "json2pb/rapidjson.h"

#include "common/metaserver_tracker.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class ClusterMap {
 public:
    explicit ClusterMap(std::string path) : path_(path) {}
    ~ClusterMap() = default;

    Status Init();

    void Add(const std::string& cluster, const std::string& uri);
    Status GetOrCreateChannel(const std::string& cluster, brpc::Channel** channel);

    std::string GetUri(const std::string& cluster);
    butil::EndPoint GetLeader(const std::string& cluster);
    std::map<std::string, std::string> GetAllUri();

 private:
    Status Load(std::map<std::string, std::string>* m);
    Status Save(const std::map<std::string, std::string>& m);

 private:
    struct Backend {
        std::shared_ptr<MetaServerTracker> tracker;

        // leader channel cache
        butil::EndPoint leader;
        brpc::Channel channel;

        Backend(const std::string& cluster, const std::string& uri)
            : tracker(new MetaServerTracker(cluster, uri)) {}
        Backend(Backend&& rhs) { tracker = rhs.tracker; }
    };

    const std::string path_;
    bthread::Mutex mu_;
    std::map<std::string, Backend> backend_map_;
    std::map<std::string, std::string> cluster_uri_map_;
};

class LocationMap {
 public:
    explicit LocationMap(std::string path);

    Status Load();
    void Add(Location loc);
    Status Get(const std::string& vdc, Location* loc);

 private:
    const std::string path_;
    std::map<std::string, Location> vdc_to_loc_;
};

class HttpApiServiceImpl : public HttpApiService {
 public:
    explicit HttpApiServiceImpl(ClusterMap* m, LocationMap* loc_map)
        : cluster_map_(m), loc_map_(loc_map) {}
    ~HttpApiServiceImpl() = default;

    void AddCluster(google::protobuf::RpcController* controller, const HttpRequest* request,
                    HttpResponse* response, google::protobuf::Closure* done) override;

    void Query(google::protobuf::RpcController* controller, const HttpRequest* request,
               HttpResponse* response, google::protobuf::Closure* done) override;

    void Manage(google::protobuf::RpcController* controller, const HttpRequest* request,
                HttpResponse* response, google::protobuf::Closure* done) override;

 private:
    Status ListX(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status QueryInfo(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status ListCluster(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status ListServer(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status ListTable(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status ListPartition(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);
    Status ListServerPartition(const brpc::HttpHeader& http_request, butil::IOBuf* http_response);

    Status QueryManageInfo(const std::string& cluster, ManagementInfo* info);
    Status SendManageRequest(std::string cluster, std::string method, std::string data,
                             butil::rapidjson::Document* response);
    Status ConvertManageForm(const std::string& method, butil::rapidjson::Document* form,
                             std::string* result);

 private:
    ClusterMap* cluster_map_{nullptr};
    LocationMap* loc_map_{nullptr};
};
}  // namespace metaserver
}  // namespace bcache2
