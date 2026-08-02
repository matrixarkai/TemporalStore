#include <iostream>
#include <memory>
#include <vector>

#include "temporalstore/client.h"

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: " << argv[0] << " <metaserver_host:port> <namespace> <table>\n";
        return 2;
    }

    temporalstore::ClientOptions options;
    options.metaserver_addr = argv[1];
    options.namespace_name = argv[2];
    options.table_name = argv[3];
    options.psm = "temporalstore.cpp.sdk.example";

    std::unique_ptr<temporalstore::Client> client;
    temporalstore::Status status = temporalstore::Client::Connect(options, &client);
    if (!status.ok()) {
        std::cerr << "connect failed: " << status.ToString() << "\n";
        return 1;
    }

    const std::string key = "cpp:user:42:sequence";
    temporalstore::SequenceFeatureRow first;
    first.timestamp = 1700000000000ULL;
    first.gid = 900ULL;
    first.action_type = 1;
    first.duration = 31;
    first.author_id = 7000ULL;
    temporalstore::SequenceFeatureRow second;
    second.timestamp = 1700000001000ULL;
    second.gid = 901ULL;
    second.action_type = 3;
    second.duration = 120;
    second.author_id = 7001ULL;
    std::vector<temporalstore::SequenceFeatureRow> rows = {first, second};
    status = client->AddSequenceFeatureRows(key, rows);
    if (!status.ok()) {
        std::cerr << "add sequence failed: " << status.ToString() << "\n";
        return 1;
    }

    temporalstore::FeatureQuery query;
    query.start_ts = 1700000000000ULL;
    query.end_ts = 1700000002000ULL;
    query.count = 10;
    temporalstore::FeatureFilter filter;
    filter.field = "action_type";
    filter.op = temporalstore::FeatureFilterOp::kEqual;
    filter.value = 3;
    query.filters.push_back(filter);

    std::vector<temporalstore::SequenceFeatureRow> out;
    status = client->QuerySequenceFeatureRows(key, query, &out);
    if (!status.ok()) {
        std::cerr << "query sequence failed: " << status.ToString() << "\n";
        return 1;
    }

    std::cout << "rows=" << out.size() << "\n";
    return 0;
}
