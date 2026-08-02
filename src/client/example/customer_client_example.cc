#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "client/temporalstore_client.h"

namespace {

bool Check(const bcache2::Status& status, const std::string& op) {
    if (!status.ok()) {
        std::cerr << "FAIL " << op << ": " << status.ToString() << std::endl;
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cout << "usage: " << argv[0] << " <metaserver_host:port> <idc> <namespace> <table>"
                  << std::endl;
        return 2;
    }

    bcache2::client::TemporalStoreClientOptions options;
    options.metaserver_addr = argv[1];
    options.idc = argv[2];
    options.namespace_name = argv[3];
    options.table_name = argv[4];
    options.psm = "customer.client.example";

    std::unique_ptr<bcache2::client::TemporalStoreClient> client;
    if (!Check(bcache2::client::TemporalStoreClient::Connect(options, &client), "connect")) {
        return 1;
    }

    const std::string prefix = "customer_sdk_" + std::to_string(std::time(nullptr));

    if (!Check(client->PutString(prefix + ":profile", R"({"uid":42,"tier":"gold"})"),
               "put string")) {
        return 1;
    }
    std::string profile;
    if (!Check(client->GetString(prefix + ":profile", &profile), "get string")) {
        return 1;
    }

    if (!Check(client->HSet(prefix + ":features", "ctr_7d", "0.042"), "hset")) {
        return 1;
    }
    std::string ctr;
    if (!Check(client->HGet(prefix + ":features", "ctr_7d", &ctr), "hget")) {
        return 1;
    }

    if (!Check(client->SAdd(prefix + ":campaigns", "campaign_100"), "sadd")) {
        return 1;
    }
    std::vector<std::string> campaigns;
    if (!Check(client->SMembers(prefix + ":campaigns", &campaigns), "smembers")) {
        return 1;
    }

    std::vector<bcache2::client::SequenceFeatureRow> rows = {
        {1700000000000ULL, 900, 1, 31, 7000},
        {1700000001000ULL, 901, 3, 120, 7001},
    };
    if (!Check(client->AddSequenceFeatureRows(prefix + ":sequence", rows), "sequence add")) {
        return 1;
    }
    bcache2::client::TemporalFeatureQuery sequence_query;
    sequence_query.start_ts = 1700000000000ULL;
    sequence_query.end_ts = 1700000002000ULL;
    sequence_query.count = 10;
    sequence_query.filters.push_back(
        bcache2::client::TemporalFeatureFilter{"action_type",
                                               bcache2::client::TemporalFeatureFilterOp::kEqual,
                                               3});
    std::vector<bcache2::client::SequenceFeatureRow> queried_rows;
    if (!Check(client->QuerySequenceFeatureRows(prefix + ":sequence", sequence_query,
                                                &queried_rows),
               "sequence query")) {
        return 1;
    }

    bcache2::client::IpsInstance ips_instance;
    ips_instance.uid = 9900000 + static_cast<int64_t>(std::time(nullptr) % 1000000);
    ips_instance.timestamp_us = static_cast<int64_t>(std::time(nullptr)) * 1000LL * 1000LL;
    ips_instance.action_type = 0;
    ips_instance.logical_table = 0;
    ips_instance.features.push_back(bcache2::client::IpsFeatureStat{456, 23, true, 0, 12, 34});
    if (!Check(client->AddIpsInstance(ips_instance), "ips add")) {
        return 1;
    }
    bcache2::client::IpsLastQuery ips_query;
    ips_query.uid = ips_instance.uid;
    ips_query.action_type = 0;
    ips_query.logical_table = 0;
    ips_query.slot = 23;
    std::vector<bcache2::client::IpsFeatureStat> ips_features;
    if (!Check(client->QueryIpsLastInstances(ips_query, &ips_features), "ips query")) {
        return 1;
    }

    const std::string risk_key = prefix + ":risk";
    for (int i = 0; i < 3; ++i) {
        if (!Check(client->RiskIncrement(risk_key, 1, 24 * 3600,
                                         bcache2::client::RiskPrecision::kOneMinute,
                                         prefix + ":risk_uuid:" + std::to_string(i)),
                   "risk increment")) {
            return 1;
        }
    }
    int64_t risk_count = 0;
    if (!Check(client->RiskCount(risk_key, bcache2::client::RiskPrecision::kOneMinute,
                                 bcache2::client::RiskWindow{-1, 0,
                                                             bcache2::client::RiskWindowUnit::kHour},
                                 &risk_count),
               "risk count")) {
        return 1;
    }

    std::cout << "profile=" << profile << std::endl;
    std::cout << "ctr_7d=" << ctr << std::endl;
    std::cout << "campaigns=" << campaigns.size() << std::endl;
    std::cout << "sequence_rows=" << queried_rows.size() << std::endl;
    std::cout << "ips_features=" << ips_features.size() << std::endl;
    std::cout << "risk_count=" << risk_count << std::endl;
    std::cout << "PASS customer production client example" << std::endl;
    return 0;
}
