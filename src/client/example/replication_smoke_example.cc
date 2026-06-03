#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "client/client.h"
#include "common/status.h"

namespace {

bool Check(const bcache2::Status& status, const std::string& op) {
    if (!status.ok()) {
        std::cerr << "FAIL " << op << ": " << status.ToString() << std::endl;
        return false;
    }
    return true;
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               bcache2::client::PartitionPickOptions::Policy policy,
               const std::string& affinity_vdc,
               std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table) {
    bcache2::client::ClientOptions client_options;
    client_options.af = bcache2::client::AddressFamily::kIp4;
    client_options.master_addr = metaserver;
    client_options.idc = idc;
    client_options.host = "127.0.0.1";
    client_options.psm = "replication.smoke.example";
    client_options.log_level = bcache2::client::LogLevel::kWarning;
    client_options.partition_pick_opts.policy = policy;
    client_options.partition_pick_opts.affinity_vdc = affinity_vdc;

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(client_options, &raw_client);
    if (!Check(status, "create client")) {
        return false;
    }
    client->reset(raw_client);

    bcache2::client::TableOptions table_options;
    table_options.io_timeout_ms = 1000;
    table_options.connect_timeout_ms = 1000;
    bcache2::client::Table* raw_table = nullptr;
    status = (*client)->OpenTable(namespace_name, table_name, table_options, &raw_table);
    if (!Check(status, "open table")) {
        return false;
    }
    table->reset(raw_table);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cout << "usage: " << argv[0] << " <metaserver_host:port> <idc> <namespace> <table>"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];

    std::unique_ptr<bcache2::client::Client> writer_client;
    std::unique_ptr<bcache2::client::Table> writer_table;
    if (!OpenTable(metaserver, idc, namespace_name, table_name,
                   bcache2::client::PartitionPickOptions::Policy::kPrimary, "",
                   &writer_client, &writer_table)) {
        return 1;
    }

    std::unique_ptr<bcache2::client::Client> secondary_reader_client;
    std::unique_ptr<bcache2::client::Table> secondary_reader_table;
    if (!OpenTable(metaserver, idc, namespace_name, table_name,
                   bcache2::client::PartitionPickOptions::Policy::kVdcAffinity,
                   "force-secondary-read", &secondary_reader_client, &secondary_reader_table)) {
        return 1;
    }

    const std::string key =
        "replication_smoke_" + std::to_string(std::time(nullptr)) + "_" +
        std::to_string(static_cast<unsigned long long>(std::rand()));
    const std::string value = "replicated-value";

    if (!Check(writer_table->Set(key, value), "primary set")) {
        return 1;
    }

    std::string got;
    bcache2::Status last_status;
    const auto start = std::chrono::steady_clock::now();
    for (int attempt = 1; attempt <= 100; ++attempt) {
        got.clear();
        last_status = secondary_reader_table->Get(key, &got);
        if (last_status.ok() && got == value) {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
            std::cout << "PASS replication smoke: secondary read matched after " << attempt
                      << " attempts, " << elapsed << " ms" << std::endl;
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cerr << "FAIL secondary read did not catch up; last_status=" << last_status.ToString()
              << " value=" << got << std::endl;
    return 1;
}
