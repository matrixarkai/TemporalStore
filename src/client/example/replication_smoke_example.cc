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
    if (argc < 5 || argc > 7) {
        std::cout << "usage: " << argv[0] << " <metaserver_host:port> <idc> <namespace> <table>"
                  << " [max_wait_ms=10000] [poll_ms=1]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int max_wait_ms = argc > 5 ? std::atoi(argv[5]) : 10000;
    const int poll_ms = std::max(0, argc > 6 ? std::atoi(argv[6]) : 1);

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
    const auto deadline = start + std::chrono::milliseconds(std::max(0, max_wait_ms));
    for (int attempt = 1;; ++attempt) {
        got.clear();
        const auto raw_begin = std::chrono::steady_clock::now();
        last_status = secondary_reader_table->Get(key, &got);
        const auto raw_end = std::chrono::steady_clock::now();
        if (last_status.ok() && got == value) {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
            const auto raw_us =
                std::chrono::duration_cast<std::chrono::microseconds>(raw_end - raw_begin)
                    .count();
            std::cout << "PASS replication smoke: secondary read matched after " << attempt
                      << " attempts, visibility_wall_ms=" << elapsed
                      << ", raw_success_read_us=" << raw_us
                      << ", poll_ms=" << poll_ms << std::endl;
            return 0;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        if (poll_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
        } else {
            std::this_thread::yield();
        }
    }

    std::cerr << "FAIL secondary read did not catch up; last_status=" << last_status.ToString()
              << " value=" << got << " max_wait_ms=" << max_wait_ms
              << " poll_ms=" << poll_ms << std::endl;
    return 1;
}
