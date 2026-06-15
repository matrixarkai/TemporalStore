#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "common/status.h"

namespace {

struct Summary {
    std::string phase;
    int ops = 0;
    int threads = 0;
    int value_bytes = 0;
    int errors = 0;
    double qps = 0;
    int64_t avg_us = 0;
    int64_t p50_us = 0;
    int64_t p95_us = 0;
    int64_t p99_us = 0;
    int64_t min_us = 0;
    int64_t max_us = 0;
    int64_t total_ms = 0;
};

int64_t Percentile(const std::vector<int64_t>& sorted, double pct) {
    if (sorted.empty()) {
        return 0;
    }
    const double pos = pct * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<size_t>(pos + 0.5)];
}

Summary Summarize(const std::string& phase, std::vector<int64_t> samples, int threads,
                  int value_bytes, int errors, int64_t total_ms) {
    std::sort(samples.begin(), samples.end());
    Summary s;
    s.phase = phase;
    s.ops = static_cast<int>(samples.size());
    s.threads = threads;
    s.value_bytes = value_bytes;
    s.errors = errors;
    s.total_ms = total_ms;
    s.qps = total_ms > 0 ? static_cast<double>(s.ops) * 1000.0 / static_cast<double>(total_ms) : 0;
    const int64_t sum = std::accumulate(samples.begin(), samples.end(), int64_t{0});
    s.avg_us = samples.empty() ? 0 : sum / static_cast<int64_t>(samples.size());
    s.p50_us = Percentile(samples, 0.50);
    s.p95_us = Percentile(samples, 0.95);
    s.p99_us = Percentile(samples, 0.99);
    s.min_us = samples.empty() ? 0 : samples.front();
    s.max_us = samples.empty() ? 0 : samples.back();
    return s;
}

void PrintSummary(const Summary& s) {
    std::cout << "system,phase,ops,threads,value_bytes,errors,qps,avg_us,p50_us,p95_us,p99_us,min_us,max_us,total_ms"
              << std::endl;
    std::cout << "TemporalStore," << s.phase << "," << s.ops << "," << s.threads << ","
              << s.value_bytes << "," << s.errors << "," << static_cast<int64_t>(s.qps) << ","
              << s.avg_us << "," << s.p50_us << "," << s.p95_us << "," << s.p99_us << ","
              << s.min_us << "," << s.max_us << "," << s.total_ms << std::endl;
}

template <typename Fn>
bool RetryUntil(int max_wait_ms, int sleep_ms, Fn fn) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max(0, max_wait_ms));
    do {
        if (fn()) {
            return true;
        }
        if (max_wait_ms <= 0) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    } while (std::chrono::steady_clock::now() < deadline);
    return fn();
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               bool pin_primary, std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table);

bool RunGetPhase(const std::string& metaserver, const std::string& idc,
                 const std::string& namespace_name, const std::string& table_name,
                 bool pin_primary, int ops, int threads, int value_bytes,
                 const std::string& prefix, const std::string& expected_value,
                 int retry_ms, Summary* raw_summary, Summary* visibility_summary) {
    std::vector<int64_t> raw_samples(static_cast<size_t>(ops), 0);
    std::vector<int64_t> visibility_samples(static_cast<size_t>(ops), 0);
    std::vector<uint8_t> success(static_cast<size_t>(ops), 0);
    std::atomic<int> errors{0};
    std::atomic<int> retry_attempts{0};
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(threads);

    auto wall_begin = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            std::unique_ptr<bcache2::client::Client> client;
            std::unique_ptr<bcache2::client::Table> table;
            if (!OpenTable(metaserver, idc, namespace_name, table_name, pin_primary, &client,
                           &table)) {
                errors.fetch_add(1);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = t; i < ops; i += threads) {
                const std::string key = prefix + ":" + std::to_string(i);
                const auto visibility_begin = std::chrono::steady_clock::now();
                const auto deadline =
                    visibility_begin + std::chrono::milliseconds(std::max(0, retry_ms));
                bool ok = false;
                int attempts = 0;
                int64_t success_raw_us = 0;
                do {
                    ++attempts;
                    std::string got;
                    const auto raw_begin = std::chrono::steady_clock::now();
                    bcache2::Status status = table->Get(key, &got);
                    const auto raw_end = std::chrono::steady_clock::now();
                    if (status.ok() && got == expected_value) {
                        success_raw_us =
                            std::chrono::duration_cast<std::chrono::microseconds>(raw_end -
                                                                                  raw_begin)
                                .count();
                        ok = true;
                        break;
                    }
                    if (retry_ms <= 0) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                } while (std::chrono::steady_clock::now() < deadline);

                if (!ok && retry_ms > 0) {
                    std::string got;
                    const auto raw_begin = std::chrono::steady_clock::now();
                    bcache2::Status status = table->Get(key, &got);
                    const auto raw_end = std::chrono::steady_clock::now();
                    ++attempts;
                    if (status.ok() && got == expected_value) {
                        success_raw_us =
                            std::chrono::duration_cast<std::chrono::microseconds>(raw_end -
                                                                                  raw_begin)
                                .count();
                        ok = true;
                    }
                }

                retry_attempts.fetch_add(std::max(0, attempts - 1));
                if (!ok) {
                    errors.fetch_add(1);
                    continue;
                }

                const auto visibility_end = std::chrono::steady_clock::now();
                success[static_cast<size_t>(i)] = 1;
                raw_samples[static_cast<size_t>(i)] = success_raw_us;
                visibility_samples[static_cast<size_t>(i)] =
                    std::chrono::duration_cast<std::chrono::microseconds>(visibility_end -
                                                                          visibility_begin)
                        .count();
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    wall_begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto wall_end = std::chrono::steady_clock::now();
    const int64_t total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(wall_end - wall_begin).count();

    std::vector<int64_t> raw_success_samples;
    std::vector<int64_t> visibility_success_samples;
    raw_success_samples.reserve(static_cast<size_t>(ops));
    visibility_success_samples.reserve(static_cast<size_t>(ops));
    for (int i = 0; i < ops; ++i) {
        if (success[static_cast<size_t>(i)] != 0) {
            raw_success_samples.push_back(raw_samples[static_cast<size_t>(i)]);
            visibility_success_samples.push_back(visibility_samples[static_cast<size_t>(i)]);
        }
    }

    *raw_summary = Summarize("get_raw_success_attempt", std::move(raw_success_samples), threads,
                             value_bytes, errors.load(), total_ms);
    *visibility_summary = Summarize("get_visibility_retry", std::move(visibility_success_samples),
                                    threads, value_bytes, errors.load(), total_ms);
    if (retry_attempts.load() > 0) {
        std::cerr << "get_retry_attempts=" << retry_attempts.load() << std::endl;
    }
    return errors.load() == 0;
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               bool pin_primary, std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table) {
    bcache2::client::ClientOptions client_options;
    client_options.af = bcache2::client::AddressFamily::kIp4;
    client_options.master_addr = metaserver;
    client_options.idc = idc;
    client_options.host = "127.0.0.1";
    client_options.psm = "string.scale.benchmark";
    client_options.log_level = bcache2::client::LogLevel::kWarning;
    if (pin_primary) {
        client_options.partition_pick_opts.policy =
            bcache2::client::PartitionPickOptions::Policy::kPrimary;
    }

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(client_options, &raw_client);
    if (!status.ok()) {
        std::cerr << "Client::Create failed: " << status.ToString() << std::endl;
        return false;
    }
    client->reset(raw_client);

    bcache2::client::TableOptions table_options;
    table_options.io_timeout_ms = 2000;
    table_options.connect_timeout_ms = 2000;
    bcache2::client::Table* raw_table = nullptr;
    status = (*client)->OpenTable(namespace_name, table_name, table_options, &raw_table);
    if (!status.ok()) {
        std::cerr << "OpenTable failed: " << status.ToString() << std::endl;
        return false;
    }
    table->reset(raw_table);
    return true;
}

template <typename Fn>
bool RunPhase(const std::string& phase, const std::string& metaserver, const std::string& idc,
              const std::string& namespace_name, const std::string& table_name, bool pin_primary,
              int ops, int threads, int value_bytes, Fn fn, Summary* summary) {
    std::vector<int64_t> samples(static_cast<size_t>(ops), 0);
    std::atomic<int> errors{0};
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(threads);

    auto wall_begin = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            std::unique_ptr<bcache2::client::Client> client;
            std::unique_ptr<bcache2::client::Table> table;
            if (!OpenTable(metaserver, idc, namespace_name, table_name, pin_primary, &client,
                           &table)) {
                errors.fetch_add(1);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = t; i < ops; i += threads) {
                auto begin = std::chrono::steady_clock::now();
                if (!fn(table.get(), i)) {
                    errors.fetch_add(1);
                }
                auto end = std::chrono::steady_clock::now();
                samples[static_cast<size_t>(i)] =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    wall_begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto wall_end = std::chrono::steady_clock::now();
    const int64_t total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(wall_end - wall_begin).count();

    *summary = Summarize(phase, std::move(samples), threads, value_bytes, errors.load(), total_ms);
    return summary->errors == 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 12) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [ops=20000]"
                     " [threads=32] [value_bytes=128] [pin_primary_reads=1]"
                     " [replica_wait_ms=1000] [mode=both|set|get] [set_retry_ms=0]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int ops = argc > 5 ? std::atoi(argv[5]) : 20000;
    const int threads = argc > 6 ? std::atoi(argv[6]) : 32;
    const int value_bytes = argc > 7 ? std::atoi(argv[7]) : 128;
    const bool pin_primary_reads = argc > 8 ? std::atoi(argv[8]) != 0 : true;
    const int replica_wait_ms = argc > 9 ? std::atoi(argv[9]) : 1000;
    const std::string mode = argc > 10 ? argv[10] : "both";
    const int set_retry_ms = argc > 11 ? std::atoi(argv[11]) : 0;

    if (ops <= 0 || threads <= 0 || value_bytes <= 0) {
        std::cerr << "ops, threads, and value_bytes must be positive" << std::endl;
        return 2;
    }
    if (mode != "both" && mode != "set" && mode != "get") {
        std::cerr << "mode must be one of: both, set, get" << std::endl;
        return 2;
    }

    const std::string prefix =
        "scale_" + std::to_string(std::time(nullptr)) + "_" +
        std::to_string(static_cast<unsigned long long>(std::rand()));
    const std::string value(static_cast<size_t>(value_bytes), 'x');

    Summary set_summary;
    std::atomic<int> set_errors_logged{0};
    if (!RunPhase(mode == "get" ? "seed_set" : "set", metaserver, idc, namespace_name,
                  table_name, true, ops, threads, value_bytes,
                  [&](bcache2::client::Table* table, int i) {
                      bcache2::Status status =
                          table->Set(prefix + ":" + std::to_string(i), value);
                      if (!status.ok() && set_retry_ms > 0) {
                          const auto deadline = std::chrono::steady_clock::now() +
                                                std::chrono::milliseconds(set_retry_ms);
                          do {
                              std::this_thread::sleep_for(std::chrono::milliseconds(20));
                              status = table->Set(prefix + ":" + std::to_string(i), value);
                          } while (!status.ok() && std::chrono::steady_clock::now() < deadline);
                      }
                      if (!status.ok() && set_errors_logged.fetch_add(1) < 10) {
                          std::cerr << "set failed index=" << i
                                    << " status=" << status.ToString() << std::endl;
                      }
                      return status.ok();
                  },
                  &set_summary)) {
        if (mode != "get") {
            PrintSummary(set_summary);
        }
        return 1;
    }
    if (mode != "get") {
        PrintSummary(set_summary);
    }
    if (mode == "set") {
        return 0;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(replica_wait_ms));

    Summary get_raw_summary;
    Summary get_visibility_summary;
    const int retry_ms = pin_primary_reads ? 0 : std::max(replica_wait_ms, 30000);
    if (!RunGetPhase(metaserver, idc, namespace_name, table_name, pin_primary_reads, ops, threads,
                     value_bytes, prefix, value, retry_ms, &get_raw_summary,
                     &get_visibility_summary)) {
        PrintSummary(get_raw_summary);
        PrintSummary(get_visibility_summary);
        return 1;
    }
    PrintSummary(get_raw_summary);
    PrintSummary(get_visibility_summary);
    return 0;
}
