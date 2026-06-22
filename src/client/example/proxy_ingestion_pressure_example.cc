#include <brpc/channel.h>
#include <brpc/thrift_message.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "thrift/server_types.h"

namespace bcache2 {
namespace proxy {
bool EnsureBrpcThriftProtocolRegistered();
}  // namespace proxy
}  // namespace bcache2

namespace {

struct Options {
    std::string proxy_endpoint;
    std::string namespace_name;
    std::string table_name;
    std::string key_prefix;
    int ops = 1000;
    int threads = 4;
    int value_size = 128;
    bool verify_reads = false;
    int verify_timeout_ms = 10000;
    int verify_poll_ms = 20;
    int write_retries = 3;
};

void Usage(const char* argv0) {
    std::cerr << "usage: " << argv0
              << " <proxy_host:port> <namespace> <table> <key_prefix> [ops] [threads] "
                 "[value_size] [verify_reads:0|1] [verify_timeout_ms] [verify_poll_ms] "
                 "[write_retries]"
              << std::endl;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    if (argc < 5 || argc > 12) {
        Usage(argv[0]);
        return false;
    }
    options->proxy_endpoint = argv[1];
    options->namespace_name = argv[2];
    options->table_name = argv[3];
    options->key_prefix = argv[4];
    if (argc >= 6) {
        options->ops = std::atoi(argv[5]);
    }
    if (argc >= 7) {
        options->threads = std::atoi(argv[6]);
    }
    if (argc >= 8) {
        options->value_size = std::atoi(argv[7]);
    }
    if (argc >= 9) {
        options->verify_reads = std::atoi(argv[8]) != 0;
    }
    if (argc >= 10) {
        options->verify_timeout_ms = std::atoi(argv[9]);
    }
    if (argc >= 11) {
        options->verify_poll_ms = std::atoi(argv[10]);
    }
    if (argc >= 12) {
        options->write_retries = std::atoi(argv[11]);
    }
    if (options->ops <= 0 || options->threads <= 0 || options->value_size <= 0 ||
        options->verify_timeout_ms <= 0 || options->verify_poll_ms <= 0 ||
        options->write_retries < 0) {
        Usage(argv[0]);
        return false;
    }
    return true;
}

bool InitChannel(const std::string& endpoint, brpc::Channel* channel) {
    if (!bcache2::proxy::EnsureBrpcThriftProtocolRegistered()) {
        std::cerr << "failed to register thrift protocol" << std::endl;
        return false;
    }
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_THRIFT;
    const char* timeout_env = std::getenv("PROXY_SMOKE_TIMEOUT_MS");
    options.timeout_ms = timeout_env == nullptr ? 5000 : std::atoi(timeout_env);
    if (channel->Init(endpoint.c_str(), "", &options) != 0) {
        std::cerr << "failed to init proxy channel: " << endpoint << std::endl;
        return false;
    }
    return true;
}

bool SetOne(brpc::Channel* channel, const Options& options, int op, const std::string& value,
            int* status_code) {
    bcache2::thrift::SetRequest request;
    bcache2::thrift::SetResponse response;
    request.__set_namespace_name(options.namespace_name);
    request.__set_table_name(options.table_name);
    request.__set_key(options.key_prefix + ":" + std::to_string(op));
    request.__set_value(value);

    brpc::ThriftStub stub(channel);
    brpc::Controller ctrl;
    const char* timeout_env = std::getenv("PROXY_SMOKE_TIMEOUT_MS");
    ctrl.set_timeout_ms(timeout_env == nullptr ? 5000 : std::atoi(timeout_env));
    stub.CallMethod("Set", &ctrl, &request, &response, nullptr);
    if (ctrl.Failed()) {
        *status_code = -1;
        return false;
    }
    *status_code = response.status.code;
    return response.status.code == bcache2::kOK;
}

bool SetOneWithRetry(brpc::Channel* channel, const Options& options, int op,
                     const std::string& value, int* status_code, int* retry_count) {
    for (int attempt = 0; attempt <= options.write_retries; ++attempt) {
        if (SetOne(channel, options, op, value, status_code)) {
            *retry_count += attempt;
            return true;
        }
        if (attempt < options.write_retries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20 * (attempt + 1)));
        }
    }
    *retry_count += options.write_retries;
    return false;
}

bool GetOneOnce(brpc::Channel* channel, const Options& options, int op, const std::string& expected) {
    bcache2::thrift::GetRequest request;
    bcache2::thrift::GetResponse response;
    request.__set_namespace_name(options.namespace_name);
    request.__set_table_name(options.table_name);
    request.__set_key(options.key_prefix + ":" + std::to_string(op));

    brpc::ThriftStub stub(channel);
    brpc::Controller ctrl;
    const char* timeout_env = std::getenv("PROXY_SMOKE_TIMEOUT_MS");
    ctrl.set_timeout_ms(timeout_env == nullptr ? 5000 : std::atoi(timeout_env));
    stub.CallMethod("Get", &ctrl, &request, &response, nullptr);
    return !ctrl.Failed() && response.status.code == bcache2::kOK && response.value == expected;
}

int CountMissingReads(brpc::Channel* channel, const Options& options,
                      const std::string& expected) {
    std::vector<char> seen(options.ops, 0);
    int remaining = options.ops;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(options.verify_timeout_ms);
    do {
        for (int op = 0; op < options.ops; ++op) {
            if (seen[op]) {
                continue;
            }
            if (GetOneOnce(channel, options, op, expected)) {
                seen[op] = 1;
                --remaining;
            }
        }
        if (remaining == 0) {
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(options.verify_poll_ms));
    } while (std::chrono::steady_clock::now() < deadline);
    return remaining;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        return 2;
    }

    const std::string value(options.value_size, 'v');
    std::atomic<int> next_op{0};
    std::atomic<int> ok{0};
    std::atomic<int> rpc_failed{0};
    std::atomic<int> status_failed{0};
    std::atomic<int> read_failed{0};
    std::atomic<int> first_status_code{bcache2::kOK};
    std::atomic<int> write_retry_attempts{0};

    const auto begin = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(options.threads);
    for (int t = 0; t < options.threads; ++t) {
        workers.emplace_back([&]() {
            brpc::Channel channel;
            if (!InitChannel(options.proxy_endpoint, &channel)) {
                rpc_failed.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            while (true) {
                const int op = next_op.fetch_add(1, std::memory_order_relaxed);
                if (op >= options.ops) {
                    break;
                }
                int status_code = bcache2::kOK;
                int retry_count = 0;
                if (!SetOneWithRetry(&channel, options, op, value, &status_code, &retry_count)) {
                    write_retry_attempts.fetch_add(retry_count, std::memory_order_relaxed);
                    if (status_code == -1) {
                        rpc_failed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        status_failed.fetch_add(1, std::memory_order_relaxed);
                        int expected = bcache2::kOK;
                        first_status_code.compare_exchange_strong(expected, status_code);
                    }
                    continue;
                }
                write_retry_attempts.fetch_add(retry_count, std::memory_order_relaxed);
                ok.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto writes_done = std::chrono::steady_clock::now();

    if (options.verify_reads) {
        brpc::Channel read_channel;
        if (!InitChannel(options.proxy_endpoint, &read_channel)) {
            rpc_failed.fetch_add(1, std::memory_order_relaxed);
            read_failed.store(options.ops, std::memory_order_relaxed);
        } else {
            read_failed.store(CountMissingReads(&read_channel, options, value),
                              std::memory_order_relaxed);
        }
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    const auto write_elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(writes_done - begin).count();
    const double qps =
            write_elapsed_ms == 0 ? 0.0 : static_cast<double>(ok.load()) * 1000.0 / write_elapsed_ms;

    std::cout << "proxy_ingestion_pressure" << std::endl;
    std::cout << "ops=" << options.ops << std::endl;
    std::cout << "threads=" << options.threads << std::endl;
    std::cout << "value_size=" << options.value_size << std::endl;
    std::cout << "ok=" << ok.load() << std::endl;
    std::cout << "write_failed=" << (options.ops - ok.load()) << std::endl;
    std::cout << "read_verified=" << (options.ops - read_failed.load()) << std::endl;
    std::cout << "verify_timeout_ms=" << options.verify_timeout_ms << std::endl;
    std::cout << "verify_poll_ms=" << options.verify_poll_ms << std::endl;
    std::cout << "write_retries=" << options.write_retries << std::endl;
    std::cout << "write_retry_attempts=" << write_retry_attempts.load() << std::endl;
    std::cout << "rpc_failed=" << rpc_failed.load() << std::endl;
    std::cout << "status_failed=" << status_failed.load() << std::endl;
    std::cout << "read_failed=" << read_failed.load() << std::endl;
    std::cout << "first_status_code=" << first_status_code.load() << std::endl;
    std::cout << "write_elapsed_ms=" << write_elapsed_ms << std::endl;
    std::cout << "elapsed_ms=" << elapsed_ms << std::endl;
    const double end_to_end_qps =
            elapsed_ms == 0 ? 0.0 : static_cast<double>(ok.load()) * 1000.0 / elapsed_ms;
    std::cout << "write_qps=" << qps << std::endl;
    std::cout << "end_to_end_qps=" << end_to_end_qps << std::endl;

    return ok.load() == options.ops && rpc_failed.load() == 0 && status_failed.load() == 0 &&
                   read_failed.load() == 0
               ? 0
               : 1;
}
