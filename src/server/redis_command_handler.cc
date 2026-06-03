// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/redis_command_handler.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "butil/file_util.h"
#include "butil/files/file_path.h"
#include "common/bthread_closure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "include/byte_log.h"
#include "protocol/server.pb.h"
#include "server/redis_service.h"
#include "src/common/controller.h"

namespace bcache2 {
namespace server {

RedisCommand::RedisCommand(const std::string& name, CmdType cmd_type, int artiy,
                           const std::string& sflags, int firstkey, int lastkey, int keystep)
    : name_(name),
      cmd_type_(cmd_type),
      artiy_(artiy),
      firstkey_(firstkey),
      lastkey_(lastkey),
      keystep_(keystep) {
    flags_ = 0;
    for (const auto& f : sflags) {
        switch (f) {
        case 'w':
            flags_ |= CmdFlag::kWrite;
            break;
        case 'r':
            flags_ |= CmdFlag::kReadonly;
            break;
        case 'm':
            flags_ |= CmdFlag::kDenyoom;
            break;
        case 'a':
            flags_ |= CmdFlag::kAdmin;
            break;
        case 'p':
            flags_ |= CmdFlag::kPubsub;
            break;
        case 's':
            flags_ |= CmdFlag::kNoscript;
            break;
        case 'R':
            flags_ |= CmdFlag::kRandom;
            break;
        case 'S':
            flags_ |= CmdFlag::kSortForScript;
            break;
        case 'l':
            flags_ |= CmdFlag::kLoading;
            break;
        case 't':
            flags_ |= CmdFlag::kStale;
            break;
        case 'M':
            flags_ |= CmdFlag::kSkipMonitor;
            break;
        case 'k':
            flags_ |= CmdFlag::kAsking;
            break;
        case 'F':
            flags_ |= CmdFlag::kFast;
            break;
        case 'c':
            flags_ |= CmdFlag::kPslot;
            break;
        case 'n':
            flags_ |= CmdFlag::kNoLimit;
            break;
        default:
            LOG_WARNING("Unsupported command flag");
            break;
        }
    }
}

RedisCommandHandler::RedisCommandHandler(
    const RedisCommand& command, RedisServiceImpl* redis_service,
    std::function<void(RedisCommandHandler*, RedisClientContext*)> handler)
    : command_(command),
      handler_(handler),
      partition_manager_(redis_service->GetPartitionManager()),
      redis_service_(redis_service) {}

brpc::RedisCommandHandler::Result RedisCommandHandler::Run(
    const std::vector<const char*>& raw_args, brpc::RedisReply* output, bool /*flush_batched*/) {
    std::vector<std::string> args;
    args.reserve(raw_args.size());
    for (const char* arg : raw_args) {
        args.emplace_back(arg == nullptr ? "" : arg);
    }

    if ((command_.GetArtiy() > 0 && static_cast<int>(args.size()) != command_.GetArtiy()) ||
        (command_.GetArtiy() < 0 && static_cast<int>(args.size()) < -1 * command_.GetArtiy())) {
        output->SetError("ERR wrong number of arguments for '" + command_.GetName() + "' command");
        return brpc::RedisCommandHandler::OK;
    }

    RedisClientContext client(args, output);
    if (handler_ == nullptr) {
        output->SetStatus("OK");
    } else {
        handler_(this, &client);
    }

    return brpc::RedisCommandHandler::OK;
}

void RedisCommandHandler::Ping(RedisClientContext* c) {
    if (c->ArgSize() > 2) {
        c->reply->SetError("ERR wrong number of arguments for 'ping' command");
        return;
    }

    if (c->ArgSize() == 1) {
        c->reply->SetStatus("PONG");
    } else {
        c->reply->SetString(c->StrArg(1));
    }
}

void FillConfig(Config* config, const std::string& key, const std::string& value) {
    if (key == "maxmemory") {
        config->mutable_evicter_config()->mutable_maxmemory()->set_value(atoi(value.c_str()));
        return;
    }
    config->mutable_extend_config()->insert({key, value});
}

void RedisCommandHandler::Config(RedisClientContext* c) {
    const std::string& op = c->StrArg(1);

    if (!strcasecmp(op.c_str(), "set") && c->ArgSize() == 4) {
        c->reply->SetError(
            "ERR config now is hosted by metaserver_v2, please interact with metaserver_v2 api");
        // ConfigSet(c);
        return;
    }

    if (!strcasecmp(op.c_str(), "get") && c->ArgSize() == 3) {
        const std::string& key = c->StrArg(2);
        if (!redis_service_->HasConfig(key)) {
            c->reply->SetArray(0);
            return;
        }
        c->reply->SetArray(2);
        (*c->reply)[0].SetString(key);
        (*c->reply)[1].SetString(redis_service_->GetConfig(key));
        return;
    }

    if (!strcasecmp(op.c_str(), "rewrite") && c->ArgSize() == 2) {
        c->reply->SetStatus("OK");
        return;
    }

    c->reply->SetError("ERR syntax error");
}

// deprecated since metaserver_v2
void RedisCommandHandler::ConfigSet(RedisClientContext* c) {
    const std::string& key = c->StrArg(2);
    const std::string& value = c->StrArg(3);
    redis_service_->SetConfig(key, value);

    uint64_t loaded_partition_id = redis_service_->GetLoadedPartitionId();
    if (loaded_partition_id == 0) {
        c->reply->SetStatus("OK");
        return;
    }

    SetConfigRequest request;
    request.set_partition_id(loaded_partition_id);
    FillConfig(request.mutable_config(), key, value);

    SetConfigResponse response;
    Controller ctrl;
    BTHREAD_SYNC_CALL(partition_manager_->SetConfig, &ctrl, &request, &response);
    if (!ctrl.status().ok() || response.status().code() != Code::kOK) {
        LOG_ERROR("Config set for partition failed")
            .put("RpcStatus", ctrl.status().ToString())
            .put("ResponseStatus", response.status().message())
            .put("PartitionId", request.partition_id());
        c->reply->SetStatus(
            "ERR config set error, " +
            (ctrl.status().ok() ? response.status().message() : ctrl.status().ToString()));
        return;
    }

    LOG_INFO("Config set success").put("PartitionId", request.partition_id());
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::SlaveOf(RedisClientContext* c) {
    if (c->ArgSize() < 3) {
        c->reply->SetError("ERR wrong arguments");
    }
    const std::string& master_host = c->StrArg(1);
    const std::string& master_port = c->StrArg(2);
    if (!strcasecmp(master_host.c_str(), "no") && !strcasecmp(master_port.c_str(), "one")) {
        redis_service_->SetMaster("", "");
    } else {
        redis_service_->SetMaster(master_host, master_port);
    }
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::Info(RedisClientContext* c) {
    if (c->ArgSize() > 2) {
        c->reply->SetError("ERR syntax error");
        return;
    }
    std::string section = "default";
    if (c->ArgSize() == 2) {
        section = c->StrArg(1);
    }
    int sections = 0;
    int allsections = strcasecmp(section.c_str(), "all") == 0;
    int defsections = strcasecmp(section.c_str(), "default") == 0;
    std::string info = "";

    /* Server */
    if (allsections || defsections || !strcasecmp(section.c_str(), "server")) {
        if (sections++) info += "\r\n";
        info +=
            "# Server\r\n"
            "redis_version:" BCACHE2_VERSION
            "\r\n"
            "redis_git_sha1:edea2d25\r\n"
            "redis_git_dirty:0\r\n"
            "redis_build_id:1c709a1f8c6631ca\r\n"
            "redis_mode:alchemy\r\n"
            "os:Linux 4.14.81.bm.17-amd64 x86_64\r\n"
            "arch_bits:64\r\n"
            "multiplexing_api:epoll\r\n"
            "atomicvar_api:atomic-builtin\r\n"
            "gcc_version:6.3.0\r\n"
            "process_id:809715\r\n"
            "run_id:ca386cbe41c4ed6a8f684c266a5ab84833129180\r\n"
            "tcp_port:4031\r\n"
            "uptime_in_seconds:9240013\r\n"
            "uptime_in_days:106\r\n"
            "hz:10\r\n"
            "configured_hz:10\r\n"
            "lru_clock:5840754\r\n"
            "executable:/opt/tiger/cache_manager/bin/redis-server-5.0.9.23.13\r\n"
            "config_file:/opt/tiger/cache_manager/machines/10.10.10.10/redis_fake_cluster_lf/"
            "redis.conf\r\n"
            "skip-assert:no\r\n"
            "password-mode:yes\r\n";
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section.c_str(), "clients")) {
        if (sections++) info += "\r\n";
        info +=
            "# Clients\r\n"
            "connected_clients:2\r\n"
            "client_recent_max_input_buffer:2\r\n"
            "client_recent_max_output_buffer:0\r\n"
            "blocked_clients:0\r\n";
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section.c_str(), "memory")) {
        if (sections++) info += "\r\n";
        info +=
            "# Memory\r\n"
            "used_memory:111403830\r\n"
            "used_memory_human:106.24M\r\n"
            "real_used_memory:111489928\r\n"
            "real_used_memory_human:106.33M\r\n"
            "used_memory_rss:477458432\r\n"
            "used_memory_rss_human:455.34M\r\n"
            "used_memory_peak:7447676472\r\n"
            "used_memory_peak_human:6.94G\r\n"
            "used_memory_peak_perc:1.50%\r\n"
            "used_memory_overhead:87294650\r\n"
            "used_memory_startup:67781744\r\n"
            "used_memory_dataset:24195278\r\n"
            "used_memory_dataset_perc:55.36%\r\n"
            "allocator_allocated:112083776\r\n"
            "allocator_active:387379200\r\n"
            "allocator_resident:489570304\r\n"
            "total_system_memory:1081326800896\r\n"
            "total_system_memory_human:1007.06G\r\n"
            "used_memory_lua:44032\r\n"
            "used_memory_lua_human:43.00K\r\n"
            "used_memory_scripts:688\r\n"
            "used_memory_scripts_human:688B\r\n"
            "number_of_cached_scripts:2\r\n"
            "maxmemory:10485760000\r\n"
            "maxmemory_human:9.77G\r\n"
            "maxmemory_policy:noeviction\r\n"
            "allocator_frag_ratio:3.46\r\n"
            "allocator_frag_bytes:275295424\r\n"
            "allocator_rss_ratio:1.26\r\n"
            "allocator_rss_bytes:102191104\r\n"
            "rss_overhead_ratio:0.98\r\n"
            "rss_overhead_bytes:-12111872\r\n"
            "mem_fragmentation_ratio:4.28\r\n"
            "mem_fragmentation_bytes:366011008\r\n"
            "mem_not_counted_for_evict:2296\r\n"
            "mem_replication_backlog:0\r\n"
            "mem_clients_slaves:0\r\n"
            "mem_clients_normal:83802\r\n"
            "mem_aof_buffer:0\r\n"
            "mem_allocator:jemalloc-5.1.0\r\n"
            "active_defrag_running:0\r\n"
            "lazyfree_pending_objects:0\r\n";
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section.c_str(), "persistence")) {
        if (sections++) info += "\r\n";
        info +=
            "# Persistence\r\n"
            "loading:0\r\n"
            "rdb_changes_since_last_save:2871331\r\n"
            "rdb_bgsave_in_progress:0\r\n"
            "rdb_last_save_time:1649943642\r\n"
            "rdb_last_bgsave_status:ok\r\n"
            "rdb_last_bgsave_time_sec:0\r\n"
            "rdb_current_bgsave_time_sec:-1\r\n"
            "rdb_last_cow_size:6242304\r\n"
            "aof_enabled:1\r\n"
            "aof_rewrite_in_progress:0\r\n"
            "aof_rewrite_scheduled:0\r\n"
            "aof_last_rewrite_time_sec:-1\r\n"
            "aof_current_rewrite_time_sec:-1\r\n"
            "aof_last_bgrewrite_status:ok\r\n"
            "aof_last_write_status:ok\r\n"
            "aof_last_cow_size:0\r\n"
            "aof_buf_unit:0\r\n"
            "aof_current_size:0\r\n"
            "aof_base_size:0\r\n"
            "aof_pending_rewrite:0\r\n"
            "aof_buffer_length:0\r\n"
            "aof_rewrite_buffer_length:0\r\n"
            "aof_pending_bio_fsync:0\r\n"
            "aof_delayed_fsync:0\r\n"
            "last_cron_bgsave_mem_use:250232480\r\n"
            "aof_inc_from_last_cron_bgsave:346454114\r\n"
            "last_cron_bgsave_mem_use:250232480\r\n"
            "aof_inc_from_last_cron_bgsave:346454114\r\n";
    }

    /* Stats */
    auto partition_stats_map = partition_manager_->GetPartitionLoadedStatus();
    std::string partition_loading_stats = "";
    if (partition_stats_map.empty()) {
        partition_loading_stats = "not_exist";
    } else {
        if (partition_stats_map.begin()->second) {
            partition_loading_stats = "loaded";
        } else {
            partition_loading_stats = "loading";
        }
    }
    if (allsections || defsections || !strcasecmp(section.c_str(), "stats")) {
        if (sections++) info += "\r\n";
        info +=
            "# Stats\r\n"
            "partition_loading_stats:" +
            partition_loading_stats +
            "\r\n"
            "total_connections_received:982455\r\n"
            "total_commands_processed:10909755651\r\n"
            "total_commands_dropped:0\r\n"
            "instantaneous_ops_per_sec:4\r\n"
            "instantaneous_write_ops_per_sec:0\r\n"
            "instantaneous_readonly_ops_per_sec:1\r\n"
            "instantaneous_write_rx_kbps:0.00\r\n"
            "instantaneous_write_tx_kbps:0.00\r\n"
            "instantaneous_readonly_rx_kbps:0.06\r\n"
            "instantaneous_readonly_tx_kbps:0.08\r\n"
            "instantaneous_total_write_rx_kbps:0.00\r\n"
            "instantaneous_total_read_rx_kbps:0.07\r\n"
            "instantaneous_dropped_ops_per_sec:0\r\n"
            "total_net_input_bytes:1580090573630\r\n"
            "total_net_output_bytes:34411680475\r\n"
            "instantaneous_input_kbps:0.35\r\n"
            "instantaneous_output_kbps:0.26\r\n"
            "rejected_connections:0\r\n"
            "sync_full:0\r\n"
            "sync_partial_ok:1672\r\n"
            "sync_partial_err:0\r\n"
            "expired_keys:0\r\n"
            "expired_stale_perc:0.00\r\n"
            "expired_time_cap_reached_count:0\r\n"
            "evicted_keys:0\r\n"
            "keyspace_hits:1351721292\r\n"
            "keyspace_misses:290257590\r\n"
            "pubsub_channels:0\r\n"
            "pubsub_patterns:0\r\n"
            "latest_fork_usec:40694\r\n"
            "migrate_cached_sockets:0\r\n"
            "slave_expires_tracked_keys:0\r\n"
            "active_defrag_hits:0\r\n"
            "active_defrag_misses:0\r\n"
            "active_defrag_key_hits:0\r\n"
            "active_defrag_key_misses:0\r\n"
            "max_qps:0\r\n";
    }

    /* AofBioStats */
    if (allsections || defsections || !strcasecmp(section.c_str(), "aofbiostats")) {
        if (sections++) info += "\r\n";
        info +=
            "# AofBioStats\r\n"
            "bio_aof_file_num:0\r\n"
            "bio_aof_queue_buf_len:0\r\n"
            "bio_flow_ctrl_duration:0\r\n"
            "bio_flow_ctrl_times:0\r\n"
            "aof_overburdened_level:0\r\n"
            "aof_overburdened_times:0\r\n"
            "bio_handling:0\r\n"
            "req_queuing_time_us:0\r\n"
            "req_write_time_us:0\r\n"
            "req_fsync_time_us:0\r\n"
            "queue_clearance_time_us:0\r\n";
    }

    /* Replication */
    if (allsections || defsections || !strcasecmp(section.c_str(), "replication")) {
        auto master = redis_service_->GetMaster();
        if (sections++) info += "\r\n";
        info += "# Replication\r\n";
        if (master.first == "") {
            info += "role:master\r\n";
        } else {
            info +=
                "role:slave\r\n"
                "master_host:" +
                master.first +
                "\r\n"
                "master_port:" +
                master.second +
                "\r\n"
                "master_link_status:up\r\n"
                "master_last_io_seconds_ago:0\r\n"
                "master_sync_in_progress:0\r\n"
                "slave_repl_offset:1455495201731\r\n"
                "slave_last_fullresync_in_progress:0\r\n"
                "slave_priority:100\r\n"
                "slave_read_only:1\r\n";
        }
        info +=
            "connected_slaves:0\r\n"
            "master_replid:62a9d95a5ae2fa44cc08d7ca7cdef5462e67d554\r\n"
            "master_replid2:8cb45ce9e7560213740cde4ffd765b2bb7ff2e54\r\n"
            "master_repl_offset:1455495201731\r\n"
            "second_repl_offset:1449584037345\r\n"
            "repl_backlog_active:1\r\n"
            "repl_backlog_size:104857600\r\n"
            "repl_backlog_first_byte_offset:1455390344132\r\n"
            "repl_backlog_histlen:104857600\r\n"
            "repl_backlog_opid:[5780249895, 5780629338]\r\n"
            "aof_psyncing_state:0\r\n"
            "aof_psync_reading_filename:NULL\r\n"
            "aof_psync_reading_offset:-1\r\n"
            "next_opid:5780629339\r\n"
            "second_replid_opid:5759112791\r\n";
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section.c_str(), "cpu")) {
        if (sections++) info += "\r\n";
        info +=
            "# CPU\r\n"
            "used_cpu_sys:64763.726124\r\n"
            "used_cpu_user:119503.961632\r\n"
            "used_cpu_sys_process:91286.91\r\n"
            "used_cpu_user_process:134374.58\r\n"
            "used_cpu_sys_children:751.430393\r\n"
            "used_cpu_user_children:8586.484241\r\n"
            "cpu_usage:3%\r\n"
            "cpu_usage_process:0%\r\n";
    }

    /* Bigkeys */
    if (allsections || defsections || !strcasecmp(section.c_str(), "bigkeys")) {
        if (sections++) info += "\r\n";
        info +=
            "# Bigkeys\r\n"
            "big_keys_switch:0\r\n"
            "big_key_string_len:10000\r\n"
            "big_key_fields_num:5000\r\n"
            "big_key_fields_len:10485760\r\n"
            "big_keys_top_num:100\r\n"
            "big_keys_scan_num:10\r\n"
            "big_keys_scan_cursor:0\r\n"
            "big_keys_last_count_time_sec:0\r\n"
            "big_keys_count_start_time:0\r\n";
    }

    /* Hotkeys */
    if (allsections || defsections || !strcasecmp(section.c_str(), "hotkeys")) {
        if (sections++) info += "\r\n";
        info +=
            "# Hotkeys\r\n"
            "hotkeys_read_keys_num:0\r\n"
            "hotkeys_write_keys_num:0\r\n"
            "hotkeys_black_read_keys_num:0\r\n"
            "hotkeys_black_write_keys_num:0\r\n";
    }

    /* Cluster */
    if (allsections || defsections || !strcasecmp(section.c_str(), "cluster")) {
        if (sections++) info += "\r\n";
        info +=
            "# Cluster\r\n"
            "cluster_enabled:0\r\n";
    }

    /* KeySpace */
    if (allsections || defsections || !strcasecmp(section.c_str(), "keyspace")) {
        if (sections++) info += "\r\n";
        info +=
            "# Keyspace\r\n"
            "db0:keys=172469,expires=172448,avg_ttl=0\r\n";
    }

    c->reply->SetString(info);
}

// Load or unload a partition
void RedisCommandHandler::Partition(RedisClientContext* c) {
    if (c->ArgSize() < 3) {
        c->reply->SetError("ERR wrong arguments");
        return;
    }

    const std::string& op = c->StrArg(1);

    try {
        if (!strcasecmp(op.c_str(), "load") && (c->ArgSize() == 8 || c->ArgSize() == 9)) {
            PartitionLoad(c);
            return;
        }
        if (op == "unload" && c->ArgSize() == 3) {
            PartitionUnload(c);
            return;
        }
    } catch (std::exception& e) {
        c->reply->SetError("ERR param type error");
        return;
    }

    c->reply->SetError("ERR syntax error");
}

void RedisCommandHandler::PartitionLoad(RedisClientContext* c) {
    uint64_t load_version = c->IntArg(2);
    uint64_t partition_id = c->IntArg(3);
    const std::string& partition_uri = c->StrArg(4);
    uint64_t start_slot = c->IntArg(5);
    uint64_t end_slot = c->IntArg(6);
    const std::string& role = c->StrArg(7);
    bool async = false;
    if (c->ArgSize() == 9 && c->StrArg(8) == "async") {
        async = true;
    }

    LOG_INFO("Loading partition")
        .put("LoadVersion", load_version)
        .put("PartitionId", partition_id)
        .put("PartitionUri", partition_uri)
        .put("StartSlot", start_slot)
        .put("EndSlot", end_slot)
        .put("Role", role);

    if (role == "slave") {
        c->reply->SetStatus("OK");
        return;
    }

    if (partition_uri.find("local://") != std::string::npos) {
        const std::string& temp_dir = partition_uri.substr(
            strlen("local://"), partition_uri.size() - std::strlen("local://"));
        bool ret = butil::CreateDirectory(butil::FilePath(temp_dir), true);
        BYTE_ASSERT_TRUE(ret);
    }

    LoadRequest request;
    request.set_load_version(load_version);
    request.set_partition_id(partition_id);
    request.set_partition_uri(partition_uri);
    request.set_start_slot(start_slot);
    request.set_end_slot(end_slot);
    request.set_sync(!async);
    for (auto config_pair : redis_service_->GetConfigMap()) {
        FillConfig(request.mutable_config(), config_pair.first, config_pair.second);
    }

    LoadResponse response;
    Controller ctrl;
    BTHREAD_SYNC_CALL(partition_manager_->Load, &ctrl, &request, &response);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Load partition failed")
            .put("Error", ctrl.status().ToString())
            .put("PartitionId", request.partition_id())
            .put("PartitionUri", request.partition_uri());
        c->reply->SetStatus("ERR load paritition error, " + ctrl.status().ToString());
        return;
    }

    LOG_INFO("Load partition success")
        .put("PartitionId", request.partition_id())
        .put("PartitionUri", request.partition_uri());
    redis_service_->SetLoadedPartitionId(request.partition_id());
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::PartitionUnload(RedisClientContext* c) {
    uint64_t partition_id = c->IntArg(2);

    UnloadRequest request;
    request.set_partition_id(partition_id);

    UnloadResponse response;
    Controller ctrl;
    BTHREAD_SYNC_CALL(partition_manager_->Unload, &ctrl, &request, &response);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Unload partition failed")
            .put("Error", ctrl.status().ToString())
            .put("PartitionId", request.partition_id());
        c->reply->SetStatus("ERR unload paritition error, " +
                                          ctrl.status().ToString());
        return;
    }

    LOG_INFO("Unload partition success").put("PartitionId", request.partition_id());
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::Auth(RedisClientContext* c) {
    const std::string& password = c->StrArg(1);
    if (password != redis_service_->GetConfig("requirepass")) {
        c->reply->SetError("ERR invalid password");
        return;
    }

    c->SetGdprAuth(true);
    c->reply->SetStatus("OK");
}

}  // namespace server
}  // namespace bcache2
