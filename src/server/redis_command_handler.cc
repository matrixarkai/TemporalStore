// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/redis_command_handler.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <memory>
#include <pthread.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <strings.h>

#include "butil/arena.h"
#include "bthread/countdown_event.h"
#include "butil/file_util.h"
#include "butil/files/file_path.h"
#include "common/bthread_closure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "include/byte_log.h"
#include "extension/common/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/set/interface.pb.h"
#include "extension/string/interface.pb.h"
#include "extension/modules.pb.h"
#include "protocol/server.pb.h"
#include "server/redis_service.h"
#include "src/common/controller.h"

namespace bcache2 {
namespace server {

namespace {

constexpr uint64_t kRedisTraceSeed = 0x7265646973ULL;
pthread_mutex_t g_redis_backend_mu = PTHREAD_MUTEX_INITIALIZER;

class PthreadMutexGuard {
 public:
    explicit PthreadMutexGuard(pthread_mutex_t* mu) : mu_(mu) { pthread_mutex_lock(mu_); }
    ~PthreadMutexGuard() { pthread_mutex_unlock(mu_); }

 private:
    pthread_mutex_t* mu_;
};

bool IsOkStatus(const RpcStatus& status) {
    return status.code() == Code::kOK;
}

void SetRedisError(brpc::RedisReply* reply, const std::string& message) {
    reply->SetError("ERR " + message);
}

void SetRedisStatusError(brpc::RedisReply* reply, const RpcStatus& status) {
    if (status.code() == Code::kNotFound) {
        reply->SetNullString();
        return;
    }
    SetRedisError(reply, status.message().empty() ? "command failed" : status.message());
}



class RedisSyncClosure : public Closure<void> {
 public:
    void Run() override { done_.signal(); }
    bool IsSelfDelete() const override { return false; }
    bool WaitForMs(int64_t timeout_ms) {
        timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += timeout_ms / 1000;
        deadline.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (deadline.tv_nsec >= 1000000000) {
            deadline.tv_sec += 1;
            deadline.tv_nsec -= 1000000000;
        }
        return done_.timed_wait(deadline) == 0;
    }

 private:
    bthread::CountdownEvent done_{1};
};

bool ExecuteRedisBatch(RedisServiceImpl* redis_service, const std::vector<CmdRequest>& commands,
                       BatchExecuteCmdResponse* response, brpc::RedisReply* reply) {
    PthreadMutexGuard backend_lock(&g_redis_backend_mu);
    uint64_t partition_id = redis_service->GetLoadedPartitionId();
    if (partition_id == 0) {
        SetRedisError(reply, "no partition loaded for Redis command serving");
        return false;
    }
    BatchExecuteCmdRequest request;
    request.mutable_opt()->set_trace_id(kRedisTraceSeed ^ butil::fast_rand());
    request.set_partition_id(partition_id);
    request.set_load_version(redis_service->GetLoadedPartitionLoadVersion());
    for (const auto& cmd : commands) {
        *request.add_request() = cmd;
    }

    Controller ctrl;
    RedisSyncClosure* sync = new RedisSyncClosure();
    redis_service->GetPartitionManager()->BatchExecuteCmdLocally(&ctrl, &request, response, sync);
    if (!sync->WaitForMs(redis_service->GetCommandTimeoutMs())) {
        // The backend may still complete later and signal this closure, so intentionally keep it
        // alive after timeout instead of returning a dangling stack callback.
        LOG_WARNING("Redis backend batch timed out")
            .put("PartitionId", request.partition_id())
            .put("LoadVersion", request.load_version())
            .put("RequestSize", request.request_size())
            .put("TraceId", request.opt().trace_id())
            .put("TimeoutMs", redis_service->GetCommandTimeoutMs());
        SetRedisError(reply, "Redis backend command timed out");
        return false;
    }
    delete sync;
    if (!ctrl.status().ok()) {
        SetRedisError(reply, ctrl.status().ToString());
        return false;
    }
    if (!IsOkStatus(response->status())) {
        SetRedisStatusError(reply, response->status());
        return false;
    }
    if (response->response_size() != static_cast<int>(commands.size())) {
        SetRedisError(reply, "partition response size mismatch");
        return false;
    }
    return true;
}

bool ExecuteRedisSingle(RedisServiceImpl* redis_service, const CmdRequest& command,
                        CmdResponse* cmd_response, brpc::RedisReply* reply) {
    BatchExecuteCmdResponse batch_response;
    if (!ExecuteRedisBatch(redis_service, {command}, &batch_response, reply)) {
        return false;
    }
    *cmd_response = batch_response.response(0);
    return true;
}

template <typename Request>
CmdRequest ModuleCmd(Module module, uint32_t function_id, const Request& request) {
    CmdRequest cmd;
    cmd.set_module_id(module);
    cmd.set_function_id(function_id);
    request.SerializeToString(cmd.mutable_request_bytes());
    return cmd;
}

CmdRequest StringSetCmd(const std::string& key, const std::string& value, bool nx = false,
                        bool xx = false) {
    str2::SetRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_nx_flag(nx);
    request.set_xx_flag(xx);
    return ModuleCmd(Module::STRING, str2::SET, request);
}

CmdRequest StringSetExCmd(const std::string& key, const std::string& value, uint64_t ttl_ms) {
    str2::SetexRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_ttl_ms(ttl_ms);
    return ModuleCmd(Module::STRING, str2::SETEX, request);
}

CmdRequest StringGetCmd(const std::string& key) {
    str2::GetRequest request;
    request.set_key(key);
    return ModuleCmd(Module::STRING, str2::GET, request);
}

CmdRequest StringAppendCmd(const std::string& key, const std::string& value) {
    str2::AppendRequest request;
    request.set_key(key);
    request.set_value(value);
    return ModuleCmd(Module::STRING, str2::APPEND, request);
}

CmdRequest StringStrlenCmd(const std::string& key) {
    str2::StrlenRequest request;
    request.set_key(key);
    return ModuleCmd(Module::STRING, str2::STRLEN, request);
}

CmdRequest StringIncrByCmd(const std::string& key, int64_t increment) {
    str2::IncrByRequest request;
    request.set_key(key);
    request.set_increment(increment);
    return ModuleCmd(Module::STRING, str2::INCRBY, request);
}

CmdRequest DelCmd(const std::string& key) {
    common2::DelObjectRequest request;
    request.set_key(key);
    return ModuleCmd(Module::COMMON, common2::DEL_OBJECT, request);
}

CmdRequest ExistsCmd(const std::string& key) {
    common2::ExistsRequest request;
    request.set_key(key);
    return ModuleCmd(Module::COMMON, common2::EXISTS, request);
}

CmdRequest ExpireCmd(const std::string& key, uint64_t ttl_ms) {
    common2::ExpireRequest request;
    request.set_key(key);
    request.set_ttl_ms(ttl_ms);
    return ModuleCmd(Module::COMMON, common2::EXPIRE, request);
}

CmdRequest TtlCmd(const std::string& key) {
    common2::TtlRequest request;
    request.set_key(key);
    return ModuleCmd(Module::COMMON, common2::TTL, request);
}

CmdRequest PersistCmd(const std::string& key) {
    common2::PersistRequest request;
    request.set_key(key);
    return ModuleCmd(Module::COMMON, common2::PERSIST, request);
}

CmdRequest HashSetCmd(const std::string& key, const std::string& field, const std::string& value) {
    hash2::SetRequest request;
    request.set_key(key);
    request.set_field(field);
    request.set_value(value);
    return ModuleCmd(Module::HASH, hash2::SET, request);
}

CmdRequest HashGetExtCmd(const std::string& key, const std::string& field) {
    hash2::GetRequest request;
    request.set_key(key);
    request.set_field(field);
    return ModuleCmd(Module::HASH, hash2::GET, request);
}

CmdRequest HashDelCmd(const std::string& key, const std::string& field) {
    hash2::DelRequest request;
    request.set_key(key);
    request.set_field(field);
    return ModuleCmd(Module::HASH, hash2::DEL, request);
}

CmdRequest HashLenCmd(const std::string& key) {
    hash2::LenRequest request;
    request.set_key(key);
    return ModuleCmd(Module::HASH, hash2::LEN, request);
}

CmdRequest HashGetAllCmd(const std::string& key) {
    hash2::GetAllRequest request;
    request.set_key(key);
    return ModuleCmd(Module::HASH, hash2::GETALL, request);
}

CmdRequest HashIncrByCmd(const std::string& key, const std::string& field, int64_t delta) {
    hash2::IncrByRequest request;
    request.set_key(key);
    request.set_field(field);
    request.set_increment(delta);
    return ModuleCmd(Module::HASH, hash2::INCRBY, request);
}

CmdRequest SetAddCmd(const std::string& key, const std::vector<std::string>& members) {
    set::SAddRequest request;
    request.set_key(key);
    for (const auto& member : members) {
        request.add_members(member);
    }
    return ModuleCmd(Module::SET, set::SADD, request);
}

CmdRequest SetRemCmd(const std::string& key, const std::vector<std::string>& members) {
    set::SRemRequest request;
    request.set_key(key);
    for (const auto& member : members) {
        request.add_members(member);
    }
    return ModuleCmd(Module::SET, set::SREM, request);
}

CmdRequest SetMembersCmd(const std::string& key) {
    set::SMembersRequest request;
    request.set_key(key);
    return ModuleCmd(Module::SET, set::SMEMBERS, request);
}

CmdRequest SetCardCmd(const std::string& key) {
    set::SCardRequest request;
    request.set_key(key);
    return ModuleCmd(Module::SET, set::SCARD, request);
}

CmdRequest SetIsMemberCmd(const std::string& key, const std::string& member) {
    set::SIsMemberRequest request;
    request.set_key(key);
    request.set_member(member);
    return ModuleCmd(Module::SET, set::SISMEMBER, request);
}

bool ParseStringGetResponse(const CmdResponse& response, str2::GetResponse* parsed,
                            brpc::RedisReply* reply) {
    if (parsed->ParseFromString(response.response_bytes())) {
        return true;
    }
    SetRedisError(reply, "failed to parse GET response");
    return false;
}

bool ParseHashGetResponse(const CmdResponse& response, hash2::GetResponse* parsed,
                          brpc::RedisReply* reply) {
    if (parsed->ParseFromString(response.response_bytes())) {
        return true;
    }
    SetRedisError(reply, "failed to parse HGET response");
    return false;
}

bool ParseInt64Arg(const std::string& arg, int64_t* value) {
    try {
        size_t pos = 0;
        long long parsed = std::stoll(arg, &pos, 10);
        if (pos != arg.size()) {
            return false;
        }
        *value = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDoubleArg(const std::string& arg, double* value) {
    char* end = nullptr;
    errno = 0;
    double parsed = std::strtod(arg.c_str(), &end);
    if (end == arg.c_str() || *end != '\0' || errno == ERANGE || !std::isfinite(parsed)) {
        return false;
    }
    *value = parsed;
    return true;
}

std::string FormatDouble(double value) {
    std::ostringstream out;
    out.precision(17);
    out << value;
    return out.str();
}

bool ReadLine(const std::string& raw, size_t* pos, std::string* line) {
    size_t end = raw.find('\n', *pos);
    if (end == std::string::npos) {
        return false;
    }
    *line = raw.substr(*pos, end - *pos);
    *pos = end + 1;
    return true;
}

std::string EncodeStringVector(const std::string& prefix, const std::vector<std::string>& values) {
    std::string out = prefix;
    out.append(std::to_string(values.size())).append("\n");
    for (const auto& value : values) {
        out.append(std::to_string(value.size())).append("\n");
        out.append(value).append("\n");
    }
    return out;
}

bool DecodeStringVector(const std::string& raw, const std::string& prefix,
                        std::vector<std::string>* values) {
    if (raw.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    size_t pos = prefix.size();
    std::string line;
    if (!ReadLine(raw, &pos, &line)) {
        return false;
    }
    size_t count = 0;
    try {
        count = static_cast<size_t>(std::stoull(line));
    } catch (...) {
        return false;
    }
    values->clear();
    values->reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (!ReadLine(raw, &pos, &line)) {
            return false;
        }
        size_t len = 0;
        try {
            len = static_cast<size_t>(std::stoull(line));
        } catch (...) {
            return false;
        }
        if (raw.size() < pos + len || raw.size() == pos + len || raw[pos + len] != '\n') {
            return false;
        }
        values->emplace_back(raw.data() + pos, len);
        pos += len + 1;
    }
    return pos == raw.size();
}

const std::string& ListPrefix() {
    static const std::string prefix = "TSREDIS_LIST_V1\n";
    return prefix;
}

bool LoadEncodedList(RedisServiceImpl* redis_service, const std::string& key,
                     std::vector<std::string>* values, brpc::RedisReply* reply) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service, StringGetCmd(key), &response, reply)) {
        return false;
    }
    if (!IsOkStatus(response.response_status())) {
        values->clear();
        return true;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(response, &get_response, reply)) {
        return false;
    }
    if (!DecodeStringVector(get_response.value(), ListPrefix(), values)) {
        SetRedisError(reply, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return false;
    }
    return true;
}

bool SaveEncodedList(RedisServiceImpl* redis_service, const std::string& key,
                     const std::vector<std::string>& values, brpc::RedisReply* reply) {
    CmdResponse response;
    CmdRequest command = values.empty() ? DelCmd(key) : StringSetCmd(key, EncodeStringVector(ListPrefix(), values));
    if (!ExecuteRedisSingle(redis_service, command, &response, reply)) {
        return false;
    }
    if (!IsOkStatus(response.response_status()) && !values.empty()) {
        SetRedisStatusError(reply, response.response_status());
        return false;
    }
    return true;
}

int64_t NormalizeIndex(int64_t index, int64_t size) {
    return index < 0 ? size + index : index;
}

std::pair<int64_t, int64_t> NormalizeRange(int64_t start, int64_t stop, int64_t size) {
    start = NormalizeIndex(start, size);
    stop = NormalizeIndex(stop, size);
    if (start < 0) start = 0;
    if (stop >= size) stop = size - 1;
    return {start, stop};
}

struct ZItem {
    std::string member;
    double score = 0;
    std::string score_text;
};

const std::string& ZSetPrefix() {
    static const std::string prefix = "TSREDIS_ZSET_V1\n";
    return prefix;
}

std::string EncodeZSet(const std::vector<ZItem>& items) {
    std::vector<std::string> values;
    values.reserve(items.size() * 2);
    for (const auto& item : items) {
        values.emplace_back(item.member);
        values.emplace_back(item.score_text);
    }
    return EncodeStringVector(ZSetPrefix(), values);
}

bool DecodeZSet(const std::string& raw, std::vector<ZItem>* items) {
    std::vector<std::string> values;
    if (!DecodeStringVector(raw, ZSetPrefix(), &values) || values.size() % 2 != 0) {
        return false;
    }
    items->clear();
    items->reserve(values.size() / 2);
    for (size_t i = 0; i + 1 < values.size(); i += 2) {
        double score = 0;
        if (!ParseDoubleArg(values[i + 1], &score)) {
            return false;
        }
        items->push_back({values[i], score, values[i + 1]});
    }
    return true;
}

bool LoadEncodedZSet(RedisServiceImpl* redis_service, const std::string& key,
                     std::vector<ZItem>* items, brpc::RedisReply* reply) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service, StringGetCmd(key), &response, reply)) {
        return false;
    }
    if (!IsOkStatus(response.response_status())) {
        items->clear();
        return true;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(response, &get_response, reply)) {
        return false;
    }
    if (!DecodeZSet(get_response.value(), items)) {
        SetRedisError(reply, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return false;
    }
    return true;
}

bool SaveEncodedZSet(RedisServiceImpl* redis_service, const std::string& key,
                     const std::vector<ZItem>& items, brpc::RedisReply* reply) {
    CmdResponse response;
    CmdRequest command = items.empty() ? DelCmd(key) : StringSetCmd(key, EncodeZSet(items));
    if (!ExecuteRedisSingle(redis_service, command, &response, reply)) {
        return false;
    }
    if (!IsOkStatus(response.response_status()) && !items.empty()) {
        SetRedisStatusError(reply, response.response_status());
        return false;
    }
    return true;
}

void SortZSet(std::vector<ZItem>* items, bool reverse = false) {
    std::sort(items->begin(), items->end(), [](const ZItem& lhs, const ZItem& rhs) {
        if (lhs.score != rhs.score) {
            return lhs.score < rhs.score;
        }
        return lhs.member < rhs.member;
    });
    if (reverse) {
        std::reverse(items->begin(), items->end());
    }
}

int FindZMember(const std::vector<ZItem>& items, const std::string& member) {
    for (size_t i = 0; i < items.size(); ++i) {
        if (items[i].member == member) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool ParseZRangeBound(const std::string& raw, double* value) {
    if (!strcasecmp(raw.c_str(), "-inf")) {
        *value = -INFINITY;
        return true;
    }
    if (!strcasecmp(raw.c_str(), "+inf") || !strcasecmp(raw.c_str(), "inf")) {
        *value = INFINITY;
        return true;
    }
    return ParseDoubleArg(raw, value);
}

bool WithScoresArg(RedisClientContext* c, size_t index) {
    return c->ArgSize() > index && !strcasecmp(c->StrArg(index).c_str(), "withscores");
}

bool ParseLimitArgs(RedisClientContext* c, size_t index, int64_t* offset, int64_t* count,
                    brpc::RedisReply* reply) {
    *offset = 0;
    *count = LLONG_MAX;
    if (c->ArgSize() <= index) {
        return true;
    }
    if (c->ArgSize() != index + 3 || strcasecmp(c->StrArg(index).c_str(), "limit")) {
        reply->SetError("ERR syntax error");
        return false;
    }
    if (!ParseInt64Arg(c->StrArg(index + 1), offset) ||
        !ParseInt64Arg(c->StrArg(index + 2), count)) {
        reply->SetError("ERR value is not an integer or out of range");
        return false;
    }
    if (*offset < 0) {
        *offset = 0;
    }
    return true;
}

}  // namespace


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

brpc::RedisCommandHandlerResult RedisCommandHandler::Run(
    const std::vector<butil::StringPiece>& raw_args, brpc::RedisReply* output,
    bool /*flush_batched*/) {
    std::vector<std::string> args;
    args.reserve(raw_args.size());
    for (const auto& arg : raw_args) {
        args.emplace_back(arg.data(), arg.size());
    }

    if ((command_.GetArtiy() > 0 && static_cast<int>(args.size()) != command_.GetArtiy()) ||
        (command_.GetArtiy() < 0 && static_cast<int>(args.size()) < -1 * command_.GetArtiy())) {
        output->SetError("ERR wrong number of arguments for '" + command_.GetName() + "' command");
        return brpc::REDIS_CMD_HANDLED;
    }

    RedisClientContext client(args, output);
    if (handler_ == nullptr) {
        Unsupported(&client);
    } else {
        handler_(this, &client);
    }

    return brpc::REDIS_CMD_HANDLED;
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

void RedisCommandHandler::Echo(RedisClientContext* c) {
    c->reply->SetString(c->StrArg(1));
}

void RedisCommandHandler::Quit(RedisClientContext* c) {
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::Client(RedisClientContext* c) {
    const std::string op = c->StrArg(1);
    if (!strcasecmp(op.c_str(), "setname") && c->ArgSize() == 3) {
        c->reply->SetStatus("OK");
        return;
    }
    if (!strcasecmp(op.c_str(), "getname") && c->ArgSize() == 2) {
        c->reply->SetNullString();
        return;
    }
    if (!strcasecmp(op.c_str(), "id") && c->ArgSize() == 2) {
        c->reply->SetInteger(0);
        return;
    }
    c->reply->SetError("ERR unsupported CLIENT subcommand");
}

void RedisCommandHandler::Command(RedisClientContext* c) {
    if (c->ArgSize() == 1) {
        c->reply->SetArray(0);
        return;
    }
    const std::string op = c->StrArg(1);
    if (!strcasecmp(op.c_str(), "count") && c->ArgSize() == 2) {
        c->reply->SetInteger(0);
        return;
    }
    if (!strcasecmp(op.c_str(), "docs") && c->ArgSize() >= 2) {
        c->reply->SetArray(0);
        return;
    }
    if (!strcasecmp(op.c_str(), "info") && c->ArgSize() >= 2) {
        c->reply->SetArray(0);
        return;
    }
    c->reply->SetError("ERR unsupported COMMAND subcommand");
}

void RedisCommandHandler::Select(RedisClientContext* c) {
    int64_t db = 0;
    if (!ParseInt64Arg(c->StrArg(1), &db) || db < 0) {
        c->reply->SetError("ERR invalid DB index");
        return;
    }
    if (db != 0) {
        c->reply->SetError("ERR SELECT is only supported for DB 0 by the native Redis bridge");
        return;
    }
    c->reply->SetStatus("OK");
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
        c->reply->SetError("ERR CONFIG REWRITE is not supported by the native Redis bridge yet");
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
    c->reply->SetError("ERR SLAVEOF is not supported by the native Redis bridge yet");
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
    redis_service_->SetLoadedPartitionLoadVersion(request.load_version());
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

void RedisCommandHandler::Type(RedisClientContext* c) {
    const std::string key = c->StrArg(1);

    CmdResponse string_response;
    butil::Arena scratch_arena;
    brpc::RedisReply scratch(&scratch_arena);
    if (!ExecuteRedisSingle(redis_service_, StringGetCmd(key), &string_response, &scratch)) {
        c->reply->SetError("ERR failed to inspect key type");
        return;
    }
    if (IsOkStatus(string_response.response_status())) {
        str2::GetResponse get_response;
        if (!ParseStringGetResponse(string_response, &get_response, c->reply)) {
            return;
        }
        std::vector<std::string> list_values;
        std::vector<ZItem> zset_items;
        if (DecodeStringVector(get_response.value(), ListPrefix(), &list_values)) {
            c->reply->SetStatus("list");
            return;
        }
        if (DecodeZSet(get_response.value(), &zset_items)) {
            c->reply->SetStatus("zset");
            return;
        }
        c->reply->SetStatus("string");
        return;
    }

    CmdResponse hash_response;
    butil::Arena hash_scratch_arena;
    brpc::RedisReply hash_scratch(&hash_scratch_arena);
    if (ExecuteRedisSingle(redis_service_, HashLenCmd(key), &hash_response, &hash_scratch) &&
        IsOkStatus(hash_response.response_status())) {
        hash2::LenResponse len_response;
        if (len_response.ParseFromString(hash_response.response_bytes()) && len_response.len() > 0) {
            c->reply->SetStatus("hash");
            return;
        }
    }

    CmdResponse set_response;
    butil::Arena set_scratch_arena;
    brpc::RedisReply set_scratch(&set_scratch_arena);
    if (ExecuteRedisSingle(redis_service_, SetCardCmd(key), &set_response, &set_scratch) &&
        IsOkStatus(set_response.response_status())) {
        set::SCardResponse scard_response;
        if (scard_response.ParseFromString(set_response.response_bytes()) && scard_response.len() > 0) {
            c->reply->SetStatus("set");
            return;
        }
    }

    c->reply->SetStatus("none");
}

void RedisCommandHandler::Get(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringGetCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetNullString();
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(response, &get_response, c->reply)) {
        return;
    }
    c->reply->SetString(get_response.value());
}


void RedisCommandHandler::Set(RedisClientContext* c) {
    bool nx = false;
    bool xx = false;
    bool get = false;
    bool has_ttl = false;
    uint64_t ttl_ms = 0;
    for (size_t i = 3; i < c->ArgSize(); ++i) {
        const std::string option = c->StrArg(i);
        if (!strcasecmp(option.c_str(), "nx")) {
            if (nx || xx) {
                c->reply->SetError("ERR syntax error");
                return;
            }
            nx = true;
        } else if (!strcasecmp(option.c_str(), "xx")) {
            if (nx || xx) {
                c->reply->SetError("ERR syntax error");
                return;
            }
            xx = true;
        } else if (!strcasecmp(option.c_str(), "get")) {
            get = true;
        } else if (!strcasecmp(option.c_str(), "ex") || !strcasecmp(option.c_str(), "px")) {
            if (has_ttl || i + 1 >= c->ArgSize()) {
                c->reply->SetError("ERR syntax error");
                return;
            }
            int64_t parsed = 0;
            if (!ParseInt64Arg(c->StrArg(++i), &parsed) || parsed <= 0) {
                c->reply->SetError("ERR invalid expire time in set");
                return;
            }
            ttl_ms = !strcasecmp(option.c_str(), "ex") ? parsed * 1000 : parsed;
            has_ttl = true;
        } else {
            c->reply->SetError("ERR unsupported SET option");
            return;
        }
    }

    if (has_ttl && (nx || xx)) {
        c->reply->SetError("ERR SET EX/PX with NX/XX is not supported by the native Redis bridge yet");
        return;
    }

    bool old_exists = false;
    std::string old_value;
    if (get) {
        CmdResponse get_response_raw;
        if (!ExecuteRedisSingle(redis_service_, StringGetCmd(c->StrArg(1)), &get_response_raw, c->reply)) {
            return;
        }
        if (IsOkStatus(get_response_raw.response_status())) {
            str2::GetResponse get_response;
            if (!ParseStringGetResponse(get_response_raw, &get_response, c->reply)) {
                return;
            }
            old_exists = true;
            old_value = get_response.value();
        }
    }

    CmdResponse response;
    CmdRequest command = has_ttl ? StringSetExCmd(c->StrArg(1), c->StrArg(2), ttl_ms)
                                 : StringSetCmd(c->StrArg(1), c->StrArg(2), nx, xx);
    if (!ExecuteRedisSingle(redis_service_, command, &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        if ((nx && response.response_status().code() == Code::kAlreadyExists) ||
            (xx && response.response_status().code() == Code::kNotFound)) {
            if (get && old_exists) {
                c->reply->SetString(old_value);
            } else {
                c->reply->SetNullString();
            }
            return;
        }
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    if (get) {
        if (old_exists) {
            c->reply->SetString(old_value);
        } else {
            c->reply->SetNullString();
        }
        return;
    }
    c->reply->SetStatus("OK");
}


void RedisCommandHandler::SetNx(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringSetCmd(c->StrArg(1), c->StrArg(2), true, false),
                            &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        if (response.response_status().code() == Code::kAlreadyExists) {
            c->reply->SetInteger(0);
            return;
        }
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    c->reply->SetInteger(1);
}

void RedisCommandHandler::SetEx(RedisClientContext* c) {
    int64_t seconds = 0;
    if (!ParseInt64Arg(c->StrArg(2), &seconds) || seconds <= 0) {
        c->reply->SetError("ERR invalid expire time in setex");
        return;
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringSetExCmd(c->StrArg(1), c->StrArg(3), seconds * 1000),
                            &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::PSetEx(RedisClientContext* c) {
    int64_t milliseconds = 0;
    if (!ParseInt64Arg(c->StrArg(2), &milliseconds) || milliseconds <= 0) {
        c->reply->SetError("ERR invalid expire time in psetex");
        return;
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_,
                            StringSetExCmd(c->StrArg(1), c->StrArg(3), milliseconds), &response,
                            c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::GetSet(RedisClientContext* c) {
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_,
                           {StringGetCmd(c->StrArg(1)),
                            StringSetCmd(c->StrArg(1), c->StrArg(2))},
                           &response, c->reply)) {
        return;
    }
    const auto& get_item = response.response(0);
    const auto& set_item = response.response(1);
    if (!IsOkStatus(set_item.response_status())) {
        SetRedisStatusError(c->reply, set_item.response_status());
        return;
    }
    if (!IsOkStatus(get_item.response_status())) {
        c->reply->SetNullString();
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(get_item, &get_response, c->reply)) {
        return;
    }
    c->reply->SetString(get_response.value());
}

void RedisCommandHandler::GetDel(RedisClientContext* c) {
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, {StringGetCmd(c->StrArg(1)), DelCmd(c->StrArg(1))},
                           &response, c->reply)) {
        return;
    }
    const auto& get_item = response.response(0);
    if (!IsOkStatus(get_item.response_status())) {
        c->reply->SetNullString();
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(get_item, &get_response, c->reply)) {
        return;
    }
    c->reply->SetString(get_response.value());
}


void RedisCommandHandler::GetEx(RedisClientContext* c) {
    bool persist = false;
    bool has_ttl = false;
    uint64_t ttl_ms = 0;
    if (c->ArgSize() == 3 && !strcasecmp(c->StrArg(2).c_str(), "persist")) {
        persist = true;
    } else if (c->ArgSize() == 4 &&
               (!strcasecmp(c->StrArg(2).c_str(), "ex") ||
                !strcasecmp(c->StrArg(2).c_str(), "px"))) {
        int64_t parsed = 0;
        if (!ParseInt64Arg(c->StrArg(3), &parsed) || parsed <= 0) {
            c->reply->SetError("ERR invalid expire time in getex");
            return;
        }
        ttl_ms = !strcasecmp(c->StrArg(2).c_str(), "ex") ? parsed * 1000 : parsed;
        has_ttl = true;
    } else if (c->ArgSize() != 2) {
        c->reply->SetError("ERR syntax error");
        return;
    }

    CmdResponse get_response_raw;
    if (!ExecuteRedisSingle(redis_service_, StringGetCmd(c->StrArg(1)), &get_response_raw, c->reply)) {
        return;
    }
    if (!IsOkStatus(get_response_raw.response_status())) {
        c->reply->SetNullString();
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(get_response_raw, &get_response, c->reply)) {
        return;
    }

    if (persist || has_ttl) {
        CmdResponse update_response;
        CmdRequest update_cmd = persist ? PersistCmd(c->StrArg(1)) : ExpireCmd(c->StrArg(1), ttl_ms);
        if (!ExecuteRedisSingle(redis_service_, update_cmd, &update_response, c->reply)) {
            return;
        }
        if (!IsOkStatus(update_response.response_status())) {
            SetRedisStatusError(c->reply, update_response.response_status());
            return;
        }
    }
    c->reply->SetString(get_response.value());
}

void RedisCommandHandler::MGet(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 1; i < c->ArgSize(); ++i) {
        commands.emplace_back(StringGetCmd(c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    c->reply->SetArray(response.response_size());
    for (int i = 0; i < response.response_size(); ++i) {
        const auto& item = response.response(i);
        if (!IsOkStatus(item.response_status())) {
            (*c->reply)[i].SetNullString();
        } else {
            str2::GetResponse get_response;
            if (!ParseStringGetResponse(item, &get_response, c->reply)) {
                return;
            }
            (*c->reply)[i].SetString(get_response.value());
        }
    }
}

void RedisCommandHandler::MSet(RedisClientContext* c) {
    if ((c->ArgSize() - 1) % 2 != 0) {
        c->reply->SetError("ERR wrong number of arguments for 'mset' command");
        return;
    }
    std::vector<CmdRequest> commands;
    for (size_t i = 1; i + 1 < c->ArgSize(); i += 2) {
        commands.emplace_back(StringSetCmd(c->StrArg(i), c->StrArg(i + 1)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    for (int i = 0; i < response.response_size(); ++i) {
        if (!IsOkStatus(response.response(i).response_status())) {
            SetRedisStatusError(c->reply, response.response(i).response_status());
            return;
        }
    }
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::Del(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 1; i < c->ArgSize(); ++i) {
        commands.emplace_back(DelCmd(c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    int64_t deleted = 0;
    for (int i = 0; i < response.response_size(); ++i) {
        if (IsOkStatus(response.response(i).response_status())) {
            ++deleted;
        }
    }
    c->reply->SetInteger(deleted);
}

void RedisCommandHandler::Exists(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 1; i < c->ArgSize(); ++i) {
        commands.emplace_back(ExistsCmd(c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    int64_t existing = 0;
    for (int i = 0; i < response.response_size(); ++i) {
        if (IsOkStatus(response.response(i).response_status())) {
            ++existing;
        }
    }
    c->reply->SetInteger(existing);
}

void RedisCommandHandler::Expire(RedisClientContext* c) {
    int64_t seconds = 0;
    if (!ParseInt64Arg(c->StrArg(2), &seconds) || seconds < 0) {
        c->reply->SetError("ERR invalid expire time in expire");
        return;
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, ExpireCmd(c->StrArg(1), seconds * 1000), &response,
                            c->reply)) {
        return;
    }
    c->reply->SetInteger(IsOkStatus(response.response_status()) ? 1 : 0);
}

void RedisCommandHandler::PExpire(RedisClientContext* c) {
    int64_t milliseconds = 0;
    if (!ParseInt64Arg(c->StrArg(2), &milliseconds) || milliseconds < 0) {
        c->reply->SetError("ERR invalid expire time in pexpire");
        return;
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, ExpireCmd(c->StrArg(1), milliseconds), &response,
                            c->reply)) {
        return;
    }
    c->reply->SetInteger(IsOkStatus(response.response_status()) ? 1 : 0);
}

void RedisCommandHandler::Ttl(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, TtlCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(-2);
        return;
    }
    common2::TtlResponse ttl_response;
    if (!ttl_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse TTL response");
        return;
    }
    uint64_t ttl_ms = ttl_response.ttl_ms();
    c->reply->SetInteger(ttl_ms == 0 ? -1 : static_cast<int64_t>((ttl_ms + 999) / 1000));
}

void RedisCommandHandler::PTtl(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, TtlCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(-2);
        return;
    }
    common2::TtlResponse ttl_response;
    if (!ttl_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse PTTL response");
        return;
    }
    uint64_t ttl_ms = ttl_response.ttl_ms();
    c->reply->SetInteger(ttl_ms == 0 ? -1 : static_cast<int64_t>(ttl_ms));
}

void RedisCommandHandler::Persist(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, PersistCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    c->reply->SetInteger(IsOkStatus(response.response_status()) ? 1 : 0);
}

void RedisCommandHandler::Append(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringAppendCmd(c->StrArg(1), c->StrArg(2)), &response,
                            c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    str2::AppendResponse append_response;
    if (!append_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse APPEND response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(append_response.len()));
}

void RedisCommandHandler::Strlen(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringStrlenCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    str2::StrlenResponse strlen_response;
    if (!strlen_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse STRLEN response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(strlen_response.len()));
}

void RedisCommandHandler::IncrBy(RedisClientContext* c) {
    int64_t increment = 1;
    const std::string command = c->StrArg(0);
    if (!strcasecmp(command.c_str(), "decr")) {
        increment = -1;
    } else if (!strcasecmp(command.c_str(), "incrby") ||
               !strcasecmp(command.c_str(), "decrby")) {
        if (!ParseInt64Arg(c->StrArg(2), &increment)) {
            c->reply->SetError("ERR value is not an integer or out of range");
            return;
        }
        if (!strcasecmp(command.c_str(), "decrby")) {
            if (increment == LLONG_MIN) {
                c->reply->SetError("ERR decrement would overflow");
                return;
            }
            increment = -increment;
        }
    }

    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringIncrByCmd(c->StrArg(1), increment), &response,
                            c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    str2::IncrByResponse incr_response;
    if (!incr_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse INCRBY response");
        return;
    }
    c->reply->SetInteger(incr_response.value());
}

void RedisCommandHandler::HSet(RedisClientContext* c) {
    if ((c->ArgSize() - 2) % 2 != 0) {
        c->reply->SetError("ERR wrong number of arguments for 'hset' command");
        return;
    }

    std::vector<CmdRequest> precheck_commands;
    for (size_t i = 2; i + 1 < c->ArgSize(); i += 2) {
        precheck_commands.emplace_back(HashGetExtCmd(c->StrArg(1), c->StrArg(i)));
    }
    BatchExecuteCmdResponse precheck_response;
    if (!ExecuteRedisBatch(redis_service_, precheck_commands, &precheck_response, c->reply)) {
        return;
    }
    int64_t added = 0;
    for (int i = 0; i < precheck_response.response_size(); ++i) {
        const auto& item = precheck_response.response(i);
        hash2::GetResponse get_response;
        if (!IsOkStatus(item.response_status())) {
            ++added;
            continue;
        }
        if (!ParseHashGetResponse(item, &get_response, c->reply)) {
            return;
        }
        if (!get_response.exist()) {
            ++added;
        }
    }

    std::vector<CmdRequest> commands;
    for (size_t i = 2; i + 1 < c->ArgSize(); i += 2) {
        commands.emplace_back(HashSetCmd(c->StrArg(1), c->StrArg(i), c->StrArg(i + 1)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    for (int i = 0; i < response.response_size(); ++i) {
        if (!IsOkStatus(response.response(i).response_status())) {
            SetRedisStatusError(c->reply, response.response(i).response_status());
            return;
        }
    }
    if (!strcasecmp(c->StrArg(0).c_str(), "hmset")) {
        c->reply->SetStatus("OK");
        return;
    }
    c->reply->SetInteger(added);
}

void RedisCommandHandler::HGet(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetExtCmd(c->StrArg(1), c->StrArg(2)), &response,
                            c->reply)) {
        return;
    }
    hash2::GetResponse get_response;
    if (!IsOkStatus(response.response_status()) ||
        !ParseHashGetResponse(response, &get_response, c->reply) || !get_response.exist()) {
        c->reply->SetNullString();
        return;
    }
    c->reply->SetString(get_response.value());
}

void RedisCommandHandler::HMGet(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        commands.emplace_back(HashGetExtCmd(c->StrArg(1), c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    c->reply->SetArray(response.response_size());
    for (int i = 0; i < response.response_size(); ++i) {
        const auto& item = response.response(i);
        hash2::GetResponse get_response;
        if (!IsOkStatus(item.response_status()) ||
            !ParseHashGetResponse(item, &get_response, c->reply) || !get_response.exist()) {
            (*c->reply)[i].SetNullString();
        } else {
            (*c->reply)[i].SetString(get_response.value());
        }
    }
}

void RedisCommandHandler::HDel(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        commands.emplace_back(HashDelCmd(c->StrArg(1), c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    int64_t deleted = 0;
    for (int i = 0; i < response.response_size(); ++i) {
        if (IsOkStatus(response.response(i).response_status())) {
            ++deleted;
        }
    }
    c->reply->SetInteger(deleted);
}

void RedisCommandHandler::HExists(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetExtCmd(c->StrArg(1), c->StrArg(2)), &response,
                            c->reply)) {
        return;
    }
    hash2::GetResponse get_response;
    c->reply->SetInteger(IsOkStatus(response.response_status()) &&
                                 ParseHashGetResponse(response, &get_response, c->reply) &&
                                 get_response.exist()
                             ? 1
                             : 0);
}

void RedisCommandHandler::HLen(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashLenCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    hash2::LenResponse len_response;
    if (!len_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse HLEN response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(len_response.len()));
}

void RedisCommandHandler::HGetAll(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetAllCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetArray(0);
        return;
    }
    hash2::GetAllResponse getall_response;
    if (!getall_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse HGETALL response");
        return;
    }
    c->reply->SetArray(getall_response.fields_size() * 2);
    for (int i = 0; i < getall_response.fields_size(); ++i) {
        (*c->reply)[i * 2].SetString(getall_response.fields(i));
        (*c->reply)[i * 2 + 1].SetString(getall_response.values(i));
    }
}

void RedisCommandHandler::HKeys(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetAllCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetArray(0);
        return;
    }
    hash2::GetAllResponse getall_response;
    if (!getall_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse HKEYS response");
        return;
    }
    c->reply->SetArray(getall_response.fields_size());
    for (int i = 0; i < getall_response.fields_size(); ++i) {
        (*c->reply)[i].SetString(getall_response.fields(i));
    }
}

void RedisCommandHandler::HVals(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetAllCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetArray(0);
        return;
    }
    hash2::GetAllResponse getall_response;
    if (!getall_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse HVALS response");
        return;
    }
    c->reply->SetArray(getall_response.values_size());
    for (int i = 0; i < getall_response.values_size(); ++i) {
        (*c->reply)[i].SetString(getall_response.values(i));
    }
}


void RedisCommandHandler::HStrlen(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashGetExtCmd(c->StrArg(1), c->StrArg(2)), &response,
                            c->reply)) {
        return;
    }
    hash2::GetResponse get_response;
    if (!IsOkStatus(response.response_status()) ||
        !ParseHashGetResponse(response, &get_response, c->reply) || !get_response.exist()) {
        c->reply->SetInteger(0);
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(get_response.value().size()));
}

void RedisCommandHandler::HIncrBy(RedisClientContext* c) {
    int64_t delta = 0;
    if (!ParseInt64Arg(c->StrArg(3), &delta)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, HashIncrByCmd(c->StrArg(1), c->StrArg(2), delta),
                            &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    hash2::IncrByResponse incr_response;
    if (!incr_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse HINCRBY response");
        return;
    }
    c->reply->SetInteger(incr_response.value());
}

void RedisCommandHandler::SAdd(RedisClientContext* c) {
    std::vector<std::string> members;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        members.emplace_back(c->StrArg(i));
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, SetAddCmd(c->StrArg(1), members), &response,
                            c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    set::SAddResponse sadd_response;
    if (!sadd_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse SADD response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(sadd_response.added()));
}

void RedisCommandHandler::SRem(RedisClientContext* c) {
    std::vector<std::string> members;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        members.emplace_back(c->StrArg(i));
    }
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, SetRemCmd(c->StrArg(1), members), &response,
                            c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        SetRedisStatusError(c->reply, response.response_status());
        return;
    }
    set::SRemResponse srem_response;
    if (!srem_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse SREM response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(srem_response.removed()));
}

void RedisCommandHandler::SMembers(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, SetMembersCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetArray(0);
        return;
    }
    set::SMembersResponse members_response;
    if (!members_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse SMEMBERS response");
        return;
    }
    c->reply->SetArray(members_response.members_size());
    for (int i = 0; i < members_response.members_size(); ++i) {
        (*c->reply)[i].SetString(members_response.members(i));
    }
}

void RedisCommandHandler::SCard(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, SetCardCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    set::SCardResponse scard_response;
    if (!scard_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse SCARD response");
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(scard_response.len()));
}

void RedisCommandHandler::SIsMember(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, SetIsMemberCmd(c->StrArg(1), c->StrArg(2)),
                            &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    set::SIsMemberResponse sismember_response;
    if (!sismember_response.ParseFromString(response.response_bytes())) {
        SetRedisError(c->reply, "failed to parse SISMEMBER response");
        return;
    }
    c->reply->SetInteger(sismember_response.exist() ? 1 : 0);
}

void RedisCommandHandler::SMIsMember(RedisClientContext* c) {
    std::vector<CmdRequest> commands;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        commands.emplace_back(SetIsMemberCmd(c->StrArg(1), c->StrArg(i)));
    }
    BatchExecuteCmdResponse response;
    if (!ExecuteRedisBatch(redis_service_, commands, &response, c->reply)) {
        return;
    }
    c->reply->SetArray(response.response_size());
    for (int i = 0; i < response.response_size(); ++i) {
        const auto& item = response.response(i);
        set::SIsMemberResponse member_response;
        if (!IsOkStatus(item.response_status()) ||
            !member_response.ParseFromString(item.response_bytes())) {
            (*c->reply)[i].SetInteger(0);
        } else {
            (*c->reply)[i].SetInteger(member_response.exist() ? 1 : 0);
        }
    }
}

void RedisCommandHandler::LPush(RedisClientContext* c) {
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        values.insert(values.begin(), c->StrArg(i));
    }
    if (!SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(values.size()));
}

void RedisCommandHandler::RPush(RedisClientContext* c) {
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        values.emplace_back(c->StrArg(i));
    }
    if (!SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(values.size()));
}


void RedisCommandHandler::LPushX(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringGetCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(response, &get_response, c->reply)) {
        return;
    }
    std::vector<std::string> values;
    if (!DecodeStringVector(get_response.value(), ListPrefix(), &values)) {
        SetRedisError(c->reply, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return;
    }
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        values.insert(values.begin(), c->StrArg(i));
    }
    if (!SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(values.size()));
}

void RedisCommandHandler::RPushX(RedisClientContext* c) {
    CmdResponse response;
    if (!ExecuteRedisSingle(redis_service_, StringGetCmd(c->StrArg(1)), &response, c->reply)) {
        return;
    }
    if (!IsOkStatus(response.response_status())) {
        c->reply->SetInteger(0);
        return;
    }
    str2::GetResponse get_response;
    if (!ParseStringGetResponse(response, &get_response, c->reply)) {
        return;
    }
    std::vector<std::string> values;
    if (!DecodeStringVector(get_response.value(), ListPrefix(), &values)) {
        SetRedisError(c->reply, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return;
    }
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        values.emplace_back(c->StrArg(i));
    }
    if (!SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(values.size()));
}

void RedisCommandHandler::LPop(RedisClientContext* c) {
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    int64_t count = 1;
    bool count_mode = c->ArgSize() == 3;
    if (count_mode && (!ParseInt64Arg(c->StrArg(2), &count) || count < 0)) {
        c->reply->SetError("ERR value is out of range, must be positive");
        return;
    }
    if (values.empty()) {
        if (count_mode) {
            c->reply->SetNullArray();
        } else {
            c->reply->SetNullString();
        }
        return;
    }
    int64_t n = std::min<int64_t>(count, values.size());
    if (count_mode) {
        c->reply->SetArray(n);
        for (int64_t i = 0; i < n; ++i) {
            (*c->reply)[i].SetString(values[i]);
        }
    } else {
        c->reply->SetString(values.front());
    }
    values.erase(values.begin(), values.begin() + n);
    SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply);
}

void RedisCommandHandler::RPop(RedisClientContext* c) {
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    int64_t count = 1;
    bool count_mode = c->ArgSize() == 3;
    if (count_mode && (!ParseInt64Arg(c->StrArg(2), &count) || count < 0)) {
        c->reply->SetError("ERR value is out of range, must be positive");
        return;
    }
    if (values.empty()) {
        if (count_mode) {
            c->reply->SetNullArray();
        } else {
            c->reply->SetNullString();
        }
        return;
    }
    int64_t n = std::min<int64_t>(count, values.size());
    if (count_mode) {
        c->reply->SetArray(n);
        for (int64_t i = 0; i < n; ++i) {
            (*c->reply)[i].SetString(values[values.size() - 1 - i]);
        }
    } else {
        c->reply->SetString(values.back());
    }
    values.erase(values.end() - n, values.end());
    SaveEncodedList(redis_service_, c->StrArg(1), values, c->reply);
}

void RedisCommandHandler::LLen(RedisClientContext* c) {
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(values.size()));
}

void RedisCommandHandler::LIndex(RedisClientContext* c) {
    int64_t index = 0;
    if (!ParseInt64Arg(c->StrArg(2), &index)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    index = NormalizeIndex(index, values.size());
    if (index < 0 || index >= static_cast<int64_t>(values.size())) {
        c->reply->SetNullString();
        return;
    }
    c->reply->SetString(values[index]);
}

void RedisCommandHandler::LRange(RedisClientContext* c) {
    int64_t start = 0;
    int64_t stop = 0;
    if (!ParseInt64Arg(c->StrArg(2), &start) || !ParseInt64Arg(c->StrArg(3), &stop)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    auto range = NormalizeRange(start, stop, values.size());
    if (values.empty() || range.first > range.second || range.first >= static_cast<int64_t>(values.size())) {
        c->reply->SetArray(0);
        return;
    }
    int64_t n = range.second - range.first + 1;
    c->reply->SetArray(n);
    for (int64_t i = 0; i < n; ++i) {
        (*c->reply)[i].SetString(values[range.first + i]);
    }
}

void RedisCommandHandler::LTrim(RedisClientContext* c) {
    int64_t start = 0;
    int64_t stop = 0;
    if (!ParseInt64Arg(c->StrArg(2), &start) || !ParseInt64Arg(c->StrArg(3), &stop)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<std::string> values;
    if (!LoadEncodedList(redis_service_, c->StrArg(1), &values, c->reply)) {
        return;
    }
    auto range = NormalizeRange(start, stop, values.size());
    std::vector<std::string> kept;
    if (!values.empty() && range.first <= range.second &&
        range.first < static_cast<int64_t>(values.size())) {
        kept.assign(values.begin() + range.first, values.begin() + range.second + 1);
    }
    if (!SaveEncodedList(redis_service_, c->StrArg(1), kept, c->reply)) {
        return;
    }
    c->reply->SetStatus("OK");
}

void RedisCommandHandler::ZAdd(RedisClientContext* c) {
    if ((c->ArgSize() - 2) % 2 != 0) {
        c->reply->SetError("ERR wrong number of arguments for 'zadd' command");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    int64_t added = 0;
    for (size_t i = 2; i + 1 < c->ArgSize(); i += 2) {
        double score = 0;
        if (!ParseDoubleArg(c->StrArg(i), &score)) {
            c->reply->SetError("ERR value is not a valid float");
            return;
        }
        int index = FindZMember(items, c->StrArg(i + 1));
        if (index < 0) {
            items.push_back({c->StrArg(i + 1), score, FormatDouble(score)});
            ++added;
        } else {
            items[index].score = score;
            items[index].score_text = FormatDouble(score);
        }
    }
    if (!SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply)) {
        return;
    }
    c->reply->SetInteger(added);
}

void RedisCommandHandler::ZRem(RedisClientContext* c) {
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    int64_t removed = 0;
    for (size_t i = 2; i < c->ArgSize(); ++i) {
        int index = FindZMember(items, c->StrArg(i));
        if (index >= 0) {
            items.erase(items.begin() + index);
            ++removed;
        }
    }
    if (!SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply)) {
        return;
    }
    c->reply->SetInteger(removed);
}


void RedisCommandHandler::ZIncrBy(RedisClientContext* c) {
    double increment = 0;
    if (!ParseDoubleArg(c->StrArg(2), &increment)) {
        c->reply->SetError("ERR value is not a valid float");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    int index = FindZMember(items, c->StrArg(3));
    double new_score = increment;
    if (index < 0) {
        items.push_back({c->StrArg(3), new_score, FormatDouble(new_score)});
    } else {
        new_score = items[index].score + increment;
        if (!std::isfinite(new_score)) {
            c->reply->SetError("ERR resulting score is not a valid float");
            return;
        }
        items[index].score = new_score;
        items[index].score_text = FormatDouble(new_score);
    }
    if (!SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply)) {
        return;
    }
    c->reply->SetString(FormatDouble(new_score));
}

void RedisCommandHandler::ZPopMin(RedisClientContext* c) {
    int64_t count = 1;
    if (c->ArgSize() == 3 && (!ParseInt64Arg(c->StrArg(2), &count) || count < 0)) {
        c->reply->SetError("ERR value is out of range, must be positive");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items);
    int64_t n = std::min<int64_t>(count, items.size());
    c->reply->SetArray(n * 2);
    for (int64_t i = 0; i < n; ++i) {
        (*c->reply)[i * 2].SetString(items[i].member);
        (*c->reply)[i * 2 + 1].SetString(items[i].score_text);
    }
    items.erase(items.begin(), items.begin() + n);
    SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply);
}

void RedisCommandHandler::ZPopMax(RedisClientContext* c) {
    int64_t count = 1;
    if (c->ArgSize() == 3 && (!ParseInt64Arg(c->StrArg(2), &count) || count < 0)) {
        c->reply->SetError("ERR value is out of range, must be positive");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items, true);
    int64_t n = std::min<int64_t>(count, items.size());
    c->reply->SetArray(n * 2);
    for (int64_t i = 0; i < n; ++i) {
        (*c->reply)[i * 2].SetString(items[i].member);
        (*c->reply)[i * 2 + 1].SetString(items[i].score_text);
    }
    items.erase(items.begin(), items.begin() + n);
    SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply);
}

void RedisCommandHandler::ZCard(RedisClientContext* c) {
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(items.size()));
}

void RedisCommandHandler::ZScore(RedisClientContext* c) {
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    int index = FindZMember(items, c->StrArg(2));
    if (index < 0) {
        c->reply->SetNullString();
        return;
    }
    c->reply->SetString(items[index].score_text);
}

void RedisCommandHandler::ZRank(RedisClientContext* c) {
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items);
    int index = FindZMember(items, c->StrArg(2));
    if (index < 0) {
        c->reply->SetNullString();
        return;
    }
    c->reply->SetInteger(index);
}

void RedisCommandHandler::ZRevRank(RedisClientContext* c) {
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items, true);
    int index = FindZMember(items, c->StrArg(2));
    if (index < 0) {
        c->reply->SetNullString();
        return;
    }
    c->reply->SetInteger(index);
}

void RedisCommandHandler::ZRange(RedisClientContext* c) {
    int64_t start = 0;
    int64_t stop = 0;
    if (!ParseInt64Arg(c->StrArg(2), &start) || !ParseInt64Arg(c->StrArg(3), &stop)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items);
    auto range = NormalizeRange(start, stop, items.size());
    bool with_scores = WithScoresArg(c, 4);
    if (items.empty() || range.first > range.second || range.first >= static_cast<int64_t>(items.size())) {
        c->reply->SetArray(0);
        return;
    }
    int64_t n = range.second - range.first + 1;
    c->reply->SetArray(with_scores ? n * 2 : n);
    for (int64_t i = 0; i < n; ++i) {
        (*c->reply)[with_scores ? i * 2 : i].SetString(items[range.first + i].member);
        if (with_scores) {
            (*c->reply)[i * 2 + 1].SetString(items[range.first + i].score_text);
        }
    }
}

void RedisCommandHandler::ZRevRange(RedisClientContext* c) {
    int64_t start = 0;
    int64_t stop = 0;
    if (!ParseInt64Arg(c->StrArg(2), &start) || !ParseInt64Arg(c->StrArg(3), &stop)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items, true);
    auto range = NormalizeRange(start, stop, items.size());
    bool with_scores = WithScoresArg(c, 4);
    if (items.empty() || range.first > range.second || range.first >= static_cast<int64_t>(items.size())) {
        c->reply->SetArray(0);
        return;
    }
    int64_t n = range.second - range.first + 1;
    c->reply->SetArray(with_scores ? n * 2 : n);
    for (int64_t i = 0; i < n; ++i) {
        (*c->reply)[with_scores ? i * 2 : i].SetString(items[range.first + i].member);
        if (with_scores) {
            (*c->reply)[i * 2 + 1].SetString(items[range.first + i].score_text);
        }
    }
}

void RedisCommandHandler::ZRangeByScore(RedisClientContext* c) {
    double min_score = 0;
    double max_score = 0;
    if (!ParseZRangeBound(c->StrArg(2), &min_score) ||
        !ParseZRangeBound(c->StrArg(3), &max_score)) {
        c->reply->SetError("ERR min or max is not a float");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items);
    bool with_scores = WithScoresArg(c, 4);
    int64_t offset = 0;
    int64_t count = LLONG_MAX;
    size_t limit_index = with_scores ? 5 : 4;
    if (!ParseLimitArgs(c, limit_index, &offset, &count, c->reply)) {
        return;
    }
    std::vector<ZItem> selected;
    for (const auto& item : items) {
        if (item.score >= min_score && item.score <= max_score) {
            selected.push_back(item);
        }
    }
    size_t begin = std::min(static_cast<size_t>(offset), selected.size());
    size_t end = count < 0 ? selected.size()
                           : std::min(selected.size(), begin + static_cast<size_t>(count));
    size_t result_size = end - begin;
    c->reply->SetArray(with_scores ? result_size * 2 : result_size);
    for (size_t i = 0; i < result_size; ++i) {
        const auto& item = selected[begin + i];
        (*c->reply)[with_scores ? i * 2 : i].SetString(item.member);
        if (with_scores) {
            (*c->reply)[i * 2 + 1].SetString(item.score_text);
        }
    }
}


void RedisCommandHandler::ZRemRangeByScore(RedisClientContext* c) {
    double min_score = 0;
    double max_score = 0;
    if (!ParseZRangeBound(c->StrArg(2), &min_score) ||
        !ParseZRangeBound(c->StrArg(3), &max_score)) {
        c->reply->SetError("ERR min or max is not a float");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    size_t old_size = items.size();
    items.erase(std::remove_if(items.begin(), items.end(), [min_score, max_score](const ZItem& item) {
        return item.score >= min_score && item.score <= max_score;
    }), items.end());
    if (!SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply)) {
        return;
    }
    c->reply->SetInteger(static_cast<int64_t>(old_size - items.size()));
}

void RedisCommandHandler::ZRemRangeByRank(RedisClientContext* c) {
    int64_t start = 0;
    int64_t stop = 0;
    if (!ParseInt64Arg(c->StrArg(2), &start) || !ParseInt64Arg(c->StrArg(3), &stop)) {
        c->reply->SetError("ERR value is not an integer or out of range");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    SortZSet(&items);
    auto range = NormalizeRange(start, stop, items.size());
    if (items.empty() || range.first > range.second || range.first >= static_cast<int64_t>(items.size())) {
        c->reply->SetInteger(0);
        return;
    }
    int64_t removed = range.second - range.first + 1;
    items.erase(items.begin() + range.first, items.begin() + range.second + 1);
    if (!SaveEncodedZSet(redis_service_, c->StrArg(1), items, c->reply)) {
        return;
    }
    c->reply->SetInteger(removed);
}

void RedisCommandHandler::ZCount(RedisClientContext* c) {
    double min_score = 0;
    double max_score = 0;
    if (!ParseZRangeBound(c->StrArg(2), &min_score) ||
        !ParseZRangeBound(c->StrArg(3), &max_score)) {
        c->reply->SetError("ERR min or max is not a float");
        return;
    }
    std::vector<ZItem> items;
    if (!LoadEncodedZSet(redis_service_, c->StrArg(1), &items, c->reply)) {
        return;
    }
    int64_t count = 0;
    for (const auto& item : items) {
        if (item.score >= min_score && item.score <= max_score) {
            ++count;
        }
    }
    c->reply->SetInteger(count);
}

void RedisCommandHandler::Unsupported(RedisClientContext* c) {
    c->reply->SetError("ERR command '" + c->StrArg(0) + "' is not supported by the native Redis bridge yet");
}

}  // namespace serve
}  // namespace bcache2
