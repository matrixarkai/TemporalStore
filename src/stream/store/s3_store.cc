#include "stream/store/s3_store.h"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <initializer_list>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"

namespace bcache2 {
namespace stream {

namespace {

struct S3Uri {
    std::string scheme;
    std::string bucket;
    std::string key;
};

struct S3Config {
    std::string endpoint;
    std::string region = "us-east-1";
    std::string access_key;
    std::string secret_key;
    std::string session_token;
    bool unsigned_requests = false;
};

struct HttpResponse {
    CURLcode curl_code = CURLE_OK;
    long http_code = 0;
    std::string body;
    std::string headers;
};

std::string EnvValue(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? "" : value;
}

std::string FirstEnvValue(std::initializer_list<const char*> names) {
    for (const char* name : names) {
        std::string value = EnvValue(name);
        if (!value.empty()) {
            return value;
        }
    }
    return "";
}

std::string TrimTrailingSlash(std::string value) {
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

Status ParseS3Uri(const std::string& uri, S3Uri* parsed, bool allow_empty_key = false) {
    const size_t scheme_pos = uri.find("://");
    if (scheme_pos == std::string::npos) {
        return Status::InvalidArgument("invalid S3 URI");
    }
    parsed->scheme = uri.substr(0, scheme_pos);
    const size_t bucket_start = scheme_pos + 3;
    const size_t key_start = uri.find('/', bucket_start);
    if (key_start == std::string::npos || key_start == bucket_start) {
        if (!allow_empty_key) {
            return Status::InvalidArgument("S3 URI requires bucket and key prefix");
        }
        parsed->bucket = uri.substr(bucket_start);
        parsed->key.clear();
        return Status::OK();
    }
    parsed->bucket = uri.substr(bucket_start, key_start - bucket_start);
    parsed->key = uri.substr(key_start + 1);
    if (parsed->key.empty()) {
        if (allow_empty_key) {
            return Status::OK();
        }
        return Status::InvalidArgument("S3 URI requires object key");
    }
    return Status::OK();
}

S3Config LoadConfig() {
    S3Config config;
    config.endpoint =
        TrimTrailingSlash(FirstEnvValue({"TEMPORALSTORE_S3_ENDPOINT", "AWS_ENDPOINT_URL_S3",
                                         "S3_ENDPOINT_URL", "AWS_ENDPOINT_URL"}));
    config.region = FirstEnvValue({"AWS_REGION", "AWS_DEFAULT_REGION", "TEMPORALSTORE_S3_REGION"});
    if (config.region.empty()) {
        config.region = "us-east-1";
    }
    config.access_key = FirstEnvValue({"AWS_ACCESS_KEY_ID", "MINIO_ROOT_USER"});
    config.secret_key = FirstEnvValue({"AWS_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD"});
    config.session_token = EnvValue("AWS_SESSION_TOKEN");
    config.unsigned_requests = EnvValue("TEMPORALSTORE_S3_UNSIGNED") == "1";
    return config;
}

std::string Hex(const unsigned char* data, size_t size) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (size_t i = 0; i < size; ++i) {
        out << std::setw(2) << static_cast<unsigned int>(data[i]);
    }
    return out.str();
}

std::string Sha256Hex(const std::string& data) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest);
    return Hex(digest, sizeof(digest));
}

std::array<unsigned char, SHA256_DIGEST_LENGTH> HmacSha256(const unsigned char* key, size_t key_len,
                                                           const std::string& data) {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    unsigned int len = digest.size();
    HMAC(EVP_sha256(), key, static_cast<int>(key_len),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest.data(), &len);
    return digest;
}

std::array<unsigned char, SHA256_DIGEST_LENGTH> HmacSha256(
    const std::array<unsigned char, SHA256_DIGEST_LENGTH>& key, const std::string& data) {
    return HmacSha256(key.data(), key.size(), data);
}

std::string UriEncode(const std::string& value, bool encode_slash) {
    std::ostringstream out;
    out << std::uppercase << std::hex << std::setfill('0');
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' ||
            (c == '/' && !encode_slash)) {
            out << static_cast<char>(c);
        } else {
            out << '%' << std::setw(2) << static_cast<unsigned int>(c);
        }
    }
    return out.str();
}

std::string HttpDate(std::string* short_date) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &now);
#else
    gmtime_r(&now, &tm);
#endif
    char date_time[17];
    char date[9];
    std::strftime(date_time, sizeof(date_time), "%Y%m%dT%H%M%SZ", &tm);
    std::strftime(date, sizeof(date), "%Y%m%d", &tm);
    *short_date = date;
    return date_time;
}

std::string HostFromEndpoint(const std::string& endpoint) {
    std::string host = endpoint;
    const size_t scheme_pos = host.find("://");
    if (scheme_pos != std::string::npos) {
        host = host.substr(scheme_pos + 3);
    }
    const size_t slash_pos = host.find('/');
    if (slash_pos != std::string::npos) {
        host = host.substr(0, slash_pos);
    }
    return host;
}

Status HttpStatusToStatus(long code, const std::string& body) {
    if (code >= 200 && code < 300) {
        return Status::OK();
    }
    if (code == 404) {
        return Status::StoreNotFound("S3 object not found");
    }
    if (code == 403) {
        return Status::PermissionDenied("S3 permission denied: " + body);
    }
    if (code == 400) {
        return Status::InvalidArgument("S3 invalid request: " + body);
    }
    return Status::StoreInternal("S3 http status " + std::to_string(code) + ": " + body);
}

size_t WriteString(void* contents, size_t size, size_t nmemb, void* userp) {
    const size_t total = size * nmemb;
    static_cast<std::string*>(userp)->append(static_cast<const char*>(contents), total);
    return total;
}

std::vector<std::string> ExtractXmlKeys(const std::string& xml) {
    std::vector<std::string> keys;
    size_t pos = 0;
    while (true) {
        size_t begin = xml.find("<Key>", pos);
        if (begin == std::string::npos) {
            break;
        }
        begin += 5;
        size_t end = xml.find("</Key>", begin);
        if (end == std::string::npos) {
            break;
        }
        std::string key = xml.substr(begin, end - begin);
        size_t amp = 0;
        while ((amp = key.find("&amp;", amp)) != std::string::npos) {
            key.replace(amp, 5, "&");
            amp += 1;
        }
        keys.push_back(std::move(key));
        pos = end + 6;
    }
    return keys;
}

std::string HeaderValue(const std::string& headers, const std::string& name) {
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
    size_t pos = 0;
    while (pos < headers.size()) {
        size_t end = headers.find('\n', pos);
        if (end == std::string::npos) {
            end = headers.size();
        }
        std::string line = headers.substr(pos, end - pos);
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
            line.pop_back();
        }
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string key = line.substr(0, colon);
            std::transform(key.begin(), key.end(), key.begin(), ::tolower);
            if (key == lower_name) {
                std::string value = line.substr(colon + 1);
                while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
                    value.erase(value.begin());
                }
                return value;
            }
        }
        pos = end + 1;
    }
    return "";
}

bool ParseSize(const std::string& value, uint64_t* size) {
    if (value.empty()) {
        *size = 0;
        return true;
    }
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0') {
        return false;
    }
    *size = parsed;
    return true;
}

class CurlGlobal {
 public:
    CurlGlobal() { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~CurlGlobal() { curl_global_cleanup(); }
};

void EnsureCurlGlobal() {
    static CurlGlobal curl_global;
    (void)curl_global;
}

void CompleteBlobOperation(ScopedInvoker* done, Closure<void>* callback) {
    done->Release();
    if (callback == nullptr) {
        return;
    }
    if (byte::GetCurrentThread() != nullptr) {
        byte::InvokeInCurrentThread(callback);
    } else {
        callback->Run();
    }
}

class S3Blob : public Blob {
 public:
    S3Blob(S3Store* store, std::string uri, MetricsManager* metrics_manager, bool writable,
           std::string initial_data)
        : store_(store),
          uri_(std::move(uri)),
          writable_(writable),
          data_(std::move(initial_data)) {
        metrics_.Init(metrics_manager, uri_);
    }

    void Close() override {}

    void Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) override {
        ScopedInvoker done(callback);
        if (!writable_) {
            ctrl->set_status(Status::PermissionDenied("S3 blob is read-only"));
            CompleteBlobOperation(&done, callback);
            return;
        }
        metrics_.append_qps->get()->Increment();
        metrics_.append_throughput->get()->Add(size);
        ScopedLatency latency(metrics_.append_latency->get());
        data_.append(static_cast<const char*>(data), size);
        ctrl->set_status(store_->PutObject(uri_, data_));
        CompleteBlobOperation(&done, callback);
    }

    void Read(Controller* ctrl, size_t offset, void* data, size_t size,
              Closure<void>* callback) override {
        ScopedInvoker done(callback);
        if (size == 0) {
            ctrl->set_status(Status::OK());
            CompleteBlobOperation(&done, callback);
            return;
        }
        metrics_.read_qps->get()->Increment();
        metrics_.read_throughput->get()->Add(size);
        ScopedLatency latency(metrics_.read_latency->get());
        std::string body;
        Status status = store_->GetObjectRange(uri_, offset, size, &body);
        if (!status.ok()) {
            ctrl->set_status(status);
            CompleteBlobOperation(&done, callback);
            return;
        }
        if (body.size() != size) {
            ctrl->set_status(Status::OutOfRange("S3 range read returned short body"));
            CompleteBlobOperation(&done, callback);
            return;
        }
        if (size > 0) {
            std::memcpy(data, body.data(), size);
        }
        ctrl->set_status(Status::OK());
        CompleteBlobOperation(&done, callback);
    }

 private:
    S3Store* store_ = nullptr;
    std::string uri_;
    bool writable_ = false;
    std::string data_;
    BlobMetrics metrics_;
};

HttpResponse SendS3Request(const std::string& method, const std::string& uri,
                           const std::string& body, const std::vector<std::string>& extra_headers,
                           const std::string& query = "", bool allow_empty_key = false) {
    EnsureCurlGlobal();
    HttpResponse result;

    S3Uri parsed;
    Status parse_status = ParseS3Uri(uri, &parsed, allow_empty_key);
    if (!parse_status.ok()) {
        result.curl_code = CURLE_URL_MALFORMAT;
        result.body = parse_status.ToString();
        return result;
    }

    S3Config config = LoadConfig();
    if (config.endpoint.empty()) {
        result.curl_code = CURLE_URL_MALFORMAT;
        result.body = "TEMPORALSTORE_S3_ENDPOINT or AWS_ENDPOINT_URL_S3 is required";
        return result;
    }

    std::string canonical_uri = "/" + UriEncode(parsed.bucket, true);
    if (!parsed.key.empty()) {
        canonical_uri.push_back('/');
        canonical_uri += UriEncode(parsed.key, false);
    } else {
        canonical_uri.push_back('/');
    }
    const std::string url = config.endpoint + canonical_uri + (query.empty() ? "" : "?" + query);
    const std::string payload_hash = Sha256Hex(body);
    const std::string host = HostFromEndpoint(config.endpoint);
    std::string short_date;
    const std::string amz_date = HttpDate(&short_date);

    std::vector<std::string> headers;
    headers.push_back("host:" + host);
    headers.push_back("x-amz-content-sha256:" + payload_hash);
    headers.push_back("x-amz-date:" + amz_date);
    if (!config.session_token.empty()) {
        headers.push_back("x-amz-security-token:" + config.session_token);
    }
    for (const auto& header : extra_headers) {
        headers.push_back(header);
    }

    if (!config.unsigned_requests) {
        if (config.access_key.empty() || config.secret_key.empty()) {
            result.curl_code = CURLE_LOGIN_DENIED;
            result.body = "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required";
            return result;
        }
        std::vector<std::string> canonical_headers = headers;
        std::sort(canonical_headers.begin(), canonical_headers.end());
        std::string canonical_header_text;
        std::string signed_headers;
        for (const auto& header : canonical_headers) {
            size_t colon = header.find(':');
            if (colon == std::string::npos) {
                continue;
            }
            std::string name = header.substr(0, colon);
            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
            canonical_header_text += name + ":" + header.substr(colon + 1) + "\n";
            if (!signed_headers.empty()) {
                signed_headers += ";";
            }
            signed_headers += name;
        }

        const std::string canonical_request = method + "\n" + canonical_uri + "\n" + query + "\n" +
                                              canonical_header_text + "\n" + signed_headers + "\n" +
                                              payload_hash;
        const std::string credential_scope =
            short_date + "/" + config.region + "/s3/aws4_request";
        const std::string string_to_sign = "AWS4-HMAC-SHA256\n" + amz_date + "\n" +
                                           credential_scope + "\n" +
                                           Sha256Hex(canonical_request);
        const std::string k_secret = "AWS4" + config.secret_key;
        auto k_date = HmacSha256(reinterpret_cast<const unsigned char*>(k_secret.data()),
                                 k_secret.size(), short_date);
        auto k_region = HmacSha256(k_date, config.region);
        auto k_service = HmacSha256(k_region, "s3");
        auto k_signing = HmacSha256(k_service, "aws4_request");
        auto signature = HmacSha256(k_signing, string_to_sign);
        headers.push_back("Authorization: AWS4-HMAC-SHA256 Credential=" + config.access_key + "/" +
                          credential_scope + ", SignedHeaders=" + signed_headers +
                          ", Signature=" + Hex(signature.data(), signature.size()));
    }

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        result.curl_code = CURLE_FAILED_INIT;
        result.body = "curl_easy_init failed";
        return result;
    }
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl_guard(curl, curl_easy_cleanup);

    curl_slist* header_list = nullptr;
    for (const auto& header : headers) {
        header_list = curl_slist_append(header_list, header.c_str());
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> header_guard(
        header_list, curl_slist_free_all);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteString);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result.body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, WriteString);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &result.headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 30000L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    if (method == "HEAD") {
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    }
    if (method == "PUT" && !body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                         static_cast<curl_off_t>(body.size()));
    }

    result.curl_code = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.http_code);
    return result;
}

Status ResponseToStatus(const HttpResponse& response) {
    if (response.curl_code == CURLE_URL_MALFORMAT) {
        return Status::InvalidArgument(response.body);
    }
    if (response.curl_code == CURLE_LOGIN_DENIED) {
        return Status::PermissionDenied(response.body);
    }
    if (response.curl_code != CURLE_OK) {
        return Status::StoreInternal(curl_easy_strerror(response.curl_code));
    }
    return HttpStatusToStatus(response.http_code, response.body);
}

}  // namespace

S3Store::S3Store(std::string backend_name) : backend_name_(std::move(backend_name)) {}

Status S3Store::CheckCondition(const Condition& condition) {
    if (condition.name.empty()) {
        return Status::OK();
    }
    std::string body;
    Status status = GetObject(condition.name, &body);
    if (!status.ok()) {
        return status.IsStoreNotFound() ? Status::StoreConditionFailed("Condition missing")
                                        : status;
    }
    const std::string expected(condition.data.data(), condition.data.size());
    if (body != expected) {
        return Status::StoreConditionFailed("Condition changed");
    }
    return Status::OK();
}

Status S3Store::PutObject(const std::string& uri, const std::string& body) {
    HttpResponse response = SendS3Request("PUT", uri, body, {});
    Status status = ResponseToStatus(response);
    if (!status.ok()) {
        LOG_ERROR("S3 PUT failed").put("Backend", backend_name_).put("Uri", uri).put("Status",
                                                                                     status);
    }
    return status;
}

Status S3Store::GetObject(const std::string& uri, std::string* body) {
    HttpResponse response = SendS3Request("GET", uri, "", {});
    Status status = ResponseToStatus(response);
    if (!status.ok()) {
        return status;
    }
    *body = std::move(response.body);
    return Status::OK();
}

Status S3Store::GetObjectRange(const std::string& uri, size_t offset, size_t size,
                               std::string* body) {
    std::vector<std::string> headers;
    if (size > 0) {
        headers.push_back("range:bytes=" + std::to_string(offset) + "-" +
                          std::to_string(offset + size - 1));
    }
    HttpResponse response = SendS3Request("GET", uri, "", headers);
    Status status = ResponseToStatus(response);
    if (!status.ok()) {
        return status;
    }
    *body = std::move(response.body);
    return Status::OK();
}

Status S3Store::HeadObject(const std::string& uri, BlobStat* stat) {
    HttpResponse response = SendS3Request("HEAD", uri, "", {});
    Status status = ResponseToStatus(response);
    if (!status.ok()) {
        return status;
    }
    const std::string content_length = HeaderValue(response.headers, "content-length");
    uint64_t size = 0;
    if (!ParseSize(content_length, &size)) {
        return Status::DataLoss("S3 HEAD returned invalid content-length: " + content_length);
    }
    stat->size = size;
    return Status::OK();
}

Status S3Store::DeleteObject(const std::string& uri) {
    HttpResponse response = SendS3Request("DELETE", uri, "", {});
    return ResponseToStatus(response);
}

Status S3Store::CopyObject(const std::string& src_uri, const std::string& dst_uri) {
    S3Uri src;
    Status status = ParseS3Uri(src_uri, &src);
    if (!status.ok()) {
        return status;
    }
    std::vector<std::string> headers = {
        "x-amz-copy-source:/" + UriEncode(src.bucket, true) + "/" + UriEncode(src.key, false)};
    HttpResponse response = SendS3Request("PUT", dst_uri, "", headers);
    return ResponseToStatus(response);
}

void S3Store::SetCondition(Controller* ctrl, const std::string& uri, const ConditionData& data,
                           const SetConditionOptions& options) {
    Status status = CheckCondition(options.condition);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    ctrl->set_status(PutObject(uri, std::string(data.data(), data.size())));
}

void S3Store::StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) {
    std::string body;
    Status status = GetObject(uri, &body);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    if (body.size() != data->size()) {
        ctrl->set_status(Status::DataLoss("S3 condition object has invalid size"));
        return;
    }
    std::copy(body.begin(), body.end(), data->begin());
    ctrl->set_status(Status::OK());
}

void S3Store::List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) {
    S3Uri parsed;
    Status status = ParseS3Uri(path, &parsed, true);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    const std::string prefix = parsed.key;
    const std::string query = "list-type=2&prefix=" + UriEncode(prefix, true);
    const std::string list_uri = parsed.scheme + "://" + parsed.bucket + "/";
    HttpResponse response = SendS3Request("GET", list_uri, "", {}, query, true);
    status = ResponseToStatus(response);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    files->clear();
    for (const auto& key : ExtractXmlKeys(response.body)) {
        if (key.size() < prefix.size() || key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        BlobInfo info;
        info.name = key.substr(prefix.size());
        if (!info.name.empty()) {
            files->push_back(std::move(info));
        }
    }
    ctrl->set_status(Status::OK());
}

void S3Store::Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
                   Blob** blob) {
    Status status = CheckCondition(options.condition);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    const bool writable = options.mode == OpenMode::kWrite;
    std::string initial_data;
    if (writable) {
        Status get_status = GetObject(uri, &initial_data);
        if (!get_status.ok() && !get_status.IsStoreNotFound()) {
            ctrl->set_status(get_status);
            return;
        }
    }
    *blob = new S3Blob(this, uri, options.metrics_manager, writable, std::move(initial_data));
    ctrl->set_status(Status::OK());
}

void S3Store::Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) {
    Status status = CheckCondition(options.condition);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    ctrl->set_status(DeleteObject(uri));
}

void S3Store::Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) {
    Status status = CheckCondition(options.condition);
    ctrl->set_status(status);
}

void S3Store::Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
                   BlobStat* stat) {
    ctrl->set_status(HeadObject(uri, stat));
}

void S3Store::Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                     const RenameOptions& options) {
    Status status = CheckCondition(options.condition);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    status = CopyObject(src_uri, dst_uri);
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }
    ctrl->set_status(DeleteObject(src_uri));
}

}  // namespace stream
}  // namespace bcache2
