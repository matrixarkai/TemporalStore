package com.temporalstore;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TemporalStoreProxyClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Options options;
    private final HttpClient client;
    private final String endpoint;

    public TemporalStoreProxyClient(Options options) {
        this.options = options;
        this.endpoint = trimTrailingSlash(options.endpoint);
        this.client = HttpClient.newBuilder().connectTimeout(options.timeout).build();
    }

    public void putString(String key, String value) {
        Map<String, Object> body = keyBody(key);
        body.put("value", bytesValue(value));
        post("/ProxyService/Set", body);
    }

    public void putStringWithTtl(String key, String value, long ttlMs) {
        Map<String, Object> body = keyBody(key);
        body.put("value", bytesValue(value));
        body.put("ttl_ms", ttlMs);
        post("/ProxyService/SetEx", body);
    }

    public String getString(String key) {
        JsonNode data = post("/ProxyService/Get", keyBody(key));
        return stringValue(data.path("value"));
    }

    public void deleteObject(String key) {
        post("/ProxyService/Delete", keyBody(key));
    }

    public void expire(String key, long ttlMs) {
        Map<String, Object> body = keyBody(key);
        body.put("ttl_ms", ttlMs);
        post("/ProxyService/Expire", body);
    }

    public long ttl(String key) {
        JsonNode data = post("/ProxyService/Ttl", keyBody(key));
        return data.path("value").asLong();
    }

    public void hset(String key, String field, String value) {
        Map<String, Object> body = keyBody(key);
        body.put("field", field);
        body.put("value", bytesValue(value));
        post("/ProxyService/HSet", body);
    }

    public String hget(String key, String field) {
        Map<String, Object> body = keyBody(key);
        body.put("field", field);
        JsonNode data = post("/ProxyService/HGet", body);
        return stringValue(data.path("value"));
    }

    public void hdel(String key, String field) {
        Map<String, Object> body = keyBody(key);
        body.put("field", field);
        post("/ProxyService/HDel", body);
    }

    public void sadd(String key, String member) {
        Map<String, Object> body = keyBody(key);
        body.put("member", bytesValue(member));
        post("/ProxyService/SAdd", body);
    }

    public List<String> smembers(String key) {
        JsonNode data = post("/ProxyService/SMembers", keyBody(key));
        return MAPPER.convertValue(data.path("members"), new TypeReference<List<String>>() {});
    }

    public void addFeaturePoints(
            String key, List<TemporalStoreClient.FeaturePoint> points) {
        Map<String, Object> body = keyBody(key);
        List<Map<String, Object>> encodedPoints = new ArrayList<>();
        if (points != null) {
            for (TemporalStoreClient.FeaturePoint point : points) {
                encodedPoints.add(encodeFeaturePoint(point));
            }
        }
        body.put("points", encodedPoints);
        post("/ProxyService/FeatureAdd", body);
    }

    public List<TemporalStoreClient.FeaturePoint> queryFeaturePoints(
            String key,
            long startTs,
            long endTs,
            long count,
            List<TemporalStoreClient.FeatureFilter> filters) {
        JsonNode data = post("/ProxyService/FeatureQuery", queryBody(key, startTs, endTs, count, filters));
        List<TemporalStoreClient.FeaturePoint> points = new ArrayList<>();
        for (JsonNode point : data.path("points")) {
            points.add(
                    new TemporalStoreClient.FeaturePoint(
                            point.path("timestamp").asLong(), point.path("value").asText()));
        }
        return points;
    }

    public void addSequenceFeatureRows(
            String key, List<TemporalStoreClient.SequenceFeatureRow> rows) {
        Map<String, Object> body = keyBody(key);
        List<Map<String, Object>> encodedRows = new ArrayList<>();
        if (rows != null) {
            for (TemporalStoreClient.SequenceFeatureRow row : rows) {
                encodedRows.add(encodeSequenceRow(row));
            }
        }
        body.put("rows", encodedRows);
        post("/ProxyService/SequenceAdd", body);
    }

    public List<TemporalStoreClient.SequenceFeatureRow> querySequenceFeatureRows(
            String key,
            long startTs,
            long endTs,
            long count,
            List<TemporalStoreClient.FeatureFilter> filters) {
        JsonNode data = post("/ProxyService/SequenceQuery", queryBody(key, startTs, endTs, count, filters));
        List<TemporalStoreClient.SequenceFeatureRow> rows = new ArrayList<>();
        for (JsonNode row : data.path("rows")) {
            rows.add(
                    new TemporalStoreClient.SequenceFeatureRow(
                            row.path("timestamp").asLong(),
                            row.path("gid").asLong(),
                            row.path("action_type").asInt(),
                            row.path("duration").asInt(),
                            row.path("author_id").asLong()));
        }
        return rows;
    }

    public void addIpsInstance(
            String ipsTable,
            long uid,
            long timestampUs,
            int actionType,
            int logicalTable,
            List<TemporalStoreClient.IpsFeatureStat> features) {
        Map<String, Object> body = new HashMap<>();
        body.put("namespace", options.namespaceName);
        body.put("table_name", options.tableName);
        body.put("ips_table", ipsTable);
        body.put("uid", uid);
        body.put("timestamp_us", timestampUs);
        body.put("action_type", actionType);
        body.put("logical_table", logicalTable);
        List<Map<String, Object>> encodedFeatures = new ArrayList<>();
        if (features != null) {
            for (TemporalStoreClient.IpsFeatureStat feature : features) {
                encodedFeatures.add(encodeIpsFeature(feature));
            }
        }
        body.put("features", encodedFeatures);
        post("/ProxyService/IpsAdd", body);
    }

    public List<TemporalStoreClient.IpsFeatureStat> queryIpsLastInstances(
            String ipsTable,
            long uid,
            int actionType,
            int logicalTable,
            int slot,
            int topK,
            long lastInstances) {
        Map<String, Object> body = new HashMap<>();
        body.put("namespace", options.namespaceName);
        body.put("table_name", options.tableName);
        body.put("ips_table", ipsTable);
        body.put("uid", uid);
        body.put("action_type", actionType);
        body.put("logical_table", logicalTable);
        body.put("slot", slot);
        body.put("top_k", topK);
        body.put("last_instances", lastInstances);
        JsonNode data = post("/ProxyService/IpsQueryLast", body);
        List<TemporalStoreClient.IpsFeatureStat> features = new ArrayList<>();
        for (JsonNode feature : data.path("features")) {
            features.add(
                    new TemporalStoreClient.IpsFeatureStat(
                            feature.path("id").asLong(),
                            feature.path("slot").asInt(),
                            feature.path("has_slot").asBoolean(true),
                            feature.path("type").asInt(),
                            feature.path("v1").asInt(),
                            feature.path("v2").asInt()));
        }
        return features;
    }

    public void riskIncrement(
            String key,
            long amount,
            long ttlSeconds,
            TemporalStoreClient.RiskPrecision precision,
            String uuid,
            long occurTimeSeconds) {
        Map<String, Object> body = keyBody(key);
        body.put("amount", amount);
        body.put("ttl_seconds", ttlSeconds);
        body.put("precision", precision.ordinal());
        body.put("uuid", uuid == null ? "" : uuid);
        body.put("occur_time_seconds", occurTimeSeconds);
        post("/ProxyService/RiskIncrement", body);
    }

    public long riskCount(
            String key,
            TemporalStoreClient.RiskPrecision precision,
            long windowStart,
            long windowEnd,
            TemporalStoreClient.WindowUnit windowUnit) {
        Map<String, Object> body = keyBody(key);
        body.put("precision", precision.ordinal());
        body.put("window_start", windowStart);
        body.put("window_end", windowEnd);
        body.put("window_unit", windowUnit.ordinal());
        JsonNode data = post("/ProxyService/RiskCount", body);
        return data.path("count").asLong();
    }

    @Override
    public void close() {}

    private JsonNode post(String path, Map<String, Object> body) {
        try {
            String requestBody = MAPPER.writeValueAsString(body);
            HttpRequest.Builder builder =
                    HttpRequest.newBuilder()
                            .uri(URI.create(endpoint + path))
                            .timeout(options.timeout)
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody));
            if (options.apiKey != null && !options.apiKey.isEmpty()) {
                builder.header("Authorization", "Bearer " + options.apiKey);
            }
            HttpResponse<String> response =
                    client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode envelope = MAPPER.readTree(response.body());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new TemporalStoreClient.TemporalStoreException(
                        response.statusCode(), envelope.path("message").asText());
            }
            if (envelope.has("status")) {
                JsonNode status = envelope.path("status");
                boolean statusOk = status.isObject()
                        ? status.path("ok").asBoolean(false)
                        : "ok".equals(status.asText());
                if (!statusOk) {
                    throw new TemporalStoreClient.TemporalStoreException(
                            response.statusCode(), status.path("message").asText(status.asText()));
                }
                JsonNode proxyResponse = envelope.path("response");
                return proxyResponse.isMissingNode() || proxyResponse.isNull()
                        ? MAPPER.createObjectNode()
                        : proxyResponse;
            }
            if (!envelope.path("ok").asBoolean(false)) {
                throw new TemporalStoreClient.TemporalStoreException(
                        envelope.path("code").asInt(), envelope.path("message").asText());
            }
            JsonNode data = envelope.path("data");
            return data.isMissingNode() || data.isNull() ? MAPPER.createObjectNode() : data;
        } catch (IOException e) {
            throw new TemporalStoreClient.TemporalStoreException(0, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TemporalStoreClient.TemporalStoreException(0, e.getMessage());
        }
    }

    private Map<String, Object> keyBody(String key) {
        Map<String, Object> body = new HashMap<>();
        body.put("namespace", options.namespaceName);
        body.put("table_name", options.tableName);
        body.put("key", key);
        return body;
    }

    private Map<String, Object> queryBody(
            String key,
            long startTs,
            long endTs,
            long count,
            List<TemporalStoreClient.FeatureFilter> filters) {
        Map<String, Object> body = keyBody(key);
        body.put("start_ms", startTs);
        body.put("end_ms", endTs);
        body.put("count", count);
        List<Map<String, Object>> encodedFilters = new ArrayList<>();
        if (filters != null) {
            for (TemporalStoreClient.FeatureFilter filter : filters) {
                Map<String, Object> item = new HashMap<>();
                item.put("field", filter.field);
                item.put("op", filter.op.ordinal());
                item.put("value", filter.value);
                encodedFilters.add(item);
            }
        }
        body.put("filters", encodedFilters);
        return body;
    }

    private static Map<String, Object> encodeFeaturePoint(TemporalStoreClient.FeaturePoint point) {
        Map<String, Object> item = new HashMap<>();
        item.put("timestamp", point.timestamp);
        item.put("value", point.value);
        return item;
    }

    private static Map<String, Object> encodeSequenceRow(TemporalStoreClient.SequenceFeatureRow row) {
        Map<String, Object> item = new HashMap<>();
        item.put("timestamp", row.timestamp);
        item.put("gid", row.gid);
        item.put("action_type", row.actionType);
        item.put("duration", row.duration);
        item.put("author_id", row.authorId);
        return item;
    }

    private static Map<String, Object> encodeIpsFeature(TemporalStoreClient.IpsFeatureStat feature) {
        Map<String, Object> item = new HashMap<>();
        item.put("id", feature.id);
        item.put("slot", feature.slot);
        item.put("has_slot", feature.hasSlot);
        item.put("type", feature.type);
        item.put("v1", feature.v1);
        item.put("v2", feature.v2);
        return item;
    }

    private static String trimTrailingSlash(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static List<Integer> bytesValue(String value) {
        List<Integer> bytes = new ArrayList<>();
        for (byte item : (value == null ? "" : value).getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            bytes.add(Byte.toUnsignedInt(item));
        }
        return bytes;
    }

    private static String stringValue(JsonNode value) {
        if (value == null || value.isMissingNode() || value.isNull()) {
            return "";
        }
        if (value.isArray()) {
            byte[] bytes = new byte[value.size()];
            for (int i = 0; i < value.size(); i++) {
                bytes[i] = (byte) value.get(i).asInt();
            }
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return value.asText();
    }

    public static final class Options {
        public String endpoint = "http://127.0.0.1:8080";
        public String namespaceName = "";
        public String tableName = "";
        public String apiKey = "";
        public Duration timeout = Duration.ofSeconds(5);
    }
}
