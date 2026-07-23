package com.temporalstore;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class TemporalStoreClient implements AutoCloseable {
    private final NativeApi api;
    private Pointer handle;

    private TemporalStoreClient(NativeApi api, Pointer handle) {
        this.api = api;
        this.handle = handle;
    }

    public static TemporalStoreClient connect(Options options) {
        NativeApi api = Native.load(
                options.libraryName == null || options.libraryName.isEmpty()
                        ? "bcache2"
                        : options.libraryName,
                NativeApi.class);

        NativeOptions nativeOptions = new NativeOptions();
        api.temporalstore_options_init(nativeOptions);
        nativeOptions.read();
        nativeOptions.metaserver_addr = options.metaserverAddr;
        nativeOptions.metaserver_consul = options.metaserverConsul;
        nativeOptions.namespace_name = options.namespaceName;
        nativeOptions.table_name = options.tableName;
        nativeOptions.idc = options.idc;
        nativeOptions.host = options.host;
        nativeOptions.psm = options.psm;
        nativeOptions.log_dir = options.logDir;
        nativeOptions.log_level = options.logLevel;
        nativeOptions.io_timeout_ms = options.ioTimeoutMs;
        nativeOptions.connect_timeout_ms = options.connectTimeoutMs;
        nativeOptions.request_timeout_ms = options.requestTimeoutMs;
        nativeOptions.max_read_retries = options.maxReadRetries;
        nativeOptions.max_write_retries = options.maxWriteRetries;
        nativeOptions.retry_backoff_ms = options.retryBackoffMs;
        nativeOptions.max_feature_points_per_request = options.maxFeaturePointsPerRequest;
        nativeOptions.max_feature_query_count = options.maxFeatureQueryCount;
        nativeOptions.max_key_bytes = options.maxKeyBytes;
        nativeOptions.max_value_bytes = options.maxValueBytes;
        nativeOptions.pin_primary = options.pinPrimary ? 1 : 0;

        PointerByReference client = new PointerByReference();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_connect(nativeOptions, client, error);
        check(api, code, error);
        return new TemporalStoreClient(api, client.getValue());
    }

    @Override
    public void close() {
        if (handle == null) {
            return;
        }
        Pointer current = handle;
        handle = null;
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_close(current, error);
        check(api, code, error);
    }

    public void putString(String key, String value) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_put_string(requireOpen(), key, value, error);
        check(api, code, error);
    }

    public void putStringWithTtl(String key, String value, long ttlMs) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_put_string_with_ttl(requireOpen(), key, value, ttlMs, error);
        check(api, code, error);
    }

    public String getString(String key) {
        PointerByReference value = new PointerByReference();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_get_string(requireOpen(), key, value, error);
        check(api, code, error);
        Pointer out = value.getValue();
        try {
            return out == null ? "" : out.getString(0);
        } finally {
            api.temporalstore_free_string(out);
        }
    }

    public void deleteObject(String key) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_delete_object(requireOpen(), key, error);
        check(api, code, error);
    }

    public void expire(String key, long ttlMs) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_expire(requireOpen(), key, ttlMs, error);
        check(api, code, error);
    }

    public long ttl(String key) {
        long[] ttl = new long[1];
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_ttl(requireOpen(), key, ttl, error);
        check(api, code, error);
        return ttl[0];
    }

    public void hset(String key, String field, String value) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_hset(requireOpen(), key, field, value, error);
        check(api, code, error);
    }

    public String hget(String key, String field) {
        PointerByReference value = new PointerByReference();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_hget(requireOpen(), key, field, value, error);
        check(api, code, error);
        Pointer out = value.getValue();
        try {
            return out == null ? "" : out.getString(0);
        } finally {
            api.temporalstore_free_string(out);
        }
    }

    public void hdel(String key, String field) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_hdel(requireOpen(), key, field, error);
        check(api, code, error);
    }

    public void sadd(String key, String member) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_sadd(requireOpen(), key, member, error);
        check(api, code, error);
    }

    public List<String> smembers(String key) {
        NativeStringArray out = new NativeStringArray();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_smembers(requireOpen(), key, out, error);
        check(api, code, error);
        try {
            out.read();
            int count = checkedSize(out.count);
            if (count == 0 || out.values == null) {
                return Collections.emptyList();
            }
            Pointer[] raw = out.values.getPointerArray(0, count);
            List<String> values = new ArrayList<>(count);
            for (Pointer pointer : raw) {
                values.add(pointer == null ? "" : pointer.getString(0));
            }
            return values;
        } finally {
            api.temporalstore_string_array_free(out);
        }
    }

    public void addFeaturePoints(String key, List<FeaturePoint> points) {
        NativeFeaturePoint[] raw = featurePointArray(points);
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_add_feature_points(
                requireOpen(), key, pointerOf(raw), new NativeLong(points.size()), error);
        check(api, code, error);
    }

    public List<FeaturePoint> queryFeaturePoints(
            String key, long startTs, long endTs, long count, List<FeatureFilter> filters) {
        NativeFeatureFilter[] rawFilters = filterArray(filters);
        NativeFeaturePointArray out = new NativeFeaturePointArray();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_query_feature_points_with_filters(
                requireOpen(),
                key,
                startTs,
                endTs,
                count,
                pointerOf(rawFilters),
                new NativeLong(filters.size()),
                out,
                error);
        check(api, code, error);
        try {
            out.read();
            int rowCount = checkedSize(out.count);
            List<FeaturePoint> result = new ArrayList<>(rowCount);
            int itemSize = new NativeFeaturePoint().size();
            for (int i = 0; i < rowCount; i++) {
                NativeFeaturePoint point = new NativeFeaturePoint(out.points.share((long) i * itemSize));
                point.read();
                result.add(new FeaturePoint(point.timestamp, point.value));
            }
            return result;
        } finally {
            api.temporalstore_feature_point_array_free(out);
        }
    }

    public void addSequenceFeatureRows(String key, List<SequenceFeatureRow> rows) {
        NativeSequenceFeatureRow[] raw = sequenceRowArray(rows);
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_add_sequence_feature_rows(
                requireOpen(), key, pointerOf(raw), new NativeLong(rows.size()), error);
        check(api, code, error);
    }

    public List<SequenceFeatureRow> querySequenceFeatureRows(
            String key, long startTs, long endTs, long count, List<FeatureFilter> filters) {
        NativeFeatureFilter[] rawFilters = filterArray(filters);
        NativeSequenceFeatureRowArray out = new NativeSequenceFeatureRowArray();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_query_sequence_feature_rows(
                requireOpen(),
                key,
                startTs,
                endTs,
                count,
                pointerOf(rawFilters),
                new NativeLong(filters.size()),
                out,
                error);
        check(api, code, error);
        try {
            out.read();
            int rowCount = checkedSize(out.count);
            List<SequenceFeatureRow> result = new ArrayList<>(rowCount);
            int itemSize = new NativeSequenceFeatureRow().size();
            for (int i = 0; i < rowCount; i++) {
                NativeSequenceFeatureRow row =
                        new NativeSequenceFeatureRow(out.rows.share((long) i * itemSize));
                row.read();
                result.add(new SequenceFeatureRow(
                        row.timestamp, row.gid, row.action_type, row.duration, row.author_id));
            }
            return result;
        } finally {
            api.temporalstore_sequence_feature_row_array_free(out);
        }
    }

    public void addIpsInstance(
            String table,
            long uid,
            long timestampUs,
            int actionType,
            int logicalTable,
            List<IpsFeatureStat> features) {
        NativeIpsFeatureStat[] raw = ipsFeatureArray(features);
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_add_ips_instance(
                requireOpen(),
                table,
                uid,
                timestampUs,
                actionType,
                logicalTable,
                pointerOf(raw),
                new NativeLong(features.size()),
                error);
        check(api, code, error);
    }

    public List<IpsFeatureStat> queryIpsLastInstances(
            String table,
            long uid,
            int actionType,
            int logicalTable,
            int slot,
            int topK,
            long lastInstances) {
        NativeIpsFeatureArray out = new NativeIpsFeatureArray();
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_query_ips_last_instances(
                requireOpen(),
                table,
                uid,
                actionType,
                logicalTable,
                slot,
                topK,
                lastInstances,
                out,
                error);
        check(api, code, error);
        try {
            out.read();
            int rowCount = checkedSize(out.count);
            List<IpsFeatureStat> result = new ArrayList<>(rowCount);
            int itemSize = new NativeIpsFeatureStat().size();
            for (int i = 0; i < rowCount; i++) {
                NativeIpsFeatureStat item = new NativeIpsFeatureStat(out.features.share((long) i * itemSize));
                item.read();
                result.add(new IpsFeatureStat(
                        item.id, item.slot, item.has_slot != 0, item.type, item.v1, item.v2));
            }
            return result;
        } finally {
            api.temporalstore_ips_feature_array_free(out);
        }
    }

    public void riskIncrement(
            String key,
            long amount,
            long ttlSeconds,
            RiskPrecision precision,
            String uuid,
            long occurTimeSeconds) {
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_risk_increment(
                requireOpen(),
                key,
                amount,
                ttlSeconds,
                precision.value,
                uuid == null ? "" : uuid,
                occurTimeSeconds,
                error);
        check(api, code, error);
    }

    public long riskCount(
            String key,
            RiskPrecision precision,
            long windowStart,
            long windowEnd,
            WindowUnit windowUnit) {
        long[] count = new long[1];
        PointerByReference error = new PointerByReference();
        int code = api.temporalstore_risk_count(
                requireOpen(),
                key,
                precision.value,
                windowStart,
                windowEnd,
                windowUnit.value,
                count,
                error);
        check(api, code, error);
        return count[0];
    }

    private Pointer requireOpen() {
        if (handle == null) {
            throw new TemporalStoreException(0, "TemporalStore client is closed");
        }
        return handle;
    }

    private static void check(NativeApi api, int code, PointerByReference error) {
        if (code == 0) {
            return;
        }
        Pointer pointer = error == null ? null : error.getValue();
        String message = pointer == null ? "unknown TemporalStore error" : pointer.getString(0);
        if (pointer != null) {
            api.temporalstore_free_string(pointer);
        }
        throw new TemporalStoreException(code, message);
    }

    private static int checkedSize(NativeLong count) {
        long value = count.longValue();
        if (value > Integer.MAX_VALUE) {
            throw new TemporalStoreException(0, "native array is too large: " + value);
        }
        return (int) value;
    }

    private static Pointer pointerOf(Structure[] values) {
        return values.length == 0 ? Pointer.NULL : values[0].getPointer();
    }

    private static NativeFeatureFilter[] filterArray(List<FeatureFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return new NativeFeatureFilter[0];
        }
        NativeFeatureFilter[] raw = (NativeFeatureFilter[]) new NativeFeatureFilter().toArray(filters.size());
        for (int i = 0; i < filters.size(); i++) {
            FeatureFilter filter = filters.get(i);
            raw[i].field = filter.field;
            raw[i].op = filter.op.value;
            raw[i].value = filter.value;
            raw[i].write();
        }
        return raw;
    }

    private static NativeFeaturePoint[] featurePointArray(List<FeaturePoint> points) {
        if (points == null || points.isEmpty()) {
            return new NativeFeaturePoint[0];
        }
        NativeFeaturePoint[] raw = (NativeFeaturePoint[]) new NativeFeaturePoint().toArray(points.size());
        for (int i = 0; i < points.size(); i++) {
            FeaturePoint point = points.get(i);
            raw[i].timestamp = point.timestamp;
            raw[i].value = point.value;
            raw[i].write();
        }
        return raw;
    }

    private static NativeSequenceFeatureRow[] sequenceRowArray(List<SequenceFeatureRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new NativeSequenceFeatureRow[0];
        }
        NativeSequenceFeatureRow[] raw =
                (NativeSequenceFeatureRow[]) new NativeSequenceFeatureRow().toArray(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            SequenceFeatureRow row = rows.get(i);
            raw[i].timestamp = row.timestamp;
            raw[i].gid = row.gid;
            raw[i].action_type = row.actionType;
            raw[i].duration = row.duration;
            raw[i].author_id = row.authorId;
            raw[i].write();
        }
        return raw;
    }

    private static NativeIpsFeatureStat[] ipsFeatureArray(List<IpsFeatureStat> features) {
        if (features == null || features.isEmpty()) {
            return new NativeIpsFeatureStat[0];
        }
        NativeIpsFeatureStat[] raw = (NativeIpsFeatureStat[]) new NativeIpsFeatureStat().toArray(features.size());
        for (int i = 0; i < features.size(); i++) {
            IpsFeatureStat feature = features.get(i);
            raw[i].id = feature.id;
            raw[i].slot = feature.slot;
            raw[i].has_slot = feature.hasSlot ? 1 : 0;
            raw[i].type = feature.type;
            raw[i].v1 = feature.v1;
            raw[i].v2 = feature.v2;
            raw[i].write();
        }
        return raw;
    }

    public static final class Options {
        public String libraryName = "bcache2";
        public String metaserverAddr = "";
        public String metaserverConsul = "";
        public String namespaceName = "";
        public String tableName = "";
        public String idc = "vdc1";
        public String host = "127.0.0.1";
        public String psm = "temporalstore.java.client";
        public String logDir = "./";
        public int logLevel = 3;
        public int ioTimeoutMs = 1000;
        public int connectTimeoutMs = 1000;
        public int requestTimeoutMs = 5000;
        public int maxReadRetries = 1;
        public int maxWriteRetries = 0;
        public int retryBackoffMs = 2;
        public int maxFeaturePointsPerRequest = 1000;
        public long maxFeatureQueryCount = 5000;
        public long maxKeyBytes = 4096;
        public long maxValueBytes = 16L * 1024L * 1024L;
        public boolean pinPrimary = true;
    }

    public enum FeatureFilterOp {
        EQUAL(0),
        NOT_EQUAL(1),
        GREATER_THAN(2),
        LESS_THAN(3);

        private final int value;

        FeatureFilterOp(int value) {
            this.value = value;
        }
    }

    public enum RiskPrecision {
        ONE_SECOND(0),
        FIVE_SECONDS(1),
        TEN_SECONDS(2),
        ONE_MINUTE(3),
        FIVE_MINUTES(4),
        TEN_MINUTES(5),
        ONE_HOUR(6),
        ONE_DAY(7),
        ONE_MONTH(8);

        private final int value;

        RiskPrecision(int value) {
            this.value = value;
        }
    }

    public enum WindowUnit {
        SECOND(0),
        MINUTE(1),
        HOUR(2),
        DAY(3);

        private final int value;

        WindowUnit(int value) {
            this.value = value;
        }
    }

    public static final class FeatureFilter {
        public final String field;
        public final FeatureFilterOp op;
        public final long value;

        public FeatureFilter(String field, FeatureFilterOp op, long value) {
            this.field = field;
            this.op = op;
            this.value = value;
        }
    }

    public static final class FeaturePoint {
        public final long timestamp;
        public final String value;

        public FeaturePoint(long timestamp, String value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    public static final class SequenceFeatureRow {
        public final long timestamp;
        public final long gid;
        public final int actionType;
        public final int duration;
        public final long authorId;

        public SequenceFeatureRow(long timestamp, long gid, int actionType, int duration, long authorId) {
            this.timestamp = timestamp;
            this.gid = gid;
            this.actionType = actionType;
            this.duration = duration;
            this.authorId = authorId;
        }
    }

    public static final class IpsFeatureStat {
        public final long id;
        public final int slot;
        public final boolean hasSlot;
        public final int type;
        public final int v1;
        public final int v2;

        public IpsFeatureStat(long id, int slot, boolean hasSlot, int type, int v1, int v2) {
            this.id = id;
            this.slot = slot;
            this.hasSlot = hasSlot;
            this.type = type;
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    public static final class TemporalStoreException extends RuntimeException {
        public final int code;

        public TemporalStoreException(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    private interface NativeApi extends Library {
        void temporalstore_options_init(NativeOptions options);

        void temporalstore_free_string(Pointer value);

        void temporalstore_string_array_free(NativeStringArray array);

        void temporalstore_feature_point_array_free(NativeFeaturePointArray array);

        void temporalstore_sequence_feature_row_array_free(NativeSequenceFeatureRowArray array);

        void temporalstore_ips_feature_array_free(NativeIpsFeatureArray array);

        int temporalstore_connect(
                NativeOptions options, PointerByReference client, PointerByReference errorMessage);

        int temporalstore_close(Pointer client, PointerByReference errorMessage);

        int temporalstore_put_string(
                Pointer client, String key, String value, PointerByReference errorMessage);

        int temporalstore_put_string_with_ttl(
                Pointer client, String key, String value, long ttlMs, PointerByReference errorMessage);

        int temporalstore_get_string(
                Pointer client, String key, PointerByReference value, PointerByReference errorMessage);

        int temporalstore_delete_object(Pointer client, String key, PointerByReference errorMessage);

        int temporalstore_expire(Pointer client, String key, long ttlMs, PointerByReference errorMessage);

        int temporalstore_ttl(Pointer client, String key, long[] ttlMs, PointerByReference errorMessage);

        int temporalstore_hset(
                Pointer client, String key, String field, String value, PointerByReference errorMessage);

        int temporalstore_hget(
                Pointer client, String key, String field, PointerByReference value, PointerByReference errorMessage);

        int temporalstore_hdel(Pointer client, String key, String field, PointerByReference errorMessage);

        int temporalstore_sadd(Pointer client, String key, String member, PointerByReference errorMessage);

        int temporalstore_smembers(
                Pointer client, String key, NativeStringArray members, PointerByReference errorMessage);

        int temporalstore_add_feature_points(
                Pointer client, String key, Pointer points, NativeLong count, PointerByReference errorMessage);

        int temporalstore_query_feature_points_with_filters(
                Pointer client,
                String key,
                long startTs,
                long endTs,
                long count,
                Pointer filters,
                NativeLong filterCount,
                NativeFeaturePointArray points,
                PointerByReference errorMessage);

        int temporalstore_add_sequence_feature_rows(
                Pointer client, String key, Pointer rows, NativeLong count, PointerByReference errorMessage);

        int temporalstore_query_sequence_feature_rows(
                Pointer client,
                String key,
                long startTs,
                long endTs,
                long count,
                Pointer filters,
                NativeLong filterCount,
                NativeSequenceFeatureRowArray rows,
                PointerByReference errorMessage);

        int temporalstore_add_ips_instance(
                Pointer client,
                String table,
                long uid,
                long timestampUs,
                int actionType,
                int logicalTable,
                Pointer features,
                NativeLong featureCount,
                PointerByReference errorMessage);

        int temporalstore_query_ips_last_instances(
                Pointer client,
                String table,
                long uid,
                int actionType,
                int logicalTable,
                int slot,
                int topK,
                long lastInstances,
                NativeIpsFeatureArray features,
                PointerByReference errorMessage);

        int temporalstore_risk_increment(
                Pointer client,
                String key,
                long amount,
                long ttlSeconds,
                int precision,
                String uuid,
                long occurTimeSeconds,
                PointerByReference errorMessage);

        int temporalstore_risk_count(
                Pointer client,
                String key,
                int precision,
                long windowStart,
                long windowEnd,
                int windowUnit,
                long[] count,
                PointerByReference errorMessage);
    }

    public static class NativeOptions extends Structure {
        public String metaserver_addr;
        public String metaserver_consul;
        public String namespace_name;
        public String table_name;
        public String idc;
        public String host;
        public String psm;
        public String log_dir;
        public int log_level;
        public int io_timeout_ms;
        public int connect_timeout_ms;
        public int request_timeout_ms;
        public int max_read_retries;
        public int max_write_retries;
        public int retry_backoff_ms;
        public int max_feature_points_per_request;
        public long max_feature_query_count;
        public long max_key_bytes;
        public long max_value_bytes;
        public int pin_primary;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(
                    "metaserver_addr",
                    "metaserver_consul",
                    "namespace_name",
                    "table_name",
                    "idc",
                    "host",
                    "psm",
                    "log_dir",
                    "log_level",
                    "io_timeout_ms",
                    "connect_timeout_ms",
                    "request_timeout_ms",
                    "max_read_retries",
                    "max_write_retries",
                    "retry_backoff_ms",
                    "max_feature_points_per_request",
                    "max_feature_query_count",
                    "max_key_bytes",
                    "max_value_bytes",
                    "pin_primary");
        }
    }

    public static class NativeStringArray extends Structure {
        public NativeLong count;
        public Pointer values;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("count", "values");
        }
    }

    public static class NativeFeaturePoint extends Structure {
        public long timestamp;
        public String value;

        public NativeFeaturePoint() {}

        public NativeFeaturePoint(Pointer pointer) {
            super(pointer);
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("timestamp", "value");
        }
    }

    public static class NativeFeaturePointArray extends Structure {
        public NativeLong count;
        public Pointer points;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("count", "points");
        }
    }

    public static class NativeFeatureFilter extends Structure {
        public String field;
        public int op;
        public long value;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("field", "op", "value");
        }
    }

    public static class NativeSequenceFeatureRow extends Structure {
        public long timestamp;
        public long gid;
        public int action_type;
        public int duration;
        public long author_id;

        public NativeSequenceFeatureRow() {}

        public NativeSequenceFeatureRow(Pointer pointer) {
            super(pointer);
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("timestamp", "gid", "action_type", "duration", "author_id");
        }
    }

    public static class NativeSequenceFeatureRowArray extends Structure {
        public NativeLong count;
        public Pointer rows;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("count", "rows");
        }
    }

    public static class NativeIpsFeatureStat extends Structure {
        public long id;
        public int slot;
        public int has_slot;
        public int type;
        public int v1;
        public int v2;

        public NativeIpsFeatureStat() {}

        public NativeIpsFeatureStat(Pointer pointer) {
            super(pointer);
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("id", "slot", "has_slot", "type", "v1", "v2");
        }
    }

    public static class NativeIpsFeatureArray extends Structure {
        public NativeLong count;
        public Pointer features;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("count", "features");
        }
    }
}
