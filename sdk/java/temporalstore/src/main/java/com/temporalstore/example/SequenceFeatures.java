package com.temporalstore.example;

import com.temporalstore.TemporalStoreClient;
import java.util.Arrays;
import java.util.List;

public final class SequenceFeatures {
    private SequenceFeatures() {}

    public static void main(String[] args) {
        TemporalStoreClient.Options options = new TemporalStoreClient.Options();
        options.libraryName = System.getenv().getOrDefault("TEMPORALSTORE_JAVA_LIB", "bcache2");
        options.metaserverAddr = args.length > 0 ? args[0] : "127.0.0.1:18200";
        options.namespaceName = args.length > 1 ? args[1] : "sdk_ns";
        options.tableName = args.length > 2 ? args[2] : "sdk_table";
        options.psm = "temporalstore.java.example";

        try (TemporalStoreClient client = TemporalStoreClient.connect(options)) {
            client.putString("java:user:42", "{\"tier\":\"gold\"}");
            System.out.println("profile=" + client.getString("java:user:42"));
            client.expire("java:user:42", 60000);
            System.out.println("ttl_ms=" + client.ttl("java:user:42"));

            client.hset("java:user:42:features", "ctr_7d", "0.042");
            System.out.println("ctr_7d=" + client.hget("java:user:42:features", "ctr_7d"));

            client.sadd("java:user:42:campaigns", "campaign_100");
            System.out.println("campaigns=" + client.smembers("java:user:42:campaigns"));

            String key = "java:user:42:sequence";
            client.addSequenceFeatureRows(
                    key,
                    Arrays.asList(
                            new TemporalStoreClient.SequenceFeatureRow(
                                    1700000000000L, 900L, 1, 31, 7000L),
                            new TemporalStoreClient.SequenceFeatureRow(
                                    1700000001000L, 901L, 3, 120, 7001L)));

            List<TemporalStoreClient.SequenceFeatureRow> rows =
                    client.querySequenceFeatureRows(
                            key,
                            1700000000000L,
                            1700000002000L,
                            10,
                            Arrays.asList(
                                    new TemporalStoreClient.FeatureFilter(
                                            "action_type",
                                            TemporalStoreClient.FeatureFilterOp.EQUAL,
                                            3)));
            System.out.println("rows=" + rows.size());

            String riskKey = "java:user:42:risk";
            client.riskIncrement(
                    riskKey,
                    1,
                    24 * 3600,
                    TemporalStoreClient.RiskPrecision.ONE_MINUTE,
                    "java-risk-1",
                    0);
            System.out.println(
                    "risk_count="
                            + client.riskCount(
                                    riskKey,
                                    TemporalStoreClient.RiskPrecision.ONE_MINUTE,
                                    -1,
                                    0,
                                    TemporalStoreClient.WindowUnit.HOUR));
        }
    }
}
