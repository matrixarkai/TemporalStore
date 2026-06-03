from temporalstore import (
    Client,
    FeatureFilter,
    FeatureFilterOp,
    Options,
    RiskPrecision,
    SequenceFeatureRow,
    WindowUnit,
)


options = Options(
    metaserver_addr="127.0.0.1:18200",
    namespace_name="sdk_ns",
    table_name="sdk_table",
)

with Client(options) as client:
    client.put_string("python:user:42", '{"tier":"gold"}')
    print("profile=", client.get_string("python:user:42"))
    client.expire("python:user:42", 60000)
    print("ttl_ms=", client.ttl("python:user:42"))

    client.hset("python:user:42:features", "ctr_7d", "0.042")
    print("ctr_7d=", client.hget("python:user:42:features", "ctr_7d"))

    client.sadd("python:user:42:campaigns", "campaign_100")
    print("campaigns=", client.smembers("python:user:42:campaigns"))

    key = "python:user:42:sequence"
    client.add_sequence_feature_rows(
        key,
        [
            SequenceFeatureRow(1700000000000, 900, 1, 31, 7000),
            SequenceFeatureRow(1700000001000, 901, 3, 120, 7001),
        ],
    )
    rows = client.query_sequence_feature_rows(
        key,
        1700000000000,
        1700000002000,
        10,
        [FeatureFilter("action_type", FeatureFilterOp.EQUAL, 3)],
    )
    print(rows)

    risk_key = "python:user:42:risk"
    client.risk_increment(risk_key, precision=RiskPrecision.ONE_MINUTE, uuid="python-risk-1")
    print("risk_count=", client.risk_count(risk_key, window_unit=WindowUnit.HOUR))
