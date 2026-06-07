#!/usr/bin/env python3
from pathlib import Path

p = Path("/home/vj/bytekv-rocksdb-server/bytekv/tools/aws_smoke/main.cc")
s = p.read_text()
s = s.replace(
    'DEFINE_uint32(replica_count, 1, "table replica count");\n'
    'DEFINE_uint32(wait_seconds, 30, "seconds to wait for table readiness");',
    'DEFINE_uint32(replica_count, 1, "table replica count");\n'
    'DEFINE_uint32(wait_seconds, 30, "seconds to wait for table readiness");\n'
    'DEFINE_uint32(quota_gb, 1, "table/namespace quota in GB");\n'
    'DEFINE_uint32(partition_size_mb, 1024, "table partition size lower/upper bound in MB");',
)
s = s.replace(
    "req.mutable_options()->set_quota_in_gb(1);\n"
    "  auto code = master->GetNamespaceServiceRpcClient()->CreateNamespace(req, &resp);",
    "req.mutable_options()->set_quota_in_gb(FLAGS_quota_gb);\n"
    "  auto code = master->GetNamespaceServiceRpcClient()->CreateNamespace(req, &resp);",
)
s = s.replace(
    "req.mutable_options()->set_quota_in_gb(1);\n"
    "  req.mutable_options()->set_replica_count(FLAGS_replica_count);\n"
    "  req.mutable_options()->set_security(bytekv::TABLE_SECURITY_LEVEL_SERVER);\n"
    "  req.mutable_options()->set_partition_size_mb_lower(64);\n"
    "  req.mutable_options()->set_partition_size_mb_upper(64);",
    "req.mutable_options()->set_quota_in_gb(FLAGS_quota_gb);\n"
    "  req.mutable_options()->set_replica_count(FLAGS_replica_count);\n"
    "  req.mutable_options()->set_security(bytekv::TABLE_SECURITY_LEVEL_SERVER);\n"
    "  req.mutable_options()->set_partition_size_mb_lower(FLAGS_partition_size_mb);\n"
    "  req.mutable_options()->set_partition_size_mb_upper(FLAGS_partition_size_mb);",
)
p.write_text(s)
