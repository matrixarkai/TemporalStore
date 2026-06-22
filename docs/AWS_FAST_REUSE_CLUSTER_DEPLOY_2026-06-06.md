# AWS Fast Reuse-Cluster Deploy - 2026-06-06

## Cluster

No new cluster was created. Reused the existing AWS cluster:

| Role | Instance | Private IP | Public IP | Type |
| --- | --- | --- | --- | --- |
| meta/proxy/client | `i-05f55360d92c43908` | `10.70.1.161` | `35.92.109.186` | `t3.small` |
| data-01 | `i-0cfbef56e86551535` | `10.70.1.214` | `34.220.17.155` | `c7i.large` |
| data-02 | `i-04c93ad8271e5b64a` | `10.70.1.24` | `34.220.9.104` | `c7i.large` |

## Artifacts

Uploaded to the existing artifact bucket:

```text
s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-runtime-fast.tar.gz
s3://temporalstore-test-artifacts-657817560042-us-west-2/bytekv-runtime-fast.tar.gz
s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-missing-libs.tar.gz
```

Installed on all nodes:

```text
/opt/temporalstore/runtime
/opt/bytekv/runtime
```

## ByteKV Status

ByteKV was deployed and is running on the reused cluster.

| Component | Node | Ports |
| --- | --- | --- |
| `kvmaster` | meta | `26010`, `26011`, `26012` |
| `tso` | meta | `26020`, `26021`, `26022` |
| `kvproxy` | meta | `26030` |
| `partitionserver` | data-01 | `26040`, `26041`, `26042` |
| `partitionserver` | data-02 | `26041`, `26042`, `26043` |

Observed running processes:

```text
/opt/bytekv/bin/kvmaster --config=/opt/bytekv/conf/master.json
/opt/bytekv/bin/tso --config=/opt/bytekv/conf/tso.json
/opt/bytekv/bin/kvproxy --config=/opt/bytekv/conf/proxy.json
/opt/bytekv/bin/partitionserver --config=/opt/bytekv/conf/partitionserver.json --machine_info_file=/opt/bytekv/conf/machine_info.json
```

Notes:

- Data nodes initially missed `libatomic.so.1`; installed `libatomic1` and related runtime packages.
- Master logs show partition create calls reaching both data nodes successfully.

## TemporalStore Status

TemporalStore runtime was staged on all nodes but not started successfully.

The current `temporalstore-runtime-fast.tar.gz` artifact is not product-ready because it does not include the complete dynamic library closure needed by the binaries.

Fixed/installed during the attempt:

```text
libfmt8
libthrift-0.16.0
libboost-context1.74.0
libboost-regex1.74.0
libgoogle-glog0v5
libevent-2.1-7
locally built thrift 0.11 library
```

Remaining blocker:

```text
/opt/temporalstore/bin/bcache2-metaserver: undefined symbol: bwrite_conv
/opt/temporalstore/bin/bcache2-proxy: undefined symbol: bwrite_conv
```

Both release and debug builds of vendored Apache Thrift 0.11 were tested; neither exports `bwrite_conv`. This indicates the TemporalStore package is missing another legacy conversion/runtime library or must be rebuilt with that symbol linked into the binary.

## Product Rollout Lesson

For customer AWS rollout, TemporalStore packaging must include all required shared libraries and pass this check before upload:

```bash
LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib ldd -r bin/bcache2-metaserver
LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib ldd -r bin/bcache2-server
LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib ldd -r bin/bcache2-proxy
```

The rollout path should fail in CI if any `not found` or `undefined symbol` remains.
