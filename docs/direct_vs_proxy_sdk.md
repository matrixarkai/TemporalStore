# Direct SDKs And Proxy SDKs

TemporalStore should ship two customer SDK families.

## Direct SDKs

Direct SDKs embed the native routing client in the application process.

```text
App -> direct SDK -> metaserver topology -> data servers
```

Use direct SDKs when:

- the application is latency sensitive;
- the deployment can ship the native shared library or link the C++ client;
- the service wants client-side routing, retries, and primary/read-replica policy;
- the language/runtime can safely load the native library.

Current direct SDKs:

- C++: native client, `sdk/cpp`
- Go: cgo over C ABI, `sdk/go/temporalstore`
- Java: JNA over C ABI, `sdk/java/temporalstore`
- Python: `ctypes` over C ABI, `sdk/python/temporalstore`
- Rust: FFI over C ABI, `sdk/rust/temporalstore`

## Proxy SDKs

Proxy SDKs are pure-language HTTP clients. The application talks to a nearby
TemporalStore proxy, and the proxy owns native routing, topology refresh, retries,
auth, observability, and connection pooling.

```text
App -> proxy SDK -> TemporalStore HTTP proxy -> direct native client -> data servers
```

Use proxy SDKs when:

- customers want zero native dependencies;
- Python/Java runtime loading of the native library is risky;
- multi-tenant auth, quotas, request logging, and policy enforcement should be centralized;
- an edge/sidecar proxy is operationally easier than shipping native clients everywhere;
- minor extra network hop latency is acceptable.

Current proxy SDKs:

- Python: standard library HTTP/JSON, `sdk/python/temporalstore/proxy_client.py`
- Go: standard library HTTP/JSON, `sdk/go/temporalstore/proxy_client.go`. The
  direct cgo client is behind the `temporalstore_direct` build tag so proxy-only
  applications do not link the native library.
- Java: Java 11 HTTP client plus Jackson, `sdk/java/temporalstore/.../TemporalStoreProxyClient.java`
- Rust: stdlib HTTP/JSON, `sdk/rust/temporalstore` with `--no-default-features --features proxy`.

## Existing Internal Proxy

The existing `src/proxy` binary is a brpc/Thrift proxy. It is useful for internal
RPC deployments, but it is not the public proxy SDK contract. The customer-facing
proxy SDK contract is HTTP/JSON so every language can use standard runtime
libraries.

## HTTP Response Envelope

All proxy responses use:

```json
{
  "ok": true,
  "code": 0,
  "message": "",
  "data": {}
}
```

If `ok=false`, SDKs raise/return an error with `code` and `message`.

## Endpoint Contract

Common request fields:

```json
{
  "namespace": "sdk_ns",
  "table": "sdk_table",
  "key": "user:42"
}
```

Endpoints:

| Endpoint | Purpose |
|---|---|
| `POST /ProxyService/Set` | set string value |
| `POST /ProxyService/SetEx` | set string value with `ttl_ms` |
| `POST /ProxyService/Get` | get string value |
| `POST /ProxyService/Delete` | delete object |
| `POST /ProxyService/Expire` | set object TTL |
| `POST /ProxyService/Ttl` | read object TTL |
| `POST /ProxyService/HSet` | set hash field |
| `POST /ProxyService/HGet` | get hash field |
| `POST /ProxyService/HDel` | delete hash field |
| `POST /ProxyService/SAdd` | add set member |
| `POST /ProxyService/SMembers` | list set members |
| `POST /ProxyService/FeatureAdd` | add raw feature points |
| `POST /ProxyService/FeatureQuery` | query raw feature points |
| `POST /ProxyService/SequenceAdd` | add typed sequence rows |
| `POST /ProxyService/SequenceQuery` | query typed sequence rows |
| `POST /ProxyService/IpsAdd` | add IPS instance |
| `POST /ProxyService/IpsQueryLast` | query IPS last instances |
| `POST /ProxyService/RiskIncrement` | increment risk counter |
| `POST /ProxyService/RiskCount` | query risk window counter |

## Direct vs Proxy Tradeoff

| Area | Direct SDK | Proxy SDK |
|---|---|---|
| Latency | Lowest | One extra hop |
| Dependencies | Native library / cgo / JNA / ctypes | Pure language HTTP |
| Topology | In-process | Centralized in proxy |
| Ops policy | Per application | Centralized |
| Auth/quotas | App-side or infra-side | Proxy-side |
| Best fit | high-QPS backend services | customer apps, serverless, notebooks, Python/Java teams |

## Recommendation

Ship both:

- Direct SDKs for performance-sensitive production services.
- Proxy SDKs as the default developer/customer onboarding path.

For Python specifically, proxy SDK should be the default until a release-safe native
shared library is packaged, because the local debug shared object is not safe for
late in-process loading.
