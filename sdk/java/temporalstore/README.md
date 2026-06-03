# TemporalStore Java SDK

This SDK uses JNA to call the stable native C ABI exported by `libbcache2`.

## Build

```bash
cd sdk/java/temporalstore
mvn package
```

## Runtime

Set the native library path before running customer code:

```bash
export LD_LIBRARY_PATH=/path/to/temporalstore/lib:$LD_LIBRARY_PATH
```

For the local Debug build, the native library is usually:

```text
output/sdk/lib/libbcache2d.so
```

Use `Options.libraryName = "bcache2d"` for that local debug library.

## Example

```java
TemporalStoreClient.Options options = new TemporalStoreClient.Options();
options.libraryName = "bcache2d";
options.metaserverAddr = "127.0.0.1:18200";
options.namespaceName = "sdk_ns";
options.tableName = "sdk_table";

try (TemporalStoreClient client = TemporalStoreClient.connect(options)) {
    client.putString("user:42", "{\"tier\":\"gold\"}");
    String profile = client.getString("user:42");
}
```
