# Full TemporalStore (Rust) image — every service binary in one image.
#
#   docker build -f Dockerfile -t temporalstore:full .
#   docker run --rm -p 17101:17101 -p 17102:17102 temporalstore:full
#
# By default this runs a single node (metaserver + datanode), exactly like
# docker/Dockerfile.single-node, but the image also ships the proxy, service
# proxy, direct SDK, and native persistence workflow binaries for advanced use.
# For the smallest/fastest image, use docker/Dockerfile.single-node instead.
FROM rust:1.87-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
RUN cargo build --release -p temporalstore-rust

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --create-home temporalstore \
    && mkdir -p /var/lib/temporalstore \
    && chown temporalstore:temporalstore /var/lib/temporalstore
# The four service binaries plus the direct SDK and native persistence workflow.
COPY --from=builder /src/target/release/matrixark_rust_metaserver    /usr/local/bin/
COPY --from=builder /src/target/release/matrixark_rust_datanode      /usr/local/bin/
COPY --from=builder /src/target/release/matrixark_rust_proxy         /usr/local/bin/
COPY --from=builder /src/target/release/matrixark_rust_service_proxy /usr/local/bin/
COPY --from=builder /src/target/release/matrixark_rust_direct_sdk    /usr/local/bin/
COPY --from=builder /src/target/release/native_persistence_workflow  /usr/local/bin/
COPY docker/single-node-entrypoint.sh /usr/local/bin/single-node-entrypoint.sh
RUN chmod +x /usr/local/bin/single-node-entrypoint.sh
USER temporalstore
WORKDIR /var/lib/temporalstore
ENV TS_DATA_DIR=/var/lib/temporalstore \
    TS_META_PORT=17101 \
    TS_DATA_PORT=17102
EXPOSE 17101 17102
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
  CMD curl -fsS http://127.0.0.1:17102/health || exit 1
ENTRYPOINT ["/usr/local/bin/single-node-entrypoint.sh"]
