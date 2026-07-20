# syntax=docker/dockerfile:1

# Builder: full glibc rust image so `ring`/rustls compile without hunting
# down build-essential by hand. Cache mounts keep `cargo build` incremental
# across image rebuilds without a hand-rolled dummy-crate dependency stage.
FROM rust:1-bookworm AS builder
WORKDIR /build

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin db-headless-mcp -p db-headless-server && \
    cp target/release/db-headless-mcp /build/db-headless-mcp

# Runtime: debian-slim, not scratch/distroless, because the CA bundle
# (ca-certificates) is needed for rustls-native-certs to verify TLS when
# talking to a ClickHouse HTTPS endpoint or a verify_identity Postgres.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin db-headless \
    && mkdir -p /data \
    && chown db-headless:db-headless /data

COPY --from=builder /build/db-headless-mcp /usr/local/bin/db-headless-mcp

USER db-headless
WORKDIR /home/db-headless
VOLUME ["/data"]

# DB_HEADLESS_MCP_TOKEN has no default here on purpose: the binary itself
# refuses to start `--http` without it (see crates/server/src/main.rs). Pass
# it at `docker run`/compose time from a secret store, never bake it in.
# DB_HEADLESS_MASTER_KEY is likewise unset by default: connection profile
# storage (save_connection_profile etc) stays disabled until an operator
# opts in, same reasoning.
ENV DB_HEADLESS_MCP_BIND=0.0.0.0:8787
ENV DB_HEADLESS_DATA_DIR=/data
ENV RUST_LOG=info

EXPOSE 8787

ENTRYPOINT ["db-headless-mcp"]
CMD ["--http"]
