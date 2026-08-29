# Build both binaries and ship them on a small runtime image.
#
# The workspace's .cargo/config.toml sets `-C target-cpu=native`, which is right
# for a measured run on a known host and wrong for a container image that may be
# built and run on different CPUs. RUSTFLAGS is cleared here deliberately: an
# image that SIGILLs on someone else's machine is worse than a slightly slower
# one, and any published number comes from a native host build anyway.

FROM rust:1.98-slim-bookworm AS build
ENV RUSTFLAGS=""
WORKDIR /src

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY schema ./schema

RUN cargo build --release --bin matching-engine --bin feed-handler \
    && strip target/release/matching-engine target/release/feed-handler

FROM debian:bookworm-slim AS runtime
RUN useradd --system --create-home --uid 10001 mdstack
WORKDIR /app
COPY --from=build /src/target/release/matching-engine /usr/local/bin/
COPY --from=build /src/target/release/feed-handler /usr/local/bin/
COPY configs /app/configs
USER mdstack
ENTRYPOINT ["matching-engine"]
CMD ["--config", "/app/configs/docker.toml"]
