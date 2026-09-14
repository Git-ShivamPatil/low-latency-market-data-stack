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
# `bench` is a workspace member and it does not live under crates/, so cargo
# refuses to load the workspace at all without it -- "failed to load manifest for
# workspace member /src/bench" -- before it gets as far as the three binaries
# this stage actually wants. Nothing here builds it; it just has to be readable.
#
# This is what broke the image at milestone 6, when the benchmark crate joined
# the workspace, and it stayed broken until milestone 9 because nothing rebuilt
# it in between. That is the argument for the CI job that runs `docker compose
# up -d`: an image nobody builds is an image nobody knows is broken.
COPY bench ./bench

# replay-service is here because docker-compose.yml names it as an entrypoint.
# It was missing, which meant `docker compose up -d` -- step one of the four
# commands the case study publishes -- started a container that could not run.
# Found by writing the test that runs those four commands.
RUN cargo build --release --bin matching-engine --bin feed-handler --bin replay-service \
    && strip target/release/matching-engine target/release/feed-handler \
             target/release/replay-service

# The C++ half: the FIX gateway and the risk service, which are two of the five
# nodes the case study draws. A separate stage because it needs a different
# toolchain entirely, and a Rust-only rebuild should not pay for cmake.
FROM debian:bookworm-slim AS cppbuild
RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ cmake ninja-build ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY cpp ./cpp
RUN cmake -S cpp -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    && cmake --build build --target fix-gateway risk-service fix-seqtool \
    && strip build/gateway/fix-gateway build/risk/risk-service build/gateway/fix-seqtool

FROM debian:bookworm-slim AS runtime
RUN useradd --system --create-home --uid 10001 mdstack
WORKDIR /app
COPY --from=build /src/target/release/matching-engine /usr/local/bin/
COPY --from=build /src/target/release/feed-handler /usr/local/bin/
COPY --from=build /src/target/release/replay-service /usr/local/bin/
COPY --from=cppbuild /src/build/gateway/fix-gateway /usr/local/bin/
COPY --from=cppbuild /src/build/risk/risk-service /usr/local/bin/
COPY --from=cppbuild /src/build/gateway/fix-seqtool /usr/local/bin/
COPY configs /app/configs
# The order path's rings and its durable stores. A named volume in compose, so
# they survive a restart -- which is the entire point of the sequence store and
# the order log.
RUN mkdir -p /app/state && chown mdstack:mdstack /app/state
USER mdstack
ENTRYPOINT ["matching-engine"]
CMD ["--config", "/app/configs/docker.toml"]
