# PostgreSQL with pg_otel built for it, for running the integration tests
# locally (see scripts/integration-tests.sh). Building inside the image keeps
# the extension and the server on the same glibc and PostgreSQL build.
ARG PG_MAJOR=18

FROM postgres:${PG_MAJOR} AS build
ARG PG_MAJOR
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# Versions are not pinned: the base image tracks the distribution's latest
# PostgreSQL headers, which must match the server in the final stage.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential ca-certificates curl clang libclang-dev pkg-config \
      postgresql-server-dev-${PG_MAJOR} \
 && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/rustup CARGO_HOME=/cargo PATH=/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
# Keep in sync with the pgrx version in Cargo.toml.
RUN cargo install --locked cargo-pgrx --version =0.19.1 \
 && cargo pgrx init --pg${PG_MAJOR} "$(command -v pg_config)"
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo pgrx package --debug --pg-config "$(command -v pg_config)" \
      --no-default-features --features pg${PG_MAJOR} --out-dir /package

FROM postgres:${PG_MAJOR}
COPY --from=build /package/ /
# The official image's postgres user (uid/gid 999).
USER 999:999
