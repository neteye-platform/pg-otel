# PostgreSQL with pg_otel built for it, for running the integration tests
# locally (see scripts/integration-tests.sh). Building inside the image keeps
# the extension and the server on the same glibc and PostgreSQL build.
ARG PG_MAJOR=18

FROM postgres:${PG_MAJOR} AS build
ARG PG_MAJOR
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
