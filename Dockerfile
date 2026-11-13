# PostgreSQL with pg_otel built for it and preloaded: the released image and
# the one the local integration tests use (see scripts/integration-tests.sh).
# Building inside the image keeps the extension and the server on the same
# glibc and PostgreSQL build.
ARG PG_MAJOR=18
# release, or debug for a faster build.
ARG BUILD_PROFILE=release

# One pinned base per supported major, so Renovate can track each tag and
# digest; PG_MAJOR selects one and BuildKit skips the others.
FROM postgres:18.7@sha256:9f4fd4aa918f52ae3d6f3b687f1de90d8f867cd4d520021545a9a50c120eae3b AS base-18

# hadolint and Trivy cannot resolve the ARG, so they take base-${PG_MAJOR}
# for an untagged image instead of a stage above.
# trivy:ignore:DS-0001
# hadolint ignore=DL3006
FROM base-${PG_MAJOR} AS base

FROM base AS build
ARG PG_MAJOR
ARG BUILD_PROFILE
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# Versions are not pinned: PGDG keeps only its latest packages, so the
# headers can be a newer minor than the server, which is ABI-compatible.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential ca-certificates curl clang libclang-dev libicu-dev pkg-config \
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
    case "${BUILD_PROFILE}" in \
      release) profile_flag="" ;; \
      debug) profile_flag="--debug" ;; \
      *) echo "BUILD_PROFILE must be release or debug, got '${BUILD_PROFILE}'" >&2; exit 1 ;; \
    esac \
 && cargo pgrx package ${profile_flag} --pg-config "$(command -v pg_config)" \
      --no-default-features --features pg${PG_MAJOR} --out-dir /package

FROM base
COPY --from=build /package/ /
COPY sql/setup.sql /usr/local/share/pg_otel/setup.sql
COPY docker/pg-otel-entrypoint.sh /usr/local/bin/pg-otel-entrypoint.sh
COPY docker/initdb.d/ /docker-entrypoint-initdb.d/
# The postgres user of the base image. Numeric, so that Kubernetes'
# runAsNonRoot can verify it.
USER 999:999
ENTRYPOINT ["pg-otel-entrypoint.sh"]
CMD ["postgres"]
