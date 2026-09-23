# syntax=docker/dockerfile:1.7
#
# Toolbelt image used by scripts/dev.sh and by CI. Every quality gate runs
# inside this image so a local run and a CI run use the same compiler and
# the same tool versions. Bump versions here, never ad hoc on a laptop.
# Same Debian release as the release build (Dockerfile), so a binary built here
# links against the glibc the production image ships.
FROM rust:1.98.1-bookworm

ARG MSRV=1.94.0
ARG BINSTALL_VERSION=1.23.0
ARG NEXTEST_VERSION=0.9.146
ARG DENY_VERSION=0.20.2
ARG LLVM_COV_VERSION=0.9.1
ARG MUTANTS_VERSION=27.1.0
ARG MACHETE_VERSION=0.9.2
ARG TYPOS_VERSION=1.50.2
ARG CYCLONEDX_VERSION=0.5.9

RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler clang lld cmake jq \
 && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy llvm-tools-preview \
 && rustup toolchain install "${MSRV}" --profile minimal

RUN curl -fsSL --proto '=https' --tlsv1.2 \
      "https://github.com/cargo-bins/cargo-binstall/releases/download/v${BINSTALL_VERSION}/cargo-binstall-$(uname -m)-unknown-linux-gnu.tgz" \
    | tar -xz -C /usr/local/cargo/bin \
 && cargo binstall --no-confirm --locked \
      "cargo-nextest@${NEXTEST_VERSION}" \
      "cargo-deny@${DENY_VERSION}" \
      "cargo-llvm-cov@${LLVM_COV_VERSION}" \
      "cargo-mutants@${MUTANTS_VERSION}" \
      "typos-cli@${TYPOS_VERSION}" \
      "cargo-cyclonedx@${CYCLONEDX_VERSION}" \
 && cargo install --locked "cargo-machete@${MACHETE_VERSION}" \
 && rm -rf /usr/local/cargo/registry

ENV CARGO_TERM_COLOR=always \
    CARGO_INCREMENTAL=0 \
    RUST_BACKTRACE=1
WORKDIR /work
