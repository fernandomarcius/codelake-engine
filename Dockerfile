# syntax=docker/dockerfile:1.7
#
# Production image. Base images are pinned by digest: a tag can be re-pointed,
# a digest cannot. Renovate/Dependabot bumps these digests through PRs, which
# run every gate again.

FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
WORKDIR /src
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler cmake clang lld \
 && rm -rf /var/lib/apt/lists/*
COPY . .
# Empty = the release profile as written. Machines too small to link with LTO
# can pass --build-arg CARGO_PROFILE_RELEASE_LTO=off (never for a release).
ARG CARGO_PROFILE_RELEASE_LTO
# Optional cargo features, e.g. `codelake-server/distributed` (M4, ADR 0011).
ARG CARGO_FEATURES=""
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    { [ -n "$CARGO_PROFILE_RELEASE_LTO" ] || unset CARGO_PROFILE_RELEASE_LTO; } \
 && cargo build --locked --release --bin codelake ${CARGO_FEATURES:+--features "$CARGO_FEATURES"} \
 && install -D target/release/codelake /out/codelake \
 && install -D "$(ldconfig -p | awk '/libz\.so\.1 / {print $NF; exit}')" /out/lib/libz.so.1

# distroless/cc: glibc + libssl + CA certificates, no shell, no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
LABEL org.opencontainers.image.source="https://github.com/fernandomarcius/CodeLake" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.description="CodeLake federated SQL engine"
COPY --from=build --chown=65532:65532 /out/codelake /usr/local/bin/codelake
# zlib is linked dynamically by the MySQL driver (its compression crate) and is not part of
# distroless/cc. Same Debian release as the base, so the ABI matches.
COPY --from=build /out/lib/libz.so.1 /usr/lib/libz.so.1
# Fails the image build when any shared library is missing.
RUN ["/usr/local/bin/codelake", "--version"]
# 65532 = "nonroot" in distroless. Never run the engine as root.
USER 65532:65532
EXPOSE 5432 8080
ENTRYPOINT ["/usr/local/bin/codelake"]
CMD ["serve", "--config", "/etc/codelake/codelake.toml"]
