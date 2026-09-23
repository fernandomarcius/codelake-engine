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
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin codelake \
 && install -D target/release/codelake /out/codelake

# distroless/cc: glibc + libssl + CA certificates, no shell, no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
LABEL org.opencontainers.image.source="https://github.com/fernandomarcius/CodeLake" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.description="CodeLake federated SQL engine"
COPY --from=build --chown=65532:65532 /out/codelake /usr/local/bin/codelake
# 65532 = "nonroot" in distroless. Never run the engine as root.
USER 65532:65532
EXPOSE 5432 8080
ENTRYPOINT ["/usr/local/bin/codelake"]
CMD ["serve", "--config", "/etc/codelake/codelake.toml"]
