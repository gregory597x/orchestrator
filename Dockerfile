FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 orch && mkdir /data && chown orch /data
COPY --from=build /src/target/release/orchestrator /usr/local/bin/orchestrator
USER orch
VOLUME /data
EXPOSE 8780
ENV ORCH_CONFIG=/etc/orchestrator/config.toml
ENTRYPOINT ["/usr/local/bin/orchestrator"]
