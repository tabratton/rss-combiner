FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef

FROM chef AS planner
WORKDIR /plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
WORKDIR /work
COPY --from=planner /plan/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN apt-get update && apt-get install -y mold
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /work/target/release/rss-combiner /usr/local/bin
RUN apt-get update && apt-get install -y openssl ca-certificates
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rss-combiner"]