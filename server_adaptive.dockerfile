FROM rust:1.95-slim-trixie AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
WORKDIR /app
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p benchmark --bin server --features adaptive

FROM debian:trixie-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/server /usr/local/bin
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/server"]
