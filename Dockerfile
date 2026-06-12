FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY public ./public
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/camerafodder /app/camerafodder
COPY --from=builder /app/public /app/public

ENV PORT=8080
ENV AUTH_STORE_PATH=/tmp/camerafodder/auth.json
EXPOSE 8080

CMD ["/app/camerafodder"]
