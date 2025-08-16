FROM rust:1.82-alpine AS builder

RUN apk add --no-cache musl-dev gcc make

WORKDIR /app
COPY Cargo.toml ./
COPY gateway/ ./gateway/
COPY load-balancer/ ./load-balancer/
COPY worker/ ./worker/
COPY shared/ ./shared/
COPY queue/ ./queue/

RUN cargo build --release --bin queue

FROM alpine:3.18

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/queue /usr/local/bin/queue

CMD ["queue"]
