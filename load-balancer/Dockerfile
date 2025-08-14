FROM rust:1.82-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml ./
COPY gateway/ ./gateway/
COPY load-balancer/ ./load-balancer/
COPY worker/ ./worker/
COPY shared/ ./shared/

RUN cargo build --release --bin load-balancer

FROM alpine:3.18

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/load-balancer /usr/local/bin/load-balancer

EXPOSE 9999

CMD ["load-balancer"]
