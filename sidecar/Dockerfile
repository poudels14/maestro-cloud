FROM rust:1.87-alpine AS builder

RUN apk add --no-cache musl-dev protoc
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /app/target/release/maestro-sidecar /usr/local/bin/maestro-sidecar
ENTRYPOINT ["maestro-sidecar"]
