# docker/Dockerfile
FROM rust:1.82-slim AS build
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src

# build fully optimized; keep panic=abort in Cargo.toml
RUN cargo build --release --locked

# Distroless has dynamic loader + libc
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/target/release/batonics-challenge /batonics-challenge
EXPOSE 8080 9000
USER nonroot:nonroot
ENTRYPOINT ["/batonics-challenge"]
