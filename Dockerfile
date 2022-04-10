FROM rust:1.60 as build-env
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM gcr.io/distroless/cc
LABEL MAINTAINER="DCsunset"
COPY --from=build-env /app/target/release/chord-dht-* /usr/bin/

