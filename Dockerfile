FROM rust:1.60 as builder
WORKDIR /src
COPY . .
RUN cargo install --path .

FROM alpine:latest
LABEL MAINTAINER="DCsunset"
COPY --from=builder /usr/local/cargo/bin/chord-dht-* /

