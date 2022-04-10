FROM rust:1.60 as builder
WORKDIR /src
COPY . .

FROM alpine:latest
LABEL MAINTAINER="DCsunset"
COPY --from=builder /usr/local/cargo/bin/chord-dht-* /

