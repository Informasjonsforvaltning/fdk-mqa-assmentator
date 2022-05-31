FROM rust:latest AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    clang

COPY ./ ./
RUN cargo build --release


FROM rust:latest

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /build/target/release/fdk-mqa-node-namer /fdk-mqa-url-checker

CMD ["/fdk-mqa-url-checker"]
