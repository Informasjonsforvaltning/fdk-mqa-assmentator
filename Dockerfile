FROM rust:latest AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang

COPY ./ ./
RUN cargo build --release


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*     

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /build/target/release/fdk-mqa-assmentator /fdk-mqa-assmentator

CMD ["/fdk-mqa-assmentator"]
