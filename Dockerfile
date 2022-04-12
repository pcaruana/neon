# Build Postgres
#
#FROM zimg/rust:1.56 AS pg-build
FROM zenithdb/build:buster-20220309 AS pg-build
WORKDIR /pg

USER root

COPY vendor/postgres vendor/postgres
COPY Makefile Makefile

ENV BUILD_TYPE release
RUN set -e \
    && make -j $(nproc) -s postgres \
    && rm -rf tmp_install/build \
    && tar -C tmp_install -czf /postgres_install.tar.gz .

# Build zenith binaries
#
#FROM zimg/rust:1.56 AS build
FROM zenithdb/build:buster-20220309 AS build
ARG GIT_VERSION=local

ARG CACHEPOT_BUCKET=zenith-rust-cachepot
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV RUSTC_WRAPPER /usr/local/cargo/bin/cachepot

COPY --from=pg-build /pg/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY . .

# Show build caching stats to check if it was used in the end.
# Has to be the part of the same RUN since cachepot daemon is killed in the end of this RUN, loosing the compilation stats.
RUN rustup install 1.58
# RUN cargo +1.58 install --locked tokio-console
RUN RUSTFLAGS="--cfg tokio_unstable" cargo +1.58 build --release --bin pageserver --bin safekeeper --bin proxy && /usr/local/cargo/bin/cachepot -s

# Build final image
#
FROM debian:bullseye-slim
WORKDIR /data

RUN set -e \
    && apt-get update \
    && apt-get install -y \
        libreadline-dev \
        libseccomp-dev \
        openssl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && useradd -d /data zenith \
    && chown -R zenith:zenith /data

COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/pageserver /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/safekeeper /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/proxy      /usr/local/bin
# COPY --from=build --chown=zenith:zenith /usr/local/cargo/bin/tokio-console               /usr/local/bin

COPY --from=pg-build /pg/tmp_install/         /usr/local/
COPY --from=pg-build /postgres_install.tar.gz /data/

COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/data"]
USER zenith
EXPOSE 6400
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["pageserver"]
