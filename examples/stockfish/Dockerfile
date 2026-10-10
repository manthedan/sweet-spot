# Build from the repository root: docker build -f examples/stockfish/Dockerfile -t sweetspot-stockfish .
FROM python:3.11-slim AS stockfish
ARG TARGETARCH
ARG STOCKFISH_REF=sf_18
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git tar build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
      amd64) \
        curl -LfsS "https://github.com/official-stockfish/Stockfish/releases/download/${STOCKFISH_REF}/stockfish-ubuntu-x86-64.tar" -o /tmp/stockfish.tar; \
        mkdir /tmp/sf; tar -xf /tmp/stockfish.tar -C /tmp/sf --strip-components=1; \
        install -m 0755 /tmp/sf/stockfish-ubuntu-x86-64 /usr/local/bin/stockfish; \
        rm /tmp/sf/stockfish-ubuntu-x86-64; mv /tmp/sf /opt/stockfish-release ;; \
      arm64) \
        git clone --depth 1 --branch "$STOCKFISH_REF" https://github.com/official-stockfish/Stockfish.git /tmp/Stockfish; \
        make -C /tmp/Stockfish/src -j"$(nproc)" build ARCH=armv8; \
        install -m 0755 /tmp/Stockfish/src/stockfish /usr/local/bin/stockfish; \
        make -C /tmp/Stockfish/src clean; rm -rf /tmp/Stockfish/.git; \
        mv /tmp/Stockfish /opt/stockfish-release ;; \
      *) echo "Unsupported architecture" >&2; exit 2 ;; \
    esac \
    && printf 'uci\nquit\n' | /usr/local/bin/stockfish | grep -q 'Stockfish 18'

FROM python:3.11-slim
COPY --from=stockfish /usr/local/bin/stockfish /usr/local/bin/stockfish
# Keep the GPL license and corresponding source supplied by the tagged release.
COPY --from=stockfish /opt/stockfish-release/ /opt/stockfish-release/
COPY . /opt/sweetspot
RUN python -m pip install --no-cache-dir /opt/sweetspot
WORKDIR /work
COPY examples/stockfish/label.py examples/stockfish/sample.jsonl /work/
ENV STOCKFISH_BIN=/usr/local/bin/stockfish
# No ENTRYPOINT: Batch supplies ["sweetspot", "worker"].
