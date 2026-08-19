FROM python:3.12-slim

# curl + jq: the `script` watcher kind runs shell commands in this container,
# and fetch-and-filter one-liners are its most common shape.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl jq ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

CMD ["fleet-worker"]
