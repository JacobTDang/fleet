FROM python:3.14-slim

# curl + jq: the `script` watcher kind runs shell commands in this container,
# and fetch-and-filter one-liners are its most common shape.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl jq ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Claude Code CLI for `claude` jobs and event handlers (native binary, no
# Node). Auth arrives at runtime via CLAUDE_CODE_OAUTH_TOKEN; tools stay off
# by default (see fleet/llm.py).
RUN curl -fsSL https://claude.ai/install.sh | bash
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

CMD ["fleet-worker"]
