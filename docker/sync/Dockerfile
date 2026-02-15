FROM python:3.12-slim

WORKDIR /app

# Install rsync and openssh-client (for rsync over SSH)
RUN apt-get update && \
    apt-get install -y --no-install-recommends rsync openssh-client && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy all project files
COPY . .

# Install runtime dependencies (ingestor deps + sync deps)
RUN uv pip install --system --no-cache \
    clickhouse-connect~=0.7.0 \
    pandas~=2.2 \
    polars~=0.20 \
    pyarrow~=14.0 \
    click~=8.1 \
    "numpy<2" \
    pyyaml>=6.0

# Expose ingestor and sync source packages
ENV PYTHONPATH=/app/ingestor/src:/app/sync

# Data directory
VOLUME ["/data"]

# Copy entrypoint script
COPY docker/sync/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
