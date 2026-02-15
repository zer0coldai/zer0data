FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy all project files
COPY . .

# Install runtime dependencies for ingestor
RUN uv pip install --system --no-cache \
    clickhouse-connect~=0.11.0 \
    pandas~=2.3 \
    click~=8.3 \
    numpy~=2.4

# Expose ingestor source package
ENV PYTHONPATH=/app/ingestor/src

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["python", "-m", "zer0data_ingestor.cli"]
CMD ["--help"]
