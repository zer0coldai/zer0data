FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy all project files
COPY . .

# Install runtime dependencies for ingestor
RUN uv pip install --system --no-cache \
    clickhouse-connect~=0.7.0 \
    pandas~=2.2 \
    polars~=0.20 \
    pyarrow~=14.0 \
    click~=8.1 \
    "numpy<2"

# Expose ingestor source package
ENV PYTHONPATH=/app/ingestor/src

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["python", "-m", "zer0data_ingestor.cli"]
CMD ["--help"]
