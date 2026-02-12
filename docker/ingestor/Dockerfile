FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy all project files
COPY . .

# Install package (using root pyproject.toml)
RUN uv pip install --system --no-cache .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["zer0data-ingestor"]
CMD ["--help"]
