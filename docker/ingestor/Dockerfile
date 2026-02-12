FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY pyproject.toml ./
RUN uv pip install --no-cache . --no-dev

# Copy code
COPY . .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["zer0data-ingestor"]
CMD ["--help"]
