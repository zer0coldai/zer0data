FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy all project files
COPY . .

# Install package
RUN uv pip install --no-cache --system .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["zer0data-ingestor"]
CMD ["--help"]
