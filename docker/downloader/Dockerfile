FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy code
COPY . .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["python", "download-kline.py"]
CMD ["--help"]
