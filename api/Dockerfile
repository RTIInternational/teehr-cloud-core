# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Use HTTPS apt mirrors so builds work when plain HTTP egress is blocked.
RUN sed -i 's|http://deb.debian.org/debian|https://deb.debian.org/debian|g; s|http://deb.debian.org/debian-security|https://deb.debian.org/debian-security|g' /etc/apt/sources.list.d/debian.sources

# Apply Debian security updates; the base image lags behind them. No compilers:
# every dependency installs from a wheel, and a toolchain only adds CVEs.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml first for dependency installation
COPY pyproject.toml ./

# Copy application code
COPY src/ ./src/

# Install dependencies from pyproject.toml
RUN pip install --no-cache-dir .

# Fail the build if a runtime library a wheel needs is missing.
RUN python -c "import src.main"

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Run the application (production by default)
# Increase timeout to 300 seconds (5 minutes) for long-running queries
CMD ["gunicorn", "src.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--timeout", "300", "--graceful-timeout", "310", "--keep-alive", "30"]
