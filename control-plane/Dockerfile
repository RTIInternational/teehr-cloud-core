FROM python:3.12-slim

WORKDIR /app

# Use HTTPS apt mirrors so builds work when plain HTTP egress is blocked.
RUN sed -i 's|http://deb.debian.org/debian|https://deb.debian.org/debian|g; s|http://deb.debian.org/debian-security|https://deb.debian.org/debian-security|g' /etc/apt/sources.list.d/debian.sources

# Apply Debian security updates; the base image lags behind them. No compilers:
# every dependency installs from a wheel.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Dependencies first so code changes don't bust the install layer.
COPY pyproject.toml ./
RUN pip install --no-cache-dir .

# Run as a non-root user. Files are owned by it so Garden's local sync can write.
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin controlplane
COPY --chown=10001:10001 src/ ./src/
COPY --chown=10001:10001 alembic.ini ./
COPY --chown=10001:10001 migrations/ ./migrations/
USER 10001

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Fail the build if a runtime library a wheel needs is missing.
RUN python -c "import src.main"

EXPOSE 8000

CMD ["gunicorn", "src.main:app", "-w", "2", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
