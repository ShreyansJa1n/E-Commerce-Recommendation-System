# API image: only the `serving` dependency group (no Spark/JVM).
# Docker Hub's official image via Google's mirror (avoids anonymous pull rate limits in CI).
FROM mirror.gcr.io/library/python:3.12.15-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.8.10 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock README.md ./
# --only-group: the serving group alone, *without* the project's own deps (pyspark etc.).
RUN uv sync --locked --only-group serving --no-install-project
COPY src ./src
ENV PYTHONPATH=/app/src PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 \
    RECSYS_LOG_FORMAT=json PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
RUN useradd --create-home --uid 10001 app
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"
# Fresh multiprocess metric dir per container start (stale worker files would double count).
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && mkdir -p \"$PROMETHEUS_MULTIPROC_DIR\" && exec uvicorn recsys.serving.app:app --host 0.0.0.0 --port 8000 --workers 4 --no-access-log"]
