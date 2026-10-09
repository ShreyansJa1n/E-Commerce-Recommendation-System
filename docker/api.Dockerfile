# API image: only the `serving` dependency group (no Spark/JVM), ~200 MB.
FROM python:3.12.15-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.8.10 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --group serving --no-install-project
COPY src ./src
ENV PYTHONPATH=/app/src PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
RUN useradd --create-home --uid 10001 app
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"
CMD ["uvicorn", "recsys.serving.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--no-access-log"]
