FROM ghcr.io/astral-sh/uv@sha256:b46b03ddfcfbf8f547af7e9eaefdf8a39c8cebcba7c98858d3162bd28cf536f6 AS uv
FROM python@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /opt/archive
COPY pyproject.toml uv.lock ./
COPY archive ./archive
COPY migrations ./migrations
RUN uv sync --frozen --no-dev --python /usr/local/bin/python \
    && useradd --uid 10001 --create-home archive \
    && mkdir -p /var/lib/archive-media /var/lib/archive-exports \
    && chown archive:archive /var/lib/archive-media /var/lib/archive-exports
USER 10001:10001
ENTRYPOINT ["/opt/archive/.venv/bin/python", "-m", "archive", "--config", "/run/archive/settings.toml"]
CMD ["api", "--host", "0.0.0.0", "--port", "8610"]
