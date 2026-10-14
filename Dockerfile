# syntax=docker/dockerfile:1
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir build && python -m build --wheel --outdir /dist

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 STA_LOG_FORMAT=json
# zstd/lz4 extras are included because Spark rolling event logs are commonly zstd-compressed
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl "zstandard>=0.22" "lz4>=4.0" && rm -f /tmp/*.whl \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin sta \
    && mkdir -p /data/eventlogs && chown sta /data/eventlogs
USER 10001
EXPOSE 8080
VOLUME ["/data/eventlogs"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status == 200 else 1)"
ENTRYPOINT ["sta"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8080", "--watch-dir", "/data/eventlogs"]
