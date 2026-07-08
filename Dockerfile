# ECPG Gateway – CUPS + Python 3.12, Multi-Arch (amd64/arm64)
FROM python:3.12-slim

# Automatische Versionierung: von der CI als build-arg injiziert (Default = dev).
ARG ECPG_VERSION=0.0.0+dev
LABEL org.opencontainers.image.title="einsatzcockpit-gateway" \
      org.opencontainers.image.version="${ECPG_VERSION}"

ENV PYTHONUNBUFFERED=1 \
    ECPG_DATA_DIR=/data \
    ECPG_VERSION=${ECPG_VERSION} \
    TZ=Europe/Vienna

# CUPS (driverless/IPP-Everywhere) + Discovery-Runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        cups cups-client cups-ipp-utils libcups2-dev gcc \
        avahi-daemon libnss-mdns \
        tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY ecpg ./ecpg
RUN pip install --no-cache-dir . && pip install --no-cache-dir pycups zeroconf

# Headless-Chromium für Leaflet-Karten-Druck (page.pdf). `--with-deps` zieht die
# nötigen System-Bibliotheken; das Browser-Binary landet in /root/.cache/ms-playwright.
RUN pip install --no-cache-dir playwright \
    && playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

VOLUME ["/data"]
EXPOSE 8631

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8631/healthz').read(); sys.exit(0)" || exit 1

# CUPS-Daemon + Agent (einfacher Supervisor via Shell)
COPY docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
