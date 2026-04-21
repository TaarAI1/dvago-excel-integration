# syntax=docker/dockerfile:1.4
FROM python:3.11-slim

WORKDIR /app

# System deps: compiler, PostgreSQL libs, and Oracle Instant Client prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libaio1t64 \
    wget \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# libaio1t64 is the Debian Bookworm name; create a symlink under the old name so
# Oracle Instant Client shared libs can find it at runtime.
RUN ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 \
           /usr/lib/x86_64-linux-gnu/libaio.so.1 || true

# ── Oracle Instant Client 19.22 (LTS) ────────────────────────────────────────
# 19.x is Long-Term Support and has the most reliable public download URL.
# Thick mode is required for Oracle DBs using legacy password verifiers (0x939).
RUN mkdir -p /opt/oracle \
    && wget --tries=3 --timeout=120 --no-verbose \
        "https://download.oracle.com/otn_software/linux/instantclient/1922000/instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip" \
        -O /tmp/ic.zip \
    && unzip -q /tmp/ic.zip -d /opt/oracle \
    && ICDIR=$(find /opt/oracle -maxdepth 1 -type d -name "instantclient_*" | head -1) \
    && mv "$ICDIR" /opt/oracle/instantclient \
    && rm /tmp/ic.zip \
    && echo "Oracle Instant Client installed:" \
    && ls /opt/oracle/instantclient | head -5

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

COPY --link requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Railway's BuildKit cannot COPY directories from the build context.
# Fetch source directly from the public GitHub repo instead.
ARG RAILWAY_GIT_COMMIT_SHA=main
RUN wget --tries=3 --timeout=120 -q \
        "https://github.com/TaarAI1/dvago-excel-integration/archive/${RAILWAY_GIT_COMMIT_SHA}.tar.gz" \
        -O /tmp/repo.tar.gz \
    && tar -xzf /tmp/repo.tar.gz -C /tmp \
    && mv /tmp/dvago-excel-integration-*/app /app/app \
    && rm -rf /tmp/repo.tar.gz /tmp/dvago-excel-integration-*

EXPOSE 8000

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
