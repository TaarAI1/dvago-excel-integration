# Root-level Dockerfile — builds the API (backend) service.
# Build context: repo root.  Railway service Root Directory: / (repo root).

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libaio1t64 \
    wget \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# libaio symlink required by Oracle Instant Client on Debian Bookworm
RUN ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 \
           /usr/lib/x86_64-linux-gnu/libaio.so.1 || true

# Oracle Instant Client 19.22 (LTS)
RUN mkdir -p /opt/oracle \
    && wget --tries=3 --timeout=120 --no-verbose \
        "https://download.oracle.com/otn_software/linux/instantclient/1922000/instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip" \
        -O /tmp/ic.zip \
    && unzip -q /tmp/ic.zip -d /opt/oracle \
    && ICDIR=$(find /opt/oracle -maxdepth 1 -type d -name "instantclient_*" | head -1) \
    && mv "$ICDIR" /opt/oracle/instantclient \
    && rm /tmp/ic.zip

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

# Copy requirements first so pip layer is cached independently of source changes
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source — explicit path avoids BuildKit cache-key bug with COPY dir/
COPY backend/app ./app

EXPOSE 8000

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
