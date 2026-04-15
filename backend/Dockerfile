FROM python:3.11-slim

WORKDIR /app

# System deps: PostgreSQL client libs + Oracle Instant Client prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libaio1t64 \
    wget \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client Basic (thick mode for oracledb)
# Update the version variables below if a newer release is available.
ARG OIC_VERSION=21.14.0.0.0
ARG OIC_BUILD=2114000
RUN mkdir -p /opt/oracle \
    && wget -q "https://download.oracle.com/otn_software/linux/instantclient/${OIC_BUILD}/instantclient-basiclite-linux.x64-${OIC_VERSION}dbru.zip" \
        -O /tmp/instantclient.zip \
    && unzip -q /tmp/instantclient.zip -d /opt/oracle \
    && mv /opt/oracle/instantclient_21_14 /opt/oracle/instantclient \
    && rm /tmp/instantclient.zip

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
