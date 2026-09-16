# Base: official PostgreSQL 15 image (Debian-based, already has the
# PGDG apt repo configured) + the wal2json logical-decoding plugin
# installed as an official .deb package. This is more reliable than
# depending on a third-party image bundling the plugin for us.
FROM postgres:15

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-15-wal2json \
    && rm -rf /var/lib/apt/lists/*
