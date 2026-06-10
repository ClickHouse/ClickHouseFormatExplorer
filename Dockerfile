# Build stage
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Runtime stage
FROM clickhouse/clickhouse-server:latest

# Install nginx and supervisor
RUN apt-get update && apt-get install -y nginx supervisor && rm -rf /var/lib/apt/lists/*

# Node runtime for the capture server (glibc binary copied from the official
# Debian-based node image; the capture scripts use only Node built-ins, so no
# node_modules are required). The clickhouse-server image is glibc-based too.
COPY --from=node:20-bookworm-slim /usr/local/bin/node /usr/local/bin/node

# clickhouse-client used by the capture proxy. The single `clickhouse` binary
# dispatches on argv[0], so a clickhouse-client symlink runs in client mode.
RUN ln -sf /usr/bin/clickhouse /usr/local/bin/clickhouse-client

# Copy built frontend
COPY --from=builder /app/dist /var/www/html

# Capture server + proxy harness (no dependencies beyond Node built-ins)
COPY scripts/native-proxy.mjs scripts/capture-middleware.mjs scripts/capture-server.mjs /app/scripts/

# nginx config template (rendered at start with the proxy user) + start scripts
COPY docker/nginx.conf.template /etc/nginx/nginx.conf.template
COPY docker/start-nginx.sh docker/start-capture.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-nginx.sh /usr/local/bin/start-capture.sh

# Copy ClickHouse user config (viewer = read-only, writer = read-write)
COPY docker/users.xml /etc/clickhouse-server/users.d/viewer.xml

# Copy supervisor config (runs ClickHouse + capture server + nginx)
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# READONLY=1 (default) serves the read-only viewer user; set READONLY=0 for an
# internal deployment that should also be able to INSERT.
ENV READONLY=1

# Expose only web port (ClickHouse and capture server are internal only)
EXPOSE 80

# Start supervisor (manages ClickHouse + capture server + nginx)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
