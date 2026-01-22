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

# Copy built frontend
COPY --from=builder /app/dist /var/www/html

# Copy nginx config (proxies /clickhouse to ClickHouse)
COPY docker/nginx.conf /etc/nginx/nginx.conf

# Copy ClickHouse user config (read-only viewer user)
COPY docker/users.xml /etc/clickhouse-server/users.d/viewer.xml

# Copy supervisor config (runs both nginx and ClickHouse)
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose only web port (ClickHouse internal only)
EXPOSE 80

# Start supervisor (manages nginx + ClickHouse)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
