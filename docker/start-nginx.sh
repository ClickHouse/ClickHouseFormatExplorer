#!/bin/sh
# Render the nginx config, substituting the ClickHouse proxy user based on the
# READONLY toggle, then run nginx in the foreground.
#
#   READONLY=1 (default) -> viewer  (read-only)
#   READONLY=0           -> writer  (can INSERT / DDL)
set -e

if [ "${READONLY:-1}" = "0" ]; then
    CH_PROXY_USER=writer
else
    CH_PROXY_USER=viewer
fi

sed "s/__CH_PROXY_USER__/${CH_PROXY_USER}/g" \
    /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

exec /usr/sbin/nginx -g "daemon off;"
