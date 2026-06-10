#!/bin/sh
# Start the native-protocol capture server. The ClickHouse user it connects as
# follows the READONLY toggle (same as the HTTP proxy):
#
#   READONLY=1 (default) -> viewer  (read-only; experimental settings come from
#                                     the profile, not per-query, so they are
#                                     not re-sent as client settings)
#   READONLY=0           -> writer  (can INSERT / DDL)
#
# Any of CH_USER / CH_PASSWORD / CH_NATIVE_HOST / CH_NATIVE_PORT can override.
set -e

if [ "${READONLY:-1}" = "0" ]; then
    DEFAULT_USER=writer
    : "${CAPTURE_EXPERIMENTAL_SETTINGS:=1}"
else
    DEFAULT_USER=viewer
    # A readonly user rejects per-query setting changes; rely on its profile.
    : "${CAPTURE_EXPERIMENTAL_SETTINGS:=0}"
fi

export CH_USER="${CH_USER:-$DEFAULT_USER}"
export CH_PASSWORD="${CH_PASSWORD:-}"
export CH_NATIVE_HOST="${CH_NATIVE_HOST:-127.0.0.1}"
export CH_NATIVE_PORT="${CH_NATIVE_PORT:-9000}"
export CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT:-clickhouse-client}"
export CAPTURE_EXPERIMENTAL_SETTINGS
export CAPTURE_BIND="${CAPTURE_BIND:-127.0.0.1}"
export CAPTURE_PORT="${CAPTURE_PORT:-8124}"

exec node /app/scripts/capture-server.mjs
