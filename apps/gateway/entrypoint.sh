#!/bin/sh
set -e

# Default values
export RATE_LIMIT_CALLS=${RATE_LIMIT_CALLS:-100}
export KVK_API_HOST=${KVK_API_HOST:-api.kvk.nl}

echo "=== Nginx Transparent Proxy ==="
echo "Rate limit: ${RATE_LIMIT_CALLS} requests/second (no burst)"
echo "Upstream: ${KVK_API_HOST}"
echo "==============================="

envsubst '
    ${RATE_LIMIT_CALLS}
    ${KVK_API_HOST}
' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

exec nginx -g 'daemon off;'
