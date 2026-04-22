#!/bin/sh
set -e

# Default values
export RATE_LIMIT_CALLS=${RATE_LIMIT_CALLS:-100}
export KVK_API_HOST=${KVK_API_HOST:-api.kvk.nl}

echo "=== Nginx Transparent Proxy ==="
echo "Rate limit: ${RATE_LIMIT_CALLS} requests/second (no burst)"
echo "Upstream: ${KVK_API_HOST}"
echo "==============================="

# Wacht tot DNS de upstream kan resolven — nginx parst proxy_pass at config-time
# en crasht hard bij "host not found". Op native Linux is de resolver bij container
# start niet altijd direct beschikbaar (v1.7.2-regressie).
echo "Waiting for DNS to resolve ${KVK_API_HOST}..."
i=0
until getent hosts "${KVK_API_HOST}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 30 ]; then
        echo "ERROR: DNS resolve for ${KVK_API_HOST} failed after 30s" >&2
        exit 1
    fi
    sleep 1
done
echo "DNS OK"

envsubst '
    ${RATE_LIMIT_CALLS}
    ${KVK_API_HOST}
' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

exec nginx -g 'daemon off;'
