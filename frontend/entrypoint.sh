#!/bin/sh
set -e

# Replace __BACKEND_URL__ placeholder with the actual BACKEND_URL env var
sed "s|__BACKEND_URL__|${BACKEND_URL}|g" \
    /etc/nginx/conf.d/default.conf.template \
    > /etc/nginx/conf.d/default.conf

exec nginx -g "daemon off;"
