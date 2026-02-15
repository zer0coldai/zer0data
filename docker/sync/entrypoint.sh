#!/bin/sh
set -e

# Copy SSH config from mounted volume and fix permissions.
# Docker volume mounts preserve host ownership which causes
# "Bad owner or permissions" errors with SSH.
if [ -d /tmp/.ssh-mount ]; then
    mkdir -p /root/.ssh
    cp -a /tmp/.ssh-mount/* /root/.ssh/ 2>/dev/null || true
    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/* 2>/dev/null || true
    [ -f /root/.ssh/config ] && chmod 644 /root/.ssh/config
    [ -f /root/.ssh/known_hosts ] && chmod 644 /root/.ssh/known_hosts
    [ -f /root/.ssh/*.pub ] && chmod 644 /root/.ssh/*.pub 2>/dev/null || true
fi

exec python /app/sync/sync.py "$@"
