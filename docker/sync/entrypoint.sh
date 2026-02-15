#!/bin/sh
set -e

# Copy SSH config from mounted volume and fix permissions.
# Docker volume mounts preserve host ownership which causes
# "Bad owner or permissions" errors with SSH.
if [ -d /tmp/.ssh-mount ]; then
    mkdir -p /root/.ssh
    # Use plain cp (not cp -a) so files are owned by root, not the host user.
    cp /tmp/.ssh-mount/* /root/.ssh/ 2>/dev/null || true
    chown -R root:root /root/.ssh
    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/* 2>/dev/null || true
    [ -f /root/.ssh/config ] && chmod 644 /root/.ssh/config
    [ -f /root/.ssh/known_hosts ] && chmod 644 /root/.ssh/known_hosts
    for f in /root/.ssh/*.pub; do [ -f "$f" ] && chmod 644 "$f"; done
fi

exec python /app/sync/sync.py "$@"
