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

# Map R2_* env vars to rclone config env vars.
# env_file loads R2_* into the container; rclone reads RCLONE_CONFIG_R2_*.
export RCLONE_CONFIG_R2_TYPE="${RCLONE_CONFIG_R2_TYPE:-s3}"
export RCLONE_CONFIG_R2_PROVIDER="${RCLONE_CONFIG_R2_PROVIDER:-Cloudflare}"
export RCLONE_CONFIG_R2_ACCESS_KEY_ID="${RCLONE_CONFIG_R2_ACCESS_KEY_ID:-$R2_ACCESS_KEY_ID}"
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="${RCLONE_CONFIG_R2_SECRET_ACCESS_KEY:-$R2_SECRET_ACCESS_KEY}"
export RCLONE_CONFIG_R2_ENDPOINT="${RCLONE_CONFIG_R2_ENDPOINT:-$R2_ENDPOINT}"
export RCLONE_CONFIG_R2_NO_CHECK_BUCKET="${RCLONE_CONFIG_R2_NO_CHECK_BUCKET:-true}"

exec python /app/sync/sync.py "$@"
