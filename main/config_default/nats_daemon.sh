#!/bin/bash
set -e

# Environment variables
if [ ! -z "$NATS_TOKEN" ]; then
    echo "NATS_TOKEN is defined: $NATS_TOKEN"
    export NATS_TOKEN=${NATS_TOKEN}
else
    echo "NATS_TOKEN is not defined"
    export NATS_TOKEN=my_default_token
fi


# Start NATS in the background
# /usr/local/bin/nats-server --jetstream --store_dir /var/log/idds --auth ${NATS_TOKEN} &
/usr/local/bin/nats-server --addr 0.0.0.0 --jetstream --store_dir /var/log/idds --auth ${NATS_TOKEN} &

# Wait for NATS to be ready
echo "Waiting for NATS to start..."
until nats --server nats://127.0.0.1:4222 --token ${NATS_TOKEN} account info >/dev/null 2>&1; do
    sleep 1
done
echo "NATS is up!"

# Create the JetStream stream
nats --server nats://127.0.0.1:4222 --token ${NATS_TOKEN} stream add event_stream \
    --subjects="event.*" \
    --replicas=1 \
    --storage=memory \
    --retention=work \
    --discard=old \
    --max-age=10m \
    --dupe-window=5m \
    --max-msgs=-1 \
    --max-msgs-per-subject=-1 \
    --no-allow-rollup \
    --max-bytes=-1 \
    --max-msg-size=-1 \
    --no-deny-delete \
    --no-deny-purge || true

# Wait for NATS to exit (keep it in foreground)
wait

