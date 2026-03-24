#!/bin/sh
# Initialise both Redis clusters (main and saga) with 3 masters + 3 replicas each.
# Runs once on first boot via the cluster-init Docker Compose service.

set -e

PASS="redis"
TIMEOUT=60

wait_for_node() {
  host=$1
  deadline=$(($(date +%s) + TIMEOUT))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if redis-cli -h "$host" -p 6379 --no-auth-warning -a "$PASS" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: $host did not become ready in ${TIMEOUT}s" >&2
  return 1
}

is_cluster_initialized() {
  host=$1
  # Check if the cluster state is 'ok' and if it knows other nodes.
  # This avoids attempting to create a cluster that already exists.
  if redis-cli -h "$host" -p 6379 --no-auth-warning -a "$PASS" cluster info 2>/dev/null | grep -q "cluster_state:ok"; then
    return 0
  fi
  return 1
}

echo "Waiting for main-cluster nodes..."
for i in 1 2 3 4 5 6; do
  wait_for_node "main-cluster-$i"
done

echo "Waiting for saga-cluster nodes..."
for i in 1 2 3 4 5 6; do
  wait_for_node "saga-cluster-$i"
done

if is_cluster_initialized "main-cluster-1"; then
  echo "Main cluster already initialized."
else
  echo "Creating main cluster (3 masters, 3 replicas)..."
  redis-cli --no-auth-warning -a "$PASS" --cluster create \
    main-cluster-1:6379 \
    main-cluster-2:6379 \
    main-cluster-3:6379 \
    main-cluster-4:6379 \
    main-cluster-5:6379 \
    main-cluster-6:6379 \
    --cluster-replicas 1 \
    --cluster-yes
fi

if is_cluster_initialized "saga-cluster-1"; then
  echo "Saga cluster already initialized."
else
  echo "Creating saga cluster (3 masters, 3 replicas)..."
  redis-cli --no-auth-warning -a "$PASS" --cluster create \
    saga-cluster-1:6379 \
    saga-cluster-2:6379 \
    saga-cluster-3:6379 \
    saga-cluster-4:6379 \
    saga-cluster-5:6379 \
    saga-cluster-6:6379 \
    --cluster-replicas 1 \
    --cluster-yes
fi

echo "Both clusters ready."
