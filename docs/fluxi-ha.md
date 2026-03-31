# Fluxi HA Notes

Fluxi supports two Redis connection modes for the engine and the engine-backed SDK:

- `direct`: connect straight to a single Redis instance through `FLUXI_REDIS_URL`
- `sentinel`: discover the active Redis master through Redis Sentinel and keep operating after failover

## Required Sentinel env vars

Set these for `fluxi-server`, `fluxi-scheduler`, and any SDK worker/client process that uses the real engine backend:

- `FLUXI_REDIS_MODE=sentinel`
- `FLUXI_REDIS_URL=redis://:redis@<redis-host>:6379/0`
- `FLUXI_SENTINEL_ENDPOINTS=host1:26379,host2:26379,host3:26379`
- `FLUXI_SENTINEL_SERVICE_NAME=fluxi-master`

Optional Sentinel env vars:

- `FLUXI_SENTINEL_MIN_OTHER_SENTINELS`
- `FLUXI_SENTINEL_USERNAME`
- `FLUXI_SENTINEL_PASSWORD`

`FLUXI_REDIS_URL` still matters in Sentinel mode because Fluxi uses it to derive the Redis username, password, database index, and TLS scheme for the master connection pool.

## Local Docker topology

The local `docker-compose.yml` now runs:

- one Redis master
- one Redis replica
- three Sentinel instances

The monitored Sentinel service name is `fluxi-master`.

## Expected failover behavior

- `fluxi-server` and `fluxi-scheduler` reconnect through Sentinel-backed Redis clients.
- SDK workers keep polling after transient Redis or master-discovery failures instead of crashing.
- Worker task acknowledgements remain conservative: if completion reporting fails, the task is left pending and recovery continues through existing retry/fencing rules.

## Remaining limitation

Redis Sentinel improves master discovery and automatic failover, but it does not eliminate write-loss windows during failover. Recent writes can still be lost if the old master fails before replication catches up.
