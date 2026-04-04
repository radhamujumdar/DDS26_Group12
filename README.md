# Distributed Data Systems Project Template

Basic project structure with Python's FastAPI and Redis.
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 

* `packages`
    Folder containing shared local packages, including the reusable `fluxi-sdk`, `fluxi-engine`, and project-level `shop-common` package.

* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

* `docs`
    Additional project documentation, including the Fluxi phase-1 bootstrap guide in `docs/phase-1-fluxi-bootstrap.md` and Sentinel HA notes in `docs/fluxi-ha.md`.

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct.
The Fluxi engine services now use Redis Sentinel discovery in the local Compose deployment; see `docs/fluxi-ha.md` for the required env contract and failover behavior.
(you can use the provided tests in the `\test` folder and change them as you wish). 

For Fluxi-heavy benchmark runs, the local Compose deployment is tuned with:
- `fluxi-server` running multiple Uvicorn workers via `FLUXI_SERVER_WORKERS` (default `4`)
- `order-checkout-worker` using `FLUXI_ORDER_WORKFLOW_CONCURRENCY` (default `8`) and `FLUXI_ORDER_ACTIVITY_CONCURRENCY` (default `4`)
- `stock-activity-worker` using `FLUXI_STOCK_ACTIVITY_CONCURRENCY` (default `16`)
- `payment-activity-worker` using `FLUXI_PAYMENT_ACTIVITY_CONCURRENCY` (default `16`)

You can scale the hot roles further during stress tests, for example:

```bash
docker compose up -d --build \
  --scale fluxi-server=2 \
  --scale order-checkout-worker=2 \
  --scale stock-activity-worker=2 \
  --scale payment-activity-worker=2
```

For laptop benchmarking, use the lean direct-Redis profile instead of the full Sentinel HA stack:

```bash
docker compose --env-file .env.benchmark -f docker-compose.benchmark.yml up -d --build
```

That profile uses:
- one direct Fluxi Redis instead of Redis master/replica/Sentinel
- fewer API and Fluxi server worker processes
- one replica of each business worker role by default
- lower in-process worker concurrency tuned for an 8-core / 16 GB laptop

For the course staff cluster, use the dedicated deployment profiles:

- `docker-compose.small.yml` plus `scripts/up-small.sh`
  - one instance of every service, database, and queue
  - about `24` CPUs total
- `docker-compose.medium.yml` plus `scripts/up-medium.sh`
  - targets exactly `50` CPUs
  - Sentinel-backed HA for Fluxi plus the `order`, `stock`, and `payment` Redis stores
  - scales API roles and hot worker roles: `order-service=2`, `stock-service=2`, `payment-service=2`, `fluxi-server=2`, `fluxi-scheduler=2`, `order-checkout-worker=3`, `stock-activity-worker=2`, `payment-activity-worker=2`
- `docker-compose.large.yml` plus `scripts/up-large.sh`
  - targets exactly `90` CPUs
  - Sentinel-backed HA for Fluxi plus the `order`, `stock`, and `payment` Redis stores
  - scales API roles and hot worker roles: `order-service=2`, `stock-service=2`, `payment-service=2`, `fluxi-server=2`, `fluxi-scheduler=2`, `order-checkout-worker=4`, `stock-activity-worker=3`, `payment-activity-worker=3`

The CPU split follows the checkout hot path while leaving room for replica overhead:

- roughly `60%` of worker CPU goes to order workflow execution, because each successful checkout still spends most of its orchestration work on the `orders` workflow queue
- roughly `20%` goes to stock activities and `20%` to payment activities, because each successful checkout performs one stock reservation and one payment charge
- the remaining fixed CPU budget is reserved for the synchronous API path, `fluxi-server`, the scheduler, ingress, and Redis/Sentinel infrastructure

The main tuning knobs in those profiles are:

- `ORDER_API_WORKERS`: gunicorn workers for synchronous `/checkout` admission
- `FLUXI_SERVER_WORKERS`: Uvicorn workers for workflow start/result traffic
- `FLUXI_MAX_CONCURRENT_WORKFLOW_TASKS`: order workflow slots per checkout-worker container
- `FLUXI_MAX_CONCURRENT_ACTIVITY_TASKS`: activity slots per stock/payment worker container
- `FLUXI_STICKY_CACHE_MAX_RUNS` and `FLUXI_STICKY_CACHE_TTL_MS`: size and lifetime of the sticky workflow session cache
- `FLUXI_RESULT_WAIT_TIMEOUT_MS`: long-poll timeout for synchronous workflow completion
- `REDIS_MODE`, `REDIS_SENTINEL_ENDPOINTS`, and `REDIS_SENTINEL_SERVICE_NAME`: business-service failover wiring for the `order`, `stock`, and `payment` Redis backends

The `small` profile keeps a single direct Fluxi Redis for simplicity. The `medium` and `large` profiles use Sentinel-backed failover for Fluxi and each business Redis store, replicate `fluxi-scheduler`, and keep all stateful Redis services on persistent AOF-backed volumes with `restart: unless-stopped`.

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
