# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
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
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

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

### Benchmark runner

The benchmark entrypoint is now a thin shim around the `benchmark` package:

```bash
pip install -r requirements.txt
python benchmark.py --backends docker-compose --scenarios throughput --modes 2pc saga --users 500 1000 2000 --locust-workers 2
```

Supported backend/scenario combinations:

* `docker-compose + throughput`
* `docker-compose + ha`
* `minikube + throughput`
* `minikube + ha`

Scenario meaning:

* `throughput` uses the same deployed system topology without fault injection
* `ha` uses the same deployed system topology with failure injection enabled

The benchmark does not change Sentinel or replica presence based on scenario selection.

Laptop-safe defaults:

* `--users 500 1000 2000`
* `--locust-workers 2`

Example commands:

```bash
python benchmark.py --backends docker-compose --scenarios throughput --modes 2pc saga --users 500 --runs 1 --duration 30s
python benchmark.py --backends docker-compose --scenarios ha --modes 2pc saga --users 500 --runs 1 --duration 30s
python benchmark.py --backends minikube --scenarios throughput --modes 2pc saga --users 500 --runs 1 --duration 30s
python benchmark.py --backends minikube --scenarios ha --modes 2pc saga --users 500 --runs 1 --duration 30s
```

Results are stored under:

```text
benchmark-results/<backend>/<scenario>/<mode>/users_<n>/run_<i>_<timestamp>/
```

Each run directory includes `metadata.json`, Locust CSVs and stdout, the consistency output, and diagnostics when startup or recovery fails.

### Internal routing and deployment knobs

The gateway is for external traffic only. Internal service-to-service calls should use the stable service URLs directly:

* `PAYMENT_SERVICE_URL=http://payment-service:5000`
* `STOCK_SERVICE_URL=http://stock-service:5000`
* `ORDER_SERVICE_URL=http://order-service:5000`

Local defaults are tuned for a laptop-sized deployment but keep the same overall HA-capable topology:

* `ORDER_REPLICAS=1`
* `PAYMENT_REPLICAS=1`
* `STOCK_REPLICAS=1`
* `ORDER_GUNICORN_WORKERS=2`
* `PAYMENT_GUNICORN_WORKERS=1`
* `STOCK_GUNICORN_WORKERS=1`
* `SENTINEL_REPLICAS=1`

These defaults demonstrate container and pod failover behavior on one machine, but they do not provide true multi-node availability. Cloud deployers are expected to tune replica counts and resources upward without changing the architecture or internal routing contract.

### Compose profiles

For course-staff Docker Compose runs we provide three explicit profile files plus three launcher scripts:

* `docker-compose.small.yml`
* `docker-compose.medium.yml`
* `docker-compose.large.yml`

The matching `.env.small`, `.env.medium`, and `.env.large` files document the tuned runtime knobs
(Gunicorn workers, gateway worker count, Saga MQ partitioning, and shard host lists) used by each profile.

Profile intent:

* `small`: one instance of each logical API, database, and queue; no Sentinel quorum or Redis replicas/shards.
* `medium`: approximately `50` CPUs total.
* `large`: approximately `90` CPUs total.

CPU budgeting:

* `medium`: `order=21`, `payment=10`, `stock=10`, `gateway=3`, `infra=6`, total `50`.
* `large`: `order=32`, `payment=18`, `stock=18`, `gateway=3`, `infra=19`, total `90`.

We bias CPU toward `order-service` because every checkout enters through order first, while `stock-service`
and `payment-service` also scale out with Redis sharding. `order-db` remains a single master and is still
the main architectural bottleneck once the order tier gets very hot.

Examples:

```bash
sh compose-up-small.sh
sh compose-up-medium.sh
sh compose-up-large.sh
```

Each launcher defaults to `TX_MODE=2pc` for throughput. To run the same profile in saga mode:

```bash
TX_MODE=saga sh compose-up-small.sh
TX_MODE=saga sh compose-up-medium.sh
TX_MODE=saga sh compose-up-large.sh
```

Equivalent raw Docker Compose commands:

```bash
docker compose -f docker-compose.small.yml up -d --build --remove-orphans

docker compose -f docker-compose.medium.yml up -d --build --remove-orphans

docker compose -f docker-compose.large.yml up -d --build --remove-orphans
```

Scaling notes:

* External traffic is routed through nginx, which resolves all currently running `order-service`, `payment-service`,
  and `stock-service` container IPs at startup and round-robins across them.
* Internal service-to-service traffic uses the stable service URLs directly (`http://order-service:5000`,
  `http://payment-service:5000`, `http://stock-service:5000`), which works for static Compose replica sets and
  maps cleanly to Kubernetes Services later.
* Saga workers scale horizontally because payment and stock consume Redis Streams through consumer groups.
* Recovery loops are safe under replication because they use Redis lease keys, so only one worker is active per
  recovery role at a time.
