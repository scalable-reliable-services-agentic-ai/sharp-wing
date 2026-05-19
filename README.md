# Banking Transaction Fraud Detection System

## Contributors

- Pietro Bertozzi
- Arash Foroozanfar
- Mikołaj Wewiór


## Prerequirements:

- docker (default)
- [optionaly] uv (if running lockaly without docker)


## Usage:

To run this toy example paste in terminal one of those commands:

```bash
docker compose up --build
```

```bash
docker compose up --build -d
```

```bash
make setup
```

Then open:
`http://localhost:5000/`
and
`http://localhost:5003/`

## Usage with Kubernetes

### prequirements

1. install helm:
```bash
```

1. install kubectl:
```bash
```

### building

1. Starting Kubernetes in Docker Desktop App

1. build images from compose.yaml (static images, without runtime info):
```bash
docker compose build
```

### when adding new dependendcies (updating chart.yaml)

1. repo update
    ```bash
    helm repo update
    ```

1. dependency update
    ```bash
    helm dependency update ./helm/transaction-fraud-detection/
    ```

### development
1. kubernetes verification:
    ```bash
    kubectl cluster-info
    ```
    ```bash
    kubectl get nodes
    ```

1. helm repo:
    ```bash
    helm repo update
    ```

1. helm dependency update:
    ```bash
    helm dependency update ./helm/transaction-fraud-detection/
    ```

1. helm install:
    ```bash
    helm install transaction-fraud-detection ./helm/transaction-fraud-detection
    ```

### monitoring app

```bash
kubectl get pods
```

```bash
kubectl get pods -w
```

```bash
kubectl logs -f job/<job-name_or_id>
```

```bash
kubectl logs -f <service-name_or_id>
```


<!-- 1. change permissions of the script and run it:
```bash
chmod +x ./docker/build_images.sh
./docker/build_images.sh
```

this script must be run every time there are changes in the services code. After building the images it is required to run also this script:
```bash
kubectl rollout restart deployment <release-name>-validator
```

### running

1. from main project directory
```bash
cd helm/transaction-fraud-detection
helm dep up
cd ../..
```

1. install
```bash
helm install transaction-release ./helm/transaction-fraud-detection
```

1. check:
```bash
kubectl get pods
```

1. creating secrets 
```bash
kubectl create secret generic timescaledb-secret --from-literal=password=passward
```


## My order:

```bash
chmod +x ./docker/build_images.sh
```

```bash
helm dep up ./helm/transaction-fraud-detection
```

```bash
kubectl create secret generic timescaledb-secret --fromliteral=<pass-key>=<real-pass>
```

```bash
helm install transaction-release ./helm/transaction-fraud-detection
```

### check:

czy są na pewno obrazy
```bash
docker images
```

stan obrazów w k8s
```bash
kubectl get pods
``` -->

## Observability

This project uses Prometheus and Grafana for observability. You can access the Grafana dashboard and Prometheus UI at the following URLs:

- **Grafana:** [http://localhost:3000](http://localhost:3000)
- **Prometheus:** [http://localhost:9090](http://localhost:9090)

## Progress


[13.04]

So far, these elements are created (for now, really simple; it's more a toy exmaple than final implementation):

<img width="400" height="651" alt="image" src="https://github.com/user-attachments/assets/8cdf5f3b-1955-43c6-9365-74a8e25da9e3" />


## Remaining things
(order does not matter)

1. Infrastructure:
    - Kubernetes
        - and other staff related to that 
    
    - Prometheus & Grafana
        - for observability, metrics and log managing

1. Anomaly detection:
    - Delete current LLM use in this service
    - Create new deterministic module
        - Mimic real advanced ML system with simple functions
        - Add quick, simple functions to use in the service
        - Copy some from other existing instances
        - Make sure not to call the whole database. Adjust the Redis to have really fast responses

1. Agents and MCP services

    1. Triage & Diagnosis &mdash;
    handles frauded transactions, can join autonomus decisions with *HITL*.
        - Telemetry and Observability

    1. Monitoring, Remediation & Escalation &mdash;
    Monitors the status of the system and make decisions based on the system state.
        - Remediation
        - Policies

1. Operational dashboards and human-in-the-loop services

1. Controlled failure of system


<!--
## TODO

- [DONE] [Miki] Database setup and queue update
    1. [DONE] Switching from Redis to Kafka [speed layer]
    1. [DONE] Configuring TimeseriesDB (extention of PostgreSQL) [batch layer]
    1. [DONE] Lambda architecture setup

- [NotAssignedYet] Architecture remaining work:
    1. Logger centralisation
        - Use grafana
    1. Error handling
    1. System status/state monitoring
    1. Bringing back control panel
    1. Kafka topics cleaning
    1. Switch to Kubernetes
    1. Implementing scalability (now everything is at fixed size defined in compose.yaml)

- [NotAssignedYet] system modules / services
    1. Validator to upgrade/adjust
    1. Simple anomaly detection
    1. Advanced anomaly detection with LLM
    1. MCP server with tools for anomaly detectors
    1. Human in the loop veryfication service


- [Arash] Database insertion [DONE]
    1. Create scenarios of different client types, including:
        - spending amounts
        - frequency of their transactions
        - amount and time patterns (e.g. spending less on weekdays and a lot on weekends)
        - location (some clients are only in italy, other are all over the world)
        - other features based on attributes from the `Transaction` class in `src/generator/transaction.py`
    1. Prepare a file/script that will be used to load the data into the DB (_needed to decide how many rows or different client IDs: 1k? 30k? 200k? 1M?_)

- [Arash] Generator update [DONE]
    1. Transactions should generate new transactions including both new clients and clients that already exist in the database. Generation of transactions for the existing clients should take into the account their behaviour patterns (described in _scenarios_ from previous point)
    1. The invlaid/fraud clients should be identified before or in the moment of generation (with known reason and explanation why). That will let us test if the anomaly detection works properly. 

- [Pietro] PowerBI dashboard
    1. to verify connection of the TimeseriesDB or PostreSQL with PowerBI
    1. to verify visualisation possibilities
    1. to verify uploading the dashboard to a local enviroment and cloud

## ToDo -- draft (thoughts, brainstorm)

### System

- Databases
    - inserting transactions from Redis queue into the DB
    - updating the DB after decision (valid/fraud)

- Anomaly detection modules and related functionality:
    - agents and modules:
    - fraud scenarios

- Dashboards and tools:
    - PowerBI for visualizing the stats and system state
    - second tool for operator interaction with the system
    - another tool for controlling the incoming transactions and the system (e.g. prof can proceed ‘stress tests’ from this tool by removing or shrinking some parts, etc.)

- Agentic and MCP usage: 

- Generator to update:
    - to generate more real-like data
    - constraints on different attributes
    - some scenarios of client types and integration with DB
    - Filling the DB at the start of the system with some ready/previously-made transactions and clients

- Automatic Scalability and Kubernetes

- Cloud:
    - Decide what should be target Cloud provider (GCP/AWS/Azure) with explanation
    - prepare a explanation how to migrate from our local environment to the cloud

### Possible solutions and propositions

- Maybe in anomaly detection modules there should be a few agents that check some things/attributes/cases simultaneously. Then some manager/master agent verifies all of these smaller agents, and then it decides a final verdict.
The manager/master could have a LLM integrated and call other subagents by MCP (?)
-->
