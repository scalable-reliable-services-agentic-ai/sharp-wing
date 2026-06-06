<p align="center">
  <img src="sharp-wing.png" alt="SharpWing Logo" width="200">
</p>

# SharpWing: Real-Time Fraud Detection & Agentic Triage

Scalable and Reliable Services M — Alma Mater Studiorum – Università di Bologna

Academic Year: 2025/2026 — Spring/Summer

Domain: Financial Services / Mission-Critical Banking Operations

---

## Overview

SharpWing is a mission-critical fraud detection system engineered to secure high-volume banking transactions. The platform utilizes a **3-tier filtering pipeline** to achieve an optimal balance between operational costs, execution speed, and analytical precision:

1. Deterministic Layer: Ultra-high-speed filters (Geometric Velocity, Smurfing detection) for instant approval or denial of trivial cases.

1. Probabilistic Layer: Machine Learning-driven anomaly detection (XGBoost) enriched with behavioral user profiling via Redis.

1. Agentic Layer: Deep contextual investigation via Model Context Protocol (MCP). A Triage Agent and a Judge Agent collaborate to resolve complex "Grey Zone" anomalies, providing human-readable reasoning (Explainable AI).

### Key Technical Pillars

* Scalability: Engineered for a baseline of 20M daily transactions with a peak throughput of 580 RPS.

* Resilience: Cloud-native architecture on Kubernetes with a Multi-Cloud Pilot Light strategy (AWS as primary, Azure as DR).

* Observability: Integrated PLG Stack (Prometheus, Loki, Grafana) for real-time SLO monitoring and telemetry.

* HITL Ready: Operational dashboard for Human-in-the-loop intervention and auditability of agentic decisions.

---

## Contributors

* Pietro Bertozzi (pietro.bertozzi3@studio.unibo.it)

* Arash Foroozanfar (arash.foroozanfar@studio.unibo.it)

* Mikołaj Wewiór (mikolaj.wewior@studio.unibo.it)

Professor: Michele Colajanni

---

## Prerequisites

Ensure the following tools are installed in your development environment:
* Container Runtime: Docker Desktop (with Kubernetes enabled)
* Orchestration: kubectl, helm
* Frontend Tools: Node.js (v22+), npm, vite
* K8s Local Engine: kind (in Docker Desktop)

---

## Build & Deploy

Follow this sequence to deploy SharpWing on your local cluster.

### 1. Environment Preparation
Ensure the Kubernetes cluster is active and reachable:
```bash
kubectl cluster-info
```

### 2. Infrastructure Configuration
Update Helm repositories and download dependencies (Kafka, Redis, TimescaleDB):
```bash
helm repo update
helm dependency update ./helm/transaction-fraud-detection/
```

### 3. Image Building

Build the microservices (Validator, Generator, Agentic Worker) defined in the compose manifest:
```bash
docker compose build --no-cache
```

### 4. Kubernetes Deployment
Install the SharpWing release into the cluster:
```bash
helm install transaction-fraud-detection ./helm/transaction-fraud-detection
```

### 5. Network Tunnelling & Dashboard
Execute port-forwarding for internal services and launch the operational interface:

### 6. In a separate terminal: enable access to internal services (Grafana, API, etc.)
```bash
chmod +x ./apptunnels.sh
./apptunnels.sh
```

### 7. In the main terminal: launch the HITL dashboard
```bash
cd frontend-dashboard
npm install
npm run dev
```

---

## Endpoints & Usage

The system exposes the following interfaces once deployment is complete:

| Service | URL | Description |
| :--- | :--- | :--- |
| Operational Dashboard | http://127.0.0.1:5173 | Anomaly management, HITL triage, and Agent decisions |
| Grafana | http://127.0.0.1:30300 | Technical dashboard, Prometheus metrics, and Loki logs |
| Ingestion Gateway | http://127.0.0.1:8080 | Entry point for incoming banking transactions |

---

## SRE & Developer Toolbox

### Cluster Status Inspection
```bash
kubectl get pods -w
kubectl get svc
```

### Log Analysis
```bash
kubectl logs -f -l app=validator --prefix
kubectl logs -f -l app=triage-agent
```

### Troubleshooting
* Persistence: If database pods remain in Pending, ensure at least 4GB of RAM is allocated to Docker Desktop.
* Secrets: The system requires the timescaledb-secret for data persistence.

---

## Architecture Notes
SharpWing implements a distributed Lambda Architecture:
* Speed Layer: Kafka for buffering and Redis for low-latency lookups (under 50ms).
* Batch/Forensic Layer: TimescaleDB for long-term archival and DORA-compliant audit logs.
* Cognitive Layer: Asynchronous multi-agent workflow orchestrated via the MCP protocol.
