# SharpWing

Banking Transaction Fraud Detection System

SRS 2026 Spring/Summer


## Contributors

- Pietro Bertozzi
- Arash Foroozanfar
- Mikołaj Wewiór


## Prerequirements:

- Docker Desktop
- npm
- vite


## Build and deploy

1. Build and launch the containerized backend infrastructure (defined in compose.yaml):
    ```bash
    docker compose up --build -d
    ```

2. Start the operational dashboard local server:
    ```bash
    cd frontend-dashboard
    npm run dev
    ```

---

## Usage

### Open system dashboards

- 🌐 Operational UI:   http://localhost:5173

### Development useful commands

1. Check statuses, health, and list all active containers:
    ```bash
    docker compose ps
    ```

2. Tail the real-time live logs of the entire pipeline:
    ```bash
    docker compose logs -f
    ```
   
3. Bring down the cluster and wipe shared container volumes safely:
    ```bash
    docker compose down -v
    ```