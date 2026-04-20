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


## Progress

[13.04]

So far, these elements are created (for now, really simple; it's more a toy exmaple than final implementation):

<img width="400" height="651" alt="image" src="https://github.com/user-attachments/assets/8cdf5f3b-1955-43c6-9365-74a8e25da9e3" />


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
