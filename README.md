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


## Remaining things

1. Infrastructure:
    - Kubernetes
        - and other staff related to that 

2. Anomaly detection:
    - MCP tools/funcitons for the first anomaly detection
        - it includes things like: checking current amount and average one, location changes, frequency of spending and timing, etc.    
    - Implementing the usage of the llm-proxy/library -- some binding for the llm
    - instructions for the LLM, some Agentic Operations Requirements, Policies, etc

3. Validator
    - Fixing the validation count
    - Managing invalid transactions

4. Operational dashboard

5. Controlled failure of system


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
