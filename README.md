# Distributed Systems Troubleshooting Lab
![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)

Hi! Welcome to my hands-on laboratory. I built this project to gain practical, real-world experience in troubleshooting, debugging, and fixing high-load distributed systems.

This repository is designed as an **On-Call Simulator**. It mimics a production environment experiencing various failures, allowing you to practice incident response using logs, metrics, and architectural patterns.

## Resources
- [Incidents](https://docs.google.com/document/d/1r1XCtIaXUxqj8zq4YKoZJPDuCGakAjiKjLkaFVGykHc)
- [Log](https://docs.google.com/document/d/1oXc02MCUhHpGu1NZA40Fu8e2Vn9FIDNGaFObrGR-eCY/edit?usp=sharing)

## How to Use This Lab

Open the `Incidents` resource containing a list of prepared incidents. Each incident includes a specific **commit hash** and a brief **context**.

Here is your workflow as the On-Call Engineer:

1.  **Checkout the Incident:** Run `git checkout <commit-hash>` to travel to the exact state where the incident is occurring.
2.  **Clean the state:** Run `make reset` (*Linux/Mac*) or `./lab.bat reset` (*Windows*) from the `/deploy` dir to clear up all the artifacts from previous runs.
3.  **Read the Context:** Read the incident description. What are the users complaining about? What alerts are firing?
4.  **Investigate & Fix:** Your goal is to restore the system as quickly as possible.
    *   *Do not just read the code.* Rely heavily on Grafana dashboards, metrics, and logs to narrow down the root cause.
    *   Deploy a **Hotfix** to stop the bleeding immediately.
    *   Separately, design and implement an **Architectural Solution** (e.g., Outbox pattern, Saga, Idempotency keys) to prevent this from happening again.
5.  **Compare Notes:** Once resolved, check my personal investigation log for that incident. You can compare your approach with mine: what anomalies I noticed in the metrics, the tools I used to isolate the bottleneck, my hotfix, and my final architectural solution.

## The Environment

To make the simulation realistic, I built a setup that mirrors a real fintech architecture handling high-load trading (users buying/selling stocks).

The infrastructure includes:
*   **Microservices:** Two Go-based services communicating asynchronously.
*   **Message Broker:** Apache Kafka.
*   **Database:** PostgreSQL.
*   **Observability Stack:** Prometheus, Grafana, Loki.

**Important:** From commit to commit, the codebase will change. This is not a bug, but rather a feature. It simulates a real-world production environment where multiple people are working on services, and no single engineer knows every line of code. Your best friends here are your observability tools.

### Consistent behavior across different machines

If you look at the `docker-compose.yml` file, you'll notice strict resource limits (e.g., `cpus: '0.5'`, `memory: '512M'`) applied to the PostgreSQL database, Kafka, and the Go service.

This is intentional. In the real world, system performance is relative. A powerful modern CPU can execute a Full Table Scan so fast that it might mask underlying architectural flaws, even at hundreds of RPS. On weaker hardware, the exact same system might collapse at just 30 RPS.

To make this SRE lab predictable and educational, I artificially bottleneck the containers. So whether you run it on an old laptop or a high-end server, the Saturation Cliff will occur predictably at a similar RPS threshold.

### Microservices Architecture

The system is split into two independent services to ensure high availability and clearly separate the internal ledger from external market execution:

1. **`trading-api` (The Ledger & API Gateway)**
    * Receives incoming HTTP REST requests from users.
    * Validates user balances, holds funds/assets, and writes the initial order to the database.
    * Utilizes the **Transactional Outbox Pattern** to guarantee that pending orders are reliably published to the Kafka `orders.pending` topic.
    * Subscribes to the `orders.completed` topic to finalize the trade. It performs the strict **Double-Entry Accounting** to officially move the assets and updates the final order status to `EXECUTED` or `FAILED`.

2. **`execution-engine` (The Market Worker)**
    * Subscribes to the Kafka `orders.pending` topic.
    * Takes the pending order and "talks" to the external market broker (simulated execution).
    * Publishes the result back to the Kafka `orders.completed` topic.

## Useful
**Grafana:** [http://localhost:3000](http://localhost:3000) *(Credentials: `admin` / `admin`)*

**Environment Management:**
Run from the `/deploy` directory:

**Linux / Mac:**
- `make clean` - wipe all data
- `make up` - start the lab (keep the data)
- `make down` - shut down the lab (keep the data)
- `make reset` - shut down, clean the data and start the lab

**Windows:**
- `./lab.bat clean` - wipe all data
- `./lab.bat up` - start the lab (keep the data)
- `./lab.bat down` - shut down the lab (keep the data)
- `./lab.bat reset` - shut down, clean the data and start the lab

*You can also do `docker-compose up --build -d [name of the service]` to restart only one particular service.*

## Testing

The project includes robust **Integration Tests** using [Testcontainers](https://golang.testcontainers.org/). These tests spin up a real, ephemeral PostgreSQL database in Docker to validate the business logic.

**What is covered:**
*   **Double-Entry Accounting:** Mathematical validation that user balances and system accounts mirror each other perfectly (zero sum).
*   **Financial Flows:** E2E testing of critical paths, including holding funds (`PENDING`), successful trade execution (`EXECUTED`), and rolling back transactions / refunding (`REJECTED`).
*   **Database Constraints:** Validating schema correctness, unique keys, and complex SQL queries natively.

To run the tests use the `go test -v ./...` command.

*PS: On Windows, you might need to expose the Docker daemon on TCP and set `$env:DOCKER_HOST="tcp://localhost:2375"` for the tests to work.*

## Contributing (Create And Share Your Own Incidents!)

You can contribute by designing new incidents (e.g., memory leaks, race conditions, Kafka consumer group lags, deadlocks) and submitting a Pull Request.

### The "Zero-Config" Rule
The core philosophy of this lab is seamless reproducibility. A user must be able to reproduce your incident just by switching to the commit and running standard commands.
**No manual configuration should be required.**

If someone runs:
1. `git checkout <your-commit-hash>`
2. `make reset` (or `./lab.bat reset`)

...the system should successfully boot up, apply the load, and start bleeding on the Grafana dashboards.

### How to contribute an incident:
1. **Fork & Branch:** Fork the repository and create a new branch.
2. **Break it:** Introduce your bug into the codebase or infrastructure configuration.
3. **Adjust the Load Test (Optional):** If your bug requires a specific traffic pattern (e.g., spiky traffic, huge payloads), update the `scripts/loadtest.js` accordingly.
4. **Submit a PR:** Open a Pull Request and include a brief description of the incident:
    * **Context:** What is the scenario?
    * **Symptoms:** What metrics will go crazy in Grafana?
    * **Root Cause:** What is the actual bug?
5. **Merge & Publish:** Once reviewed, I will merge your PR into the `main` branch. I will then capture the exact merge commit hash and officially add it to the **Incidents Document**, giving you full credit for the scenario!