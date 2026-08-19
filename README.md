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

## The Environment

To make the simulation realistic, I built a setup that mirrors a real fintech architecture handling high-load trading (users buying/selling stocks).

The infrastructure includes:
*   **Microservices:** Two Go-based services communicating asynchronously.
*   **Message Broker:** Apache Kafka.
*   **Database:** PostgreSQL.
*   **Observability Stack:** Prometheus, Grafana, Loki.

## How to Use This Lab

Open the `Incidents` resource containing a list of prepared incidents. Each incident includes a specific **commit hash** and a brief **context**.

Here is your workflow as the On-Call Engineer:

1.  **Checkout the Incident:** Run `git checkout <commit-hash>` to travel to the exact state where the incident is occurring.
2.  **Clean the state:** Run `make reset` (*Linux/Mac*) or `./lab.bat reset` (*Windows*) from the `/deploy` dir to clear up all of the artifacts from previous runs.
3.  **Read the Context:** Read the incident description. What are the users complaining about? What alerts are firing?
4.  **Investigate & Fix:** Your goal is to restore the system as quickly as possible.
    *   *Do not just read the code.* Rely heavily on Grafana dashboards, metrics, and logs to narrow down the root cause.
    *   Deploy a **Hotfix** to stop the bleeding immediately.
    *   Separately, design and implement an **Architectural Solution** (e.g., Outbox pattern, Saga, Idempotency keys) to prevent this from happening again.
5.  **Compare Notes:** Once resolved, check my personal investigation log for that incident. You can compare your approach with mine: what anomalies I noticed in the metrics, the tools I used to isolate the bottleneck, my hotfix, and my final architectural solution.

## The Evolving Codebase

**Important:** From commit to commit, the codebase will change. This is not a bug, but rather a feature. It simulates a real-world production environment where multiple people are working on services, and no single engineer knows every line of code. Your best friends here are your observability tools.

## Useful commands
Run from the `/deploy` dir:

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