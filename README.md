# OmniBank — Distributed Banking Account & Ledger System

OmniBank is a distributed banking system designed using a polyglot microservices architecture. It provides account management, secure fund transfers, transaction logging, notification handling, and semantic transaction search.

## Features

- Account creation and management
- Secure JWT-based authentication
- Banking fund transfers
- Transaction and ledger management
- Double-entry-like debit and credit ledger entries
- RabbitMQ-based asynchronous communication
- Redis-based API rate limiting
- MongoDB notification service
- Semantic transaction search using embeddings
- PostgreSQL with pgvector
- Dockerized microservices
- Compensation mechanism for failed notifications
- Health monitoring endpoints

## Architecture

The system consists of the following services:

- **Frontend** — HTML, CSS and JavaScript served through Nginx
- **FastAPI** — API gateway, authentication and search
- **Spring Boot** — Transaction ledger and transfer processing
- **PostgreSQL** — Accounts, transactions and ledger data
- **RabbitMQ** — Asynchronous event communication
- **MongoDB** — Notification storage
- **Redis** — Rate limiting
- **Docker Compose** — Container orchestration

## Technologies

| Component | Technology |
|---|---|
| Frontend | HTML, CSS, JavaScript |
| API | Python, FastAPI |
| Ledger Service | Java, Spring Boot |
| Database | PostgreSQL |
| Vector Search | pgvector |
| Notifications | Node.js, Express, MongoDB |
| Messaging | RabbitMQ |
| Caching / Rate Limiting | Redis |
| Containerization | Docker |
| Authentication | JWT |

## Project Structure

```text
OmniBank_Project/
│
├── ledger-service/
│   ├── src/
│   ├── Dockerfile
│   └── pom.xml
│
├── notification-service/
│   ├── server.js
│   ├── package.json
│   └── Dockerfile
│
├── main.py
├── index.html
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
