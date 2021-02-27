<p align="center">
  <h3 align="center">ICON Kafka Worker</h3>

  <p align="center">
    The Kafka data processing microservice for <a href="https://github.com/geometry-labs/icon-api">icon-api</a>.
    <br />
</p>

## Getting Started

### Docker Build

To build container for production:

```bash
docker build --target prod -t icon-kafka-worker .
```

To build container for development/testing:

```bash
docker build --target test -t icon-kafka-worker .
```

## Usage

Docker container can be used either as a standalone worker or in a docker-compose stack.
To use in a standalone configuration:

```bash
docker run icon-kafka-worker
```

Or in a docker-compose stack:

```yaml
filter-worker-transaction:
image: geometrylabs/icon-kafka-worker:latest
hostname: kakfa-worker-transaction
environment:
  CONTRACT_WORKER_KAFKA_SERVER: kafka:29092
  CONTRACT_WORKER_SCHEMA_SERVER: http://schemaregistry:8081
  CONTRACT_WORKER_CONSUMER_GROUP: filter-worker-transaction
  CONTRACT_WORKER_POSTGRES_SERVER: postgres
  CONTRACT_WORKER_POSTGRES_USER: postgres
  CONTRACT_WORKER_POSTGRES_PASSWORD: password
  CONTRACT_WORKER_OUTPUT_TOPIC: outputs
  CONTRACT_WORKER_PROCESSING_MODE: transaction
```

Just be sure to set the corresponding environment variables to suit your configuration.

### Environment Variables
| Variable                 | Default                   | Description                                                           |
|--------------------------|---------------------------|-----------------------------------------------------------------------|
| KAFKA_SERVER             |                           | URL for Kafka server                                                  |
| CONSUMER_GROUP           | contract_worker           | Name to use for consumer group                                        |
| SCHEMA_SERVER            |                           | URL for Schema Registry server                                        |
| KAFKA_COMPRESSION        | gzip                      | Kafka compression type                                                |
| KAFKA_MIN_COMMIT_COUNT   | 10                        | Minimum number of messages to process before sending a commit message |
| REGISTRATIONS_TOPIC      | event_registrations       | Kafka topic for registration messages                                 |
| BROADCASTER_EVENTS_TOPIC | broadcaster_events        | Kafka topic for broadcaster event messages                            |
| BROADCASTER_EVENTS_TABLE | broadcaster_registrations | Postgres table to store broadcaster event registrations               |
| LOGS_TOPIC               | logs                      | Kafka topic for logs to be processed                                  |
| TRANSACTIONS_TOPIC       | transactions              | Kafka topic for transactions to be processed                          |
| OUTPUT_TOPIC             |                           | Kafka topic to output processed messages                              |
| POSTGRES_SERVER          |                           | Postgres server hostname                                              |
| POSTGRES_PORT            | 5432                      | Postgres server port                                                  |
| POSTGRES_USER            |                           | Postgres username                                                     |
| POSTGRES_PASSWORD        |                           | Postgres password                                                     |
| POSTGRES_DATABASE        | postgres                  | Postgres database name                                                |
| PROCESSING_MODE          | contract                  | Worker processing mode (contract/transaction)                         |
| USE_SCHEMA_FOR_DATA      | False                     | Enable to use Schema Registry server for blockchain data              |

## License

Distributed under the Apache 2.0 License. See `LICENSE` for more information.
