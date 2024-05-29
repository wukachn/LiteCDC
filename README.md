# LiteCDC

## Overview
LiteCDC is a lightweight Change Data Capture (CDC) solution designed for PostgreSQL databases, with optional replication to MySQL. It enables you to efficiently capture and track change events in your PostgreSQL databases, before sending them to Kafka for future consumption through an external application or using one of the internal MySQL sinks. 

This behavior is determined by a user-defined pipeline, the architecture of which is illustrated below:
![image](https://github.com/wukachn/LiteCDC/assets/68754675/c4cac017-0a78-43ea-8bf8-a4be2e2a6c75)

For a more comprehensive overview of the system, please refer to the project report (`report.pdf`).

## Requirements

**PostgreSQL**:
 - Version: `9.4+`
 - wal_level: `logical`

The (optional) sinks should work with most versions of MySQL but the application has been tested with versions `8.0+`.

## Getting Started
The provided example `docker-compose.yml` file spins up the necessary components for local testing. Please note that this setup is intended for development and testing purposes only and should not be used for production environments.

### Steps to Run Locally
1. **Build the Project:**

   ```sh
   mvn clean install
   ```
2. Start the Docker Containers
   ```sh
   docker-compose --profile local-postgres --profile local-mysql up --build
   ```
### Postman Collection
Import the example Postman API collection (`postman_collection.json`) to view some sample requests and start up a demo pipeline.

The following requests are available:
 - Run Pipeline: `POST /pipeline/run`
 - Halt Pipeline: `POST /pipeline/halt`
 - Get Pipeline Status: `GET /pipeline/status`
 - Get Snapshot Metrics: `GET /pipeline/metrics/snapshot`
 - Get (General) Metrics: `GET /pipeline/metrics`

## Testing

### Run Configuration
When running the tests, ensure that the follow environment variables are set in your run configuration:
 - PG_PASS = `pg_password`
 - MYSQL_PASS = `mysql_password`

**Known Issue:** The end-to-end tests (`EndToEnd___Test` and `NoDestination___Test`) currently fail when ran in the same run configuration. These tests should be run in isolation.

### Performance
Using a **very** crude local approach to performance testing, the following behaviour was observed:

**Snapshotting**
 - `80,000,000` rows across 4 tables (`13.7 GB`)
 - Rows processed in `655s`.
 - Upper Limit Throughput: `~ 122,137 rows/s`

**Streaming**
 - `20,000,000` rows from a single table (`3.4 GB`).
 - These rows were generated using a SQL statement, which committed transactions of 5000 insert operations until 20 million rows were inserted.
 - The system processed the changes at a rate faster than they were being generated, maintaining a consistent source-producer time lag of less than 70 ms.
 - Changes processed in `332s`.
 - Upper Limit Throughput: `> 60,240 rows/s`

**MySQL Sinks**
 - `10,000,000` rows across 4 tables (`1.7 GB`)
 - Used a similar method of generating change data as the streaming performance test, using different mixes of event types to show the range in performance.
 - **Batching**: `(15,175 ~ 15,267) rows/s`
 - **Transactional**: `(6,165 ~ 6,978) rows/s`

## References
 - https://github.com/davyam/pgEasyReplication
 - https://github.com/debezium/debezium
