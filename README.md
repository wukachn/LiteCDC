# LiteCDC

## Overview
This repo provides a simple application to push change events from a postgres source database to kafka for consumption. Additionally, an optional mysql sink is provided, allowing a user to consume their change data to replicate their source in a mysql instance.

## Getting Started
I have provided an example docker-compose file to spin up the necessary components, this is for local testing and should not be used for real applications.

To get started, import the example postman API collection to view some example requests to start your pipeline.

For a more complete overview of the system, please refer to the project report.

If you would like to run the app locally, you can use the following commands. This will spin up the necessary components to create a postgres -> mysql pipeline:
1. `mvn clean install`
2. `docker-compose --profile local-postgres --profile local-mysql up --build` (with test postgres and mysql dbs)

## Testing
When running the tests, ensure that the follow environment variables are set in your run configuration:
 - PG_PASS = `pg_password`
 - MYSQL_PASS = `mysql_password`

Known Issue: The end-to-end tests (`EndToEnd___Test` and `NoDestination___Test`) currently fail when ran in the same run configuration. These tests should be run in isolation.