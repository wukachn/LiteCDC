# Third Year Project: Change Data Capture

## Overview
This repo provides a simple application to push change events from a postgres source database to kafka for consumption. Additionally, an optional mysql sink is provided, allowing a user to consume their change data to replicate their source in a mysql instance.

## Getting Started
I have provided an example docker-compose file to spin up the necessary components, this is for local testing and should not be used for real applications.

To get started, import the example postman API collection to view some example requests to start your pipeline.

For a more complete overview of the system, please refer to the project report.