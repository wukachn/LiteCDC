# Third Year Project: Change Data Capture App

Run the App
1. `mvn clean install`
2. `docker-compose --profile local-postgres up --build` (local postgres test db)

Lucid Chart: https://lucid.app/lucidchart/b21cdc93-e535-4cff-abe6-6bfbcc1910bc/edit?invitationId=inv_6d264c25-5c45-4cff-81c6-cb8350444901

Reformat Directory (Windows): CTRL + ALT + L

## Week 2

 - Set up basic skeleton of project
   - Created Repository
   - New Maven Project
   - Basic Spring App - With Basic API Request
   - Initial Code Flow of Pipeline Creation
 - Created Test RDS Postgres Database


## Week 3/4

Set up initial snapshot environment

`ALTER SYSTEM SET wal_level = logical;`
rds.logical_replication=1

Starting to work on setting up the initial snapshot environment. We need to snapshot a consistent view of the database.

`CREATE_REPLICATION_SLOT \"slot_name\" LOGICAL pgoutput;` creates a snapshot of the database internally, and returns the consistent_point in which we are going to snapshot then stream from.
Be aware that snapshots are still tied to the life cycle of their associated transaction, and giving them IDs doesn't change anything. 
Once the exporting transaction commits or rolls back, new transactions trying to access an exported snapshot will see:
db=> set transaction snapshot '000ED905-1';
ERROR:  invalid snapshot identifier: "000ED905-1"
(https://www.willglynn.com/2013/10/25/postgresql-snapshot-export/)
(https://www.postgresql.org/docs/9.3/functions-admin.html#:~:text=9.26.5.-,Snapshot%20Synchronization%20Functions,-PostgreSQL%20allows%20database)

`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;` only sees data committed before the transaction began; 
It never sees either uncommitted data or changes committed by concurrent transactions during the transaction's execution.

Started to experiment with kafka. This lead me to start using a `docker-compose.yml` file to spin up the full app stack.
(https://howtodoinjava.com/kafka/kafka-cluster-setup-using-docker-compose/)

I had to start to use a local instance of postgres, I ran out of the AWS free tier.

Started to capture the data in the snapshot. Currently, a Struct is built which allows us to track each column name, type and value. 
The next steps are to build more of the actual change object with some added metadata, and send it off to kafka. 

## Week 5/Reading Week

Created simple kafka application. Can now push and pull change data from kafka. This caused more issues than expected, I had a lot of issues with serialization/deserialization. 
I was concerned about performance, so I was hesitant to go the json route. I finally got everything working using avro, which should be more performant anyway.