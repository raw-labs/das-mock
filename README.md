# DAS Mock

This is a mock implementation for the DAS service. It demonstrates how to build create a simple DAS service and its
underlying data model.

## Prerequisites

- PostgreSQL 13+
- [multicorn2](https://github.com/raw-labs/multicorn2)
- [multicorn-das](https://github.com/raw-labs/multicorn-das)

## DASMock

DASMock is an implementation of [das-scala-sdk](https://github.com/raw-labs/das-sdk-scala) that provides both on the go
generated tables and in memory table.

- **small** is a table that generates 100 rows of data on the fly when queried.
- **big** is a table that generates 2000000000 rows of data on the fly when queried.
- **in_memory** is a table that stores data in memory.

## Running the service in IntelliJ

After setting up the environment and building the project, go to `External Libraries` ->
`sbt: com.raw-labs:das-server-scala_<version>:jar`.
Open DASSServerMain and run the main method.

## Interact with the DASMock FDW
```DROP SERVER multicorn_das CASCADE;
CREATE SERVER multicorn_das FOREIGN DATA WRAPPER multicorn OPTIONS (
    wrapper 'multicorn_das.DASFdw',
    das_url 'localhost:50051',
    das_type 'mock'
);
DROP SCHEMA test CASCADE;
CREATE SCHEMA test;
IMPORT FOREIGN SCHEMA foo FROM SERVER multicorn_das INTO test;
```
```SELECT * FROM test.small LIMIT 3;```
```INSERT INTO test.in_memory (column1, column2) VALUES (1,'1');```