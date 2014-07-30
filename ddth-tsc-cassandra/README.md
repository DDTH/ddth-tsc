ddth-tsc-cassandra
==================

Persistent Time Series Counter with Cassandra as storage backend.

See more: [ddth-tsc](https://github.com/DDTH/ddth-tsc).

## Usage ##

Counter Factory:

```java
ICounterFactory counterFactory = new CassandraCounterFactory()
    .setHost("localhost")
    .setPort(9042)
    .setKeyspace("mykeyspace")
    .setTableMetadata("metadata_table_name")
    .init();
```