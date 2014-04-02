ddth-tsc-cassandra Table Schema
===============================

## History ##

#### 2014-03-17 ####
- Add `tsc_metadata` table.
- Add support for normal `bigint` column.

#### 2014-03-12 ####
First release


## Keyspace Schema ##

```sql
CREATE KEYSPACE keyspace_name
WITH replication={'class':'SimpleStrategy','replication_factor':'1'}
 AND durable_writes=true;
```


## Metadata Table Schema ##
```
CREATE TABLE tsc_metadata (
    c        varchar,
    o        text,
    PRIMARY KEY (c)
) WITH COMPACT STORAGE;

UPDATE tsc_metadata SET o='[{"pattern":"^.*$","table":"tsc_counters","counter_column":true}]' WHERE c='*';

UPDATE tsc_metadata SET o='{"table":"tsc_counters_1", "counter_column":true}' WHERE c='counter_metric_1';

UPDATE tsc_metadata SET o='{"table":"tsc_counters_2", "counter_column":false}' WHERE c='counter_metric_2';
```

## Counter Table Schema ##

Utilize Cassandra `counter` column:

```
CREATE TABLE tsc_counters (
    c        varchar,
    ym       int,
    d        int,
    t        bigint,
    v        counter,
    PRIMARY KEY ((c, ym, d), t)
) WITH COMPACT STORAGE;
```

- counter column does not support `set(...)`.
- used when you want to count things like "how many".

Use normal `bigint` column:

```
CREATE TABLE tsc_counters (
    c        varchar,
    ym       int,
    d        int,
    t        bigint,
    v        bigint,
    PRIMARY KEY ((c, ym, d), t)
) WITH COMPACT STORAGE;
```

- bigint column does not support `add(...)`.
- used when you want to track things like "what the value at time t was".

