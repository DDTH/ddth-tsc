ddth-tsc-cassandra
==================

Persistent Time Series Counter with Cassandra as storage backend.

See more: [ddth-tsc](https://github.com/DDTH/ddth-tsc).

## Usage ##

Obtain Counter Factory:

```java
import com.github.ddth.cql.SessionManager;
...
// obtain a Cassandra SessionManager
// See https://github.com/DDTH/ddth-cql-utils
SessionManager sessionManager = new SessionManager();
sessionManager.init();
....
// To enable cache: obtain a Cache Factory
// See https://github.com/DDTH/ddth-cache-adapter
ICacheFactory cacheFactory = ...;
...
ICounterFactory counterFactory = new CassandraCounterFactory()
    .setHostsAndPorts("host1:9042,host2:9042,host3:9042")
    .setUsername("cassandra_user")
    .setPassword("cassandra_password")
    .setKeyspace("mykeyspace")
    .setSessionManager(sessionManager)
    .setTableMetadata("metadata_table_name")
    .setCacheFactory(cacheFactory)
    .init();
```

See [README.md](../README.md) for more information.
