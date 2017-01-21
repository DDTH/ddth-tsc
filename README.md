ddth-tsc
========

DDTH's Time Series Counter library.

Project home:
[https://github.com/DDTH/ddth-tsc](https://github.com/DDTH/ddth-tsc)

**ddth-commons requires Java 8+ since v0.7.0**


## License ##

See LICENSE.txt for details. Copyright (c) 2014-2017 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation ##

Latest release version: `0.7.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Latest release version: 0.6.0.2. See RELEASE-NOTES.md.

Maven dependency: if only a sub-set of `ddth-tsc` functionality is used, choose the corresponding dependency artifact(s) to reduce the number of unused jar files.

`ddth-tsc-core`: in-memory counters, Redis and Cassandra dependencies are optional.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-tsc-core</artifactId>
    <version>0.7.0</version>
</dependency>
```

`ddth-tsc-redis`: include all `ddth-tsc-core` and Redis dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-tsc-redis</artifactId>
    <version>0.7.0</version>
    <type>pom</type>
</dependency>
```

`ddth-tsc-cassandra`: include all `ddth-tsc-core` and Cassandra dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-tsc-cassandra</artifactId>
    <version>0.7.0</version>
    <type>pom</type>
</dependency>
```


## Usage ##

Use `ddth-tsc` to count things for a period of time. Such as:

- How does my application's memory usage look like for the last hour?
- How many hits per minute to my web site during 9AM to 11AM?

**Step 1: obtain the counter factory**

```java
//in-memory counter factory (no persistency support)
ICounterFactory counterFactory = new InmemCounterFactory().init();
```

```java
//Cassandra counter factory (write counter data to Cassandra)
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
    .setCacheFactory(cacheFactory)
    .init();
```

```java
//Redis counter factory (write counter data to Redis)
ICounterFactory counterFactory = new RedisCounterFactory()
    .setRedisHostAndPort("localhost:6379")
    .setRedisPassword("redis_password")
    .init();

//or
import redis.clients.jedis.JedisPool;
...
JedisPool jedisPool = ...; //see https://github.com/xetorthio/jedis
ICounterFactory counterFactory = new RedisCounterFactory()
    .setJedisPool(jedisPool)
    .setRedisPassword("redis_password")
    .init();
```

**Step 2: obtain counters from the counter factory**

```java
ICounter countSiteVisits = counterFactory.getCounter("my-site-visits");

ICounter myCounter = counterFactory.getCounter("counter-name");
```

**Step 3: count!**

```java
//there is one visit to my site
counterSiteVisits.add(1);

//at a specific time, there are 3 visits to my site
counterSiteVisits.add(unixTimestampMs, 3);

//and get the data out
long timestampLastHour = System.currentTimeMillis() - 3600000;
DataPoint[] lastHour = counterSiteVisits.get(timestampLastHour);

long timestampLast15Mins = System.currentTimeMillis() - 15*60*1000; //15 mins = 900000 ms
DataPoint[] last15MinsGroupPerMin = counterSiteVisits.get(timestampLast15Mins, 15*60); //1 min = 60 secs
```

Finally: destroy the factory when done

```java
((AbstractCounterFactory)counterFactory().destroy();
```

### Counter methods ###
- `add(...)`: add a value
- `set(...)`: set a value
- `get()`: get a single data point value
- `getSeries(...)`: get a series of data points
- `getLastN(...)`: get last N data points
