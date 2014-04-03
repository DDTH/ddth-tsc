ddth-tsc
========

DDTH's Time Series Counter.

Project home:
[https://github.com/DDTH/ddth-tsc](https://github.com/DDTH/ddth-tsc)

OSGi environment: ddth-tsc modules are packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2014 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Modules #

ddth-tsc modules are released via Maven. Latest release version: `0.4.0.3`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-tsc-<module></artifactId>
	<version>0.4.0.3</version>
</dependency>
```

- [`ddth-tsc`](ddth-tsc-inmem/): in-memory counter. Maven artifactId: `ddth-tsc`.
- [`ddth-tsc-cassandra`](ddth-tsc-cassandra/): persistent counter using Cassandra as storage backend. Maven artifactId: `ddth-tsc-cassandra` (requires `ddth-tsc`).
- [`ddth-tsc-redis`](ddth-tsc-redis/): counter using Redis as storage backend. Maven artifactId: `ddth-tsc-redis` (requires `ddth-tsc`).


## Usage ##

Use `ddth-tsc` to count things for a period of time. Such as:

- How does my application's memory usage look like for the last hour?
- How many hits per minute to my web site during 9AM to 11AM?

Step 1: obtain the counter factory

```java
//in-memory counter factory
ICounterFactory counterFactory = new InmemCounterFactory().init();

//Cassandra counter factory
ICounterFactory counterFactory = new CassandraCounterFactory()
    .setHost("localhost")
    .setPort(9042)
    .setKeyspace("mykeyspace")
    .setTableTemplate("counter_tablename_template")
    .init();
    
//Redis counter factory
PoolConfig poolConfig = new PoolConfig()
    .setMaxActive(10).
    .setMaxIdle(8)
    .setMinIdle(2);
ICounterFactory counterFactory = new RedisCounterFactory()
    .setHost("localhost")
    .setPort(6379)
    .setRedisPoolConfig(poolConfig)
    .init();
```

Step 2: obtain counters from the counter factory

```java
ICounter countSiteVisits = counterFactory.getCounter("my-site-visits");

ICounter myCounter = counterFactory.getCounter("counter-name");
```

Step 3: count!

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
couterFactory.destroy();
```

### ICounter methods ###
- `add(...)`: add a value
- `set(...)`: set a value
- `get()`: get a single data point value
- `getSeries(...)`: get a series of data points
- `getLastN(...)`: get last N data points
