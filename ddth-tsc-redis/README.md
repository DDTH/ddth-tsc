ddth-tsc-redis
==============

Time Series Counter with Redis as storage backend.

See more: [ddth-tsc](https://github.com/DDTH/ddth-tsc).

## Usage ##

Counter Factory:

```java
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