ddth-tsc release notes
======================

0.7.0 - 2017-01-22
------------------

- Refactor & Restructure project.
- Bump to `com.github.ddth:ddth-parent:6`, now requires Java 8+.
- Redis counter:
  - Switch to `Jedis` (https://github.com/xetorthio/jedis).
  - Support sharded-Redis
- Add new unit test cases.


0.6.1 - 2015-07-30
------------------

- `ddth-tsc-cassandra`: review & several performance improvements.


0.6.0 - 2014-10-10
------------------

- `CassandraCounterFactory`: Use [ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils) to access Cassandra storage.
- `CassandraCounterFactory`: Use [ddth-cache-adapter](https://github.com/DDTH/ddth-cache-adapter) for caching.


0.5.1 - 2014-08-06
------------------

- `CassandraCounterFactory`: cache can be enabled/disabled.


0.5.0 - 2014-08-06
------------------

- Major algorithm changes:
  - `AbstractCounter.getSeries(...)`: `timestampStartMs` and `timestampEndMs` are no-longer rounded, caller is responsible for providing correct timestamp-range.
  - `AbstractCounter.getLastN(...)`: update rounding formula to cope with change(s) in method `getSeries(...)`.
- `CassandraCounter`:
  - (Experimental) support `set` operator for counter columns.
  - (Experimental) support `add` operator for bigint columns.


0.4.2 - 2014-07-30
------------------

- General bug fix & peformance improvement.


0.4.1.1 - 2014-07-30
--------------------

- `ddth-tsc-redis`: use Redis pipeline operations to improve performance.


0.4.0.4 - 2014-04-18
--------------------

- `ddth-tsc-cassandra`: bug fix.
- `ddth-tsc-redis`: performance improvement.
- Bugs fix


0.4.0.3 - 2014-04-03
--------------------

- `ddth-tsc-redis`: minor storage space optimization.
- Pom fixed


0.4.0 - 2014-04-02
------------------

- `ddth-tsc-inmem` and `ddth-tsc-cassandra` are now sub-projects of `ddth-tsc`.
- New sub-project `ddth-tsc-redis`


0.3.2 - 2014-03-28
------------------

- Bugs fix & improvements.


0.3.1.1 - 2014-03-27
--------------------

- Bugs fix.


0.3.1 - 2014-03-17
------------------

- POM fix.


0.3.0 - 2014-03-12
------------------

- Add `ICounter.set()` methods.
- Add aggregation support.


0.2.0 - 2014-03-10
------------------

- Packaged as OSGi bundle.
- Performance improvements.


0.1.1 - 2014-03-07
------------------

- Add methods `AbstractCounterFactory.init()`, `AbstractCounterFactory.destroy()`.
- Minor docs update


0.1.0 - 2014-03-06
------------------

- First release.
