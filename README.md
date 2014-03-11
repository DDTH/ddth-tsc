ddth-tsc
========

DDTH's In-memory Time Series Counter.

Project home:
[https://github.com/DDTH/ddth-tsc](https://github.com/DDTH/ddth-tsc)

OSGi environment: ddth-tsc is packaged as an OSGi bundle.

Note: for persistent Time Series Counter, consider using [ddth-tsc-cassandra](https://github.com/DDTH/ddth-tsc-cassandra).


## License ##

See LICENSE.txt for details. Copyright (c) 2014 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Maven Release #

Latest release version: `0.2.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-tsc</artifactId>
	<version>0.2.0</version>
</dependency>
```


## Usage ##

Use `ddth-tsc` to count things for a period of time. Such as:

- How does my application's memory usage look like for the last hour?
- How many hits per minute to my web site during 9AM to 11AM?

Sample code:

```java
//first: we need a counter factory
InmemCounterFactory counterFactory = new InmemCounterFactory();
counterFactory.init();

//second: we need the counter
ICounter countSiteVisits = counterFactory.getCounter("my-site-visits");

//third: count!
  //there is one visit to my site
counterSiteVisits.add(1);

  //at a specific time, there are 3 visits to my site
counterSiteVisits.add(unixTimestampMs, 3);

//last: get the data out
long timestampLastHour = System.currentTimeMillis() - 3600000;
DataPoint[] lastHour = counterSiteVisits.get(timestampLastHour);

long timestampLast15Mins = System.currentTimeMillis() - 15*60*1000; //15 mins = 900000 ms
DataPoint[] last15MinsGroupPerMin = counterSiteVisits.get(timestampLast15Mins, 15*60); //1 min = 60 secs

//destroy the counter factory when done
couterFactory.destroy();
```

### ICounter methods ###
- `add(...)`: add a value
- `get()`: get a single data point value
- `getSeries(...)`: get a series of data points
- `getLastN(...)`: get last N data points

