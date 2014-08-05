ddth-tsc release notes
======================

0.5.0 - 2014-08-06
------------------
- Major algorithm changes:
  - `AbstractCounter.getSeries(...)`: `timestampStartMs` and `timestampEndMs` are no-longer rounded, caller is responsible for providing correct timestamp-range.
  - `AbstractCounter.getLastN(...)`: update rounding formula to cope with change(s) in method `getSeries(...)`.


0.4.2 - 2014-07-30
------------------
- General bug fix & peformance improvement.


0.4.0.4 - 2014-04-18
--------------------
- Bug fix: zero/empty data point behavior.


0.4.0 - 2014-04-01
------------------
- Merged as a sub-project of [ddth-tsc](https://github.com/DDTH/ddth-tsc).
