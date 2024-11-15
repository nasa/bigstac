
# Duckdb logging

A very short list of commands that can be used to log in Duckdb


## Turning things on

To get a query plan out of duckdb prepend your query with `explain analyze`.

    explain analyze <sql command>


There are also a set of pragmas you can use:

https://duckdb.org/docs/configuration/pragmas

* .timer on
* set enable_profiling = 'json';
* set profiling_output = './log.json';
* set profiling_mode = 'detailed';

## Other notes

To get HTTP verbs logged, you will need to access s3

https://duckdb.org/docs/extensions/httpfs/s3api
