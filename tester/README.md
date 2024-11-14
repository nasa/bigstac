# Testing Big Stac

A hastily thrown together sub-project to bulk test different BigSTAC solutions.

TL;DR:

Just want to run tests? Create a [suite.json](suite.json) file that conforms to [util/test_config.py](util/test_config.py), pass it to `create_sql.py` and pipe the output to `sql_tester.py`. See #create_sql.py and sql_tester.py below.

	./create_sql.py suite.json | ./sql_tester.py --data '"../path/to/data/*.parquet"' --note run-two

## Overview

Create a testing platform for conduting sets of codafied tests against different BigSTAC solutions.

## Test Definition

A JSON schema and configuration file specifying the set of tests to perform against any candidate sysem. The basic idea is to define a Temporal, Spatial, or parameter search in the abstract and then have those tests translated to what ever input is required for the testing target. For example, duckdb takes SQL (the only target at this point).

The code itself actually uses [pydantic](https://docs.pydantic.dev/latest/) to validate the test definition, where
the JSON schema was to flush out the ideas on what the input should look like. You can find the definition at:
[util/test_config.py](util/test_config.py). The file also has a working JSON and YAML example which is validated
with some assert statments which can be run by calling `python3 util/test_config.py`. It is hoped that this script
will maybe do a better job then this readme.

To write a test, create a file that conforms to the schema.json file. Implimentations of this schema contain a list of `tests`. A test object then contains a list of `operations` which are at this time are of type `ands` (and). Each member of the operations list contains:

| Field       | Required | Example      | Description |
| ----------- | -------- | ------------ | ----------- |
| type_of     | Yes      | geometry     | Search time, one of [geometry, time, parameter]
| option      |          | intersects   | Specific to type, ex: geometry: intersects
| value       | Yes      | POLYGON((... | data to search with, ex: geometry: a Polygon
| description |          | anything     | optional note on the test

Alternatively you can supply a `raw` query which is a raw search query to be run against the target
engine, which in the case of duckdb is SQL. When doing this there still needs to be a placeholder for the data
to be loaded which test scripts will supply. Use `{data}` to do this, for sql this will always be in the `from`
statment. Operation based tests will use the `source` setting.

Finally, if there are any setup or takedown commands that need to be run, these can be specified at
the top level. For a working example, see suite.json.

Each test also needs a `name` and an optional description.

NOTE: the system also allows for the same configuration to be created as a YAML file.

Here is a simple example configuration:

    {
    "name": "Primary tests",
    "description": "A basic set of tests",
    "setup": {"sql": "SELECT version();"},
    "takedown": {"sql": "SELECT version();"},
    "tests": [
        {
            "name": "test1-with-time",
            "description": "conduct an intersecting box search which is then sorted",
            "columns": ["granuleid"],
            "operations": [
                {
                    "ands": [
                        {
                            "description": "does a box interset and find records",
                            "type_of": "geometry",
                            "option": "intersects",
                            "value": "POLYGON((...))"
                        }
                    ]
                }
            ],
            "sortby": "GranuleID",
            "source": "{data}",
            "expected": {"action": "greater-then", "value": 11208}
        },
		 {
            "name": "raw-test",
            "description": "conduct an time based search which is less then",
            "raw": "SELECT * FROM read_parquet({data}) WHERE StartTime <= '2015-06-29T16:21' ORDER BY granuleid LIMIT 2000",
            "source": "{data}"
        }
	   ]
    }

Don't skimp on the names and descriptions, many of these find their way into the result output and also the SQL
that is created to help debug issues.

## Testing Application

| Application   | Status | Usage
| ------------- | ------ | ----
| blast.py      | Draft  | A full testing app which tries to test multiple instances of duckdb
| create_sql.py | In Use | Takes the same configuration files and generates a CSV of sql statments
| locustfile.py | Draft  | Like blast, but in locust for the bug lovers
| run.duckdb.py | n/a    | A wrapper for use in blast
| single.py     | Done   | Runs the configuration and generates results.
| sql_tester.py | In Use | Runs a sequential list of tests from create_sql.py

### blast.py

(not activly maintained)

A testing application that will convert the test configuration file into search commands specific to the search engine that is to be worked against.

This is a standalone tester which will load the configuration file, generate queries and then run those queries against the target engine (currently only duckdb). This tool may be depricated in favor of a locust test.

---

### locustfile.py

(not activly maintained)

A [Locust](https://locust.io/) test which generates test queries from the Schema configuration and then runs them against the target engine (currently only duckdb).

Currently there is a limitation on using duckdb with multiple threads under python. To get around this duckdb is called through a script. This enables duckdb to be tested in a fresh memory space for each test. These calls are not reused for multiple calls.

#### Configuration

Configuration is done via environmental variables. The Following variables are supported:

| Name       | Example        | Description |
| ---------- | -------------- | ----------- |
| data_path  | data/NSIDC_ECS | Path to the directory direction
| call_count | 10             | Number of times to execute query against test engine
| test_file  | suite.json     | Search query config file
| engine     | duckdb         | Name of the engine to test against, currently only DuckDB

---

### single.py

	./single.py \
		--verbose
		--data '"../../path/to/data/*.parquet"' \
		--note 'fastrun' \
		suite.json \

* --verbose adds output to the console
* --data is the path to parquet file or where to start looking for them if the config file has paths
* --note text to add to reports indicating the nature of this run
* suite.json, no flag given, name of config file

Output will be written to two files starting with the following fields in the name:
1. an iso date and time
2. name of the test from config file
3. note from command line
4. format (json and csv)

For example: `2024-10-21_16-22-01-Primary_tests-fastrun.csv`

---

### create_sql.py and sql_tester.py

These two applcations work off of the same configuration files from above but split up the work of generating tests and the testing itself. These two scripts form the bulk of the lattest tests.

The **first** step is to generate a list of statments. These statments are based on the original config file but additional queries are created to create a more broad set of tests. The queries are changed to replace the select statment with a select all query. Limit as also changed with an input value. Finnally Order by is dropped. In all 8 queries can exist for one base query.

This can be done with:

	./create_sql.py --all --order suite.json > out.csv

where:

* --all, will add tests with `SELECT *` unless test already is a star select
* --order, will add tests without order by unless test allread is missing order by
* suite.json, no flag is given, name of config file

Other params are:

* --no-orig, will drop the original query. It is assumed that --all and/or --order will be used.
* --limit, add a limit to queries if they do not already have one

Output is CSV and can be piped to a file.

The **second** step is to run tests with:

	./sql_tester.py \
		--data "../../path/to/data/*.parquet" \
		--note run-one \
		--config out.csv

where:

* --data is the path to parquet file or where to start looking for them if the config file has paths
* --note text to include in report about the run, what is being tested.
* out.csv, no flag given, name of config file

Output will be written similar to `single.py` but to a `reports` directory so as to not get in the way of those runs.

Both scripts can be run together:

	./create_sql.py suite.json --all --order | \
		./sql_tester.py --data '"../../path/to/data/*.parquet"' --note run-two

## Findings

(more to be added)

### Multi processing

the duckdb python library will support multiple threads but only if a `conn = duckdb.connect()` is used first and then each thread gets a copy of the connection with `conn.cursor()`.

Read only mode is not supported with in-memory databases.

Other notes:

* https://duckdb.org/faq.html#how-does-duckdb-handle-concurrency-can-multiple-processes-write-to-duckdb
* https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html
* https://duckdb.org/docs/connect/concurrency.html
* https://duckdb.org/docs/guides/python/multiple_threads
* https://duckdb.org/docs/api/python/reference/#duckdb.connect
* https://duckdb.org/docs/data/multiple_files/overview.html#list-of-globs
* https://duckdb.org/docs/configuration/overview.html
* https://duckdb.org/docs/guides/performance/my_workload_is_slow

### Profiling

* https://duckdb.org/docs/configuration/pragmas

## License
Copyright &copy; 2024 United States Government as represented by the Administrator of the National Aeronautics and Space Administration. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
