# Testing Big Stac

A sub-project to bulk test different BigSTAC solutions

## Overview

Create a testing platform for conduting sets of codafied tests against different BigSTAC solutions.

## Test Definition

A JSON schema and configuration file specifying the set of tests to perform against any candidate sysem. The basic idea is to define a Temporal, Spatial, or parameter search in the abstract and then have those tests translated to what ever input is required for the testing target. For example, duckdb takes SQL.

To write a test, create a file that conforms to the schema.json file. Implimentations of this schema contain a list of `operations` which are at this time are of type `ands` (and). Each member of the operations list contains:

| Field       | Required | Example      | Description |
| ----------- | -------- | ------------ | ----------- |
| type_of     | Yes      | geometry     | Search time, one of [geometry, time, parameter]
| option      |          | intersects   | Specific to type, ex: geometry: intersects
| value       | Yes      | POLYGON((... | data to search with, ex: geometry: a Polygon
| description |          | anything     | optional note on the test

Each test also needs a `name` and an optional description.

## Testing cases

* query
	* all records (no input?)
	* spatial only
	* temporal only
	* field only
	* spatial & temporal
	* spatial & field
	* temporal & field
	* spatial, temporal, & field

## Testing Application

| Application   | Status | Usage
| ------------- | ------ | ----
| blast.py      | Draft  | A full testing app which tries to test multiple instances of duckdb
| create_sql.py | ?      | Takes the same configuration files and generates a CSV of sql statments
| locustfile.py | Done   | Like blast, but in locust for the bug lovers
| run.duckdb.py | n/a    | A wrapper for use in blast
| single.py     | Done   | Runs the configuration and generates
| sql_tester.py | ?      | Runs a sequential list of tests from create_sql.py


### blast.py

A testing application that will convert the test configuration file into search commands specific to the search engine that is to be worked against.

This is a standalone tester which will load the configuration file, generate queries and then run those queries against the target engine (currently only duckdb). This tool may be depricated in favor of a locust test.

---

### locustfile.py

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

These two applcations work off of the same configuration files but split up the work of generating tests and then testing them. First generate a list of statments with:

	./create_sql.py --all --order suite.json > out.csv

where:
* --all, will add tests with `SELECT *` unless test already is a star select
* --order, will add tests without order by unless test alread is missing order by
* suite.json, no flag given, name of config file

Output is CSV and can be piped to a file.

To run tests use:

	./sql_tester.py \
		--data "../../path/to/data/*.parquet" \
		--note authors-run \
		out.csv

where:
* --data is the path to parquet file or where to start looking for them if the config file has paths
* --note text to include in report about the run
* out.csv, no flag given, name of config file

Output will be written similar to `single.py` but to a `reports` directory so as to not get in the way of those runs.

## Findings

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

## License
Copyright &copy; 2024 United States Government as represented by the Administrator of the National Aeronautics and Space Administration. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
