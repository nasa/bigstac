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

Input choices:

* Top level Parquet (metadata) Field 'Bounds' filtering
* Top level Parquet (metadata) datetime Fields filtering
* Parquet file
	* count
		* one per collection
		* one per provider
	* size
* Parquet row count
* Parquet Row Group size
* Parquet grouping
	* by location
	* by time
* wild cards in paths
	* by provider
	* by collection
	* by partial granule id


## Testing Application

TBD - A testing application that will convert the test configuration file into search commands specific to the search engine that is to be worked against.

### blast.py

This is a standalone tester which will load the configuration file, generate queries and then run those queries against the target engine (currently only duckdb). This tool may be depricated in favor of a locust test.

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


## License
Copyright &copy; 2024 United States Government as represented by the Administrator of the National Aeronautics and Space Administration. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
