# Testing Big Stac

A sub-project to bulk test different BigSTAC solutions

## Overview

Cretae a testing platform for conduting sets of codafied tests against different BigSTAC solutions.

## Test Definition

A JSON schema and configuration file specifying the set of tests to perform against any candidate sysem. The basic idea is to define a Temporal, Spatial, or parameter search in the abstract and then have those tests translated to what ever input is required for the testing target. For example, duckdb takes SQL.

To write a test, create a file that conforms to the schema.json file. Implimentations of this schema contain a list of `operations` which are at this time of type `and`. Each member of the operations list contains:

| Field       | Required | Example      | Description |
| ----------- | -------- | ------------ | ----------- |
| type        | Yes      | geometry     | Search time, one of [geometry, time, parameter]
| option      |          | intersects   | Specific to type, ex: geometry: intersects
| value       | Yes      | POLYGON((... | data to search with, ex: geometry: a Polygon
| description |          | anything     | optional note on the test

## Testing cases

* Top level Parquet (metadata) Field 'Bounds' filtering
* Top level Parquet (metadata) datetime Fields filtering
* Parquet file
	* count
		* one per collection
		* one per provider
	* size
* Parquet row count
* Parquet Row Group size
* wild cards in paths
	* by provider
	* by collection
	* by partial granule id
* query
	* all records (no input?)
	* spatial only
	* temporal only
	* field only
	* spatial & temporal
	* spatial & field
	* temporal & field
	* spatial, temporal, & field
* Parquet grouping
	* by location
	* by time

## Testing Application

TBD - A testing application that will convert the test configuration file into search commands specific to the search engine that is to be worked against.

### bigstac

TBD
