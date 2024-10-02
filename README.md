# BigSTAC

An exploration on storing granule information as flat files on an S3 bucket and then searching with tools like DuckDB

## Links of Interest

* Background
  * https://en.wikipedia.org/wiki/GeoJSON
  * https://stacspec.org/en/tutorials/intro-to-stac/
* Tools
  * https://github.com/planetlabs/gpq
  * https://stac-utils.github.io/stac-geoparquet/latest/
  * https://github.com/cholmes/duckdb-geoparquet-tutorials
* API documentation
  * https://arrow.apache.org/docs/python/index.html

## Testing
A testing application exists at [tester](tester). This application will perform a set of queries
against a proposed setup. Initially this will be duckdb.
