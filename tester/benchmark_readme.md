Follow these steps to run the same benchmarks the NASA EED BiGSTAC team ran on 2024-11-15:

- Use Python 3.11 or higher
- Clone the https://github.com/nasa/bigstac/ repo
- Change to `tester` subdirectory
- Create a virtual environment
    - `python -m venv venv_tester`
- Activate the virtual environment
    - `source venv_tester/bin/activate`
- Install requirements
    - `pip install --upgrade pip`
    - `pip install -r requirements.txt`
- Run benchmarks
    - Single file example:
      - `./create_sql.py suite7.json | ./sql_tester.py --data "'path/to/file.parquet'" -n 'name_of_this_benchmark_run' -t 5`
    - Multiple files example:
      - `./create_sql.py suite7.json | ./sql_tester.py --data "['path/to/first_file.parquet', 'path/to/second_file.parquet']" -n 'name_of_this_benchmark_run' -t 5`
    - Multiple directories example:
      - `./create_sql.py suite7.json | ./sql_tester.py --data "['path/to/first_directory/*.parquet', 'path/to/second_directory/*.parquet']" -n 'name_of_this_benchmark_run' -t 5`

The benchmark results will be saved in JSON and CSV format to the `tester/reports` directory.

About Data:
-----------

**17 million granule dataset**<br>
This dataset is based on the 75m granule dataset (below), but retaining only the single largest shape for every unique granule ID.
- 2.4 GB compressed parquet data
- 41 files

The data in the `geohash_bins` prefix contains files split into 32 1-character resolution geohash bins, 8 larger spatial bins based on hemispheres or quadrants, and one file containing granules that intersect with all geohash bins (thereby being global or nearly global in extent).

**75 million granule dataset**<br>
This dataset contains duplicate records in that a single granule unique ID is split into multiple records when the geometry is a complex shape (e.g. with islands or with a hole).

The data in the following S3 prefixes contains a total of:
- 6.8 GB compressed parquet data
- 75,000,314 CMR granules
- 1,308 files

`data_wl_20120101_20150101_05pg/`<br>
`data_wl_20150101_20170701_05pg/`<br>
`data_wl_20170701_20190101_05pg/`<br>
`data_wl_20190101_20200101_05pg/`<br>
`data_wl_20200101_20210101_05pg/`<br>
`data_wl_20210101_20210705_05pg/`<br>

There is also a single file version:<br>
`merged_sorted_ddb_75mil.parquet`
- 3.5 GB compressed parquet data
- 75,000,314 CMR granules