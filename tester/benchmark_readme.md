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
