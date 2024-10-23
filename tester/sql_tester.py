#!/usr/bin/env python3

'''
Take the test config file and generate a list of sql statments. In some cases new sql statments will
be created based on the current ones to expand out the test cases, such as making sure that there is
always a matching SELECT * version of all queries, or an unsorted (dropping ORDER BY).

example run:

 ./sql_tester.py out.csv
    --tries 10
    --note authors-run
    --data "\"../path/to/data/*.parquet\""

'''

import argparse
import csv
import sys
import time

from util import file as filer
from util import output
from util import stats
from util import tools

from target_duckdb import engine as duck
from target_duckdb import native as mallard

# ################################################################################################ #
# Mark: - Functions

def run_one_test(engine:duck, args:argparse.Namespace, stat:stats.Stats, data:dict):
    ''' Run one query and record the statistical information about that call. '''
    data_dir = args.data

    config_name = f"{data['name']}-{data['action']}"
    test_query = data['sql']
    test_query = test_query.replace('{data}', data_dir)
    output.log.debug (test_query)
    out = None
    for _ in range(args.tries):
        mark_start = int(time.time() * 1000)
        out = engine.run_test(test_query)
        mark_stop = int(time.time() * 1000)

        #4. take stats
        mark_diff = mark_stop - mark_start
        stat.value(mark_diff, config_name)
        sub = stat.get_sub(config_name)
        sub.value(mark_diff)
        sub.note("note", args.note)

        #5. validate response
        valid = None
        #if config.expected:
        #    # configuration has an expected setting, so verify it
        #    valid = engine.verify(config.expected, out)
        #    stat.add('valid' if valid else 'failed', 1) # top level, all tests
        #    sub.add('valid' if valid else 'failed', 1) # lower level, just this test
        output.log.info("\tn=%s\tr=%d\tms=%d\tv=%s",
            config_name, len(out), mark_stop-mark_start, valid)
    out = str(len(out))
    return out #give the last one back so there is something to work with in the caller


def run(args:argparse.Namespace):
    '''
    Run the steps of the script:
    1. Parse configuration
    2. Select target engine
    3. Run the tests in each row
    4. Write out the results
    '''

    # 1. Parse configuration
    if args.config is None and sys.stdin.isatty():
        output.error("No configuration file provided.")
        sys.exit(1)

    data = None # Decoded CSV rows
    if args.config:
        with open(args.config, 'r', encoding='utf-8') as file:
            # Assuming 'file' is a file object opened in read mode
            csv_reader = csv.DictReader(file)
            data = list(csv_reader)
    else:
        # read from standard in
        csv_reader = csv.DictReader(sys.stdin)
        data = list(csv_reader)

    # 2. select test target engine
    engine = None
    if args.system == 'duckdb':
        engine = duck.DuckDbSystem()
    elif args.system == 'mallard': # duckdb using a native database ; Mallards are native to America
        # not well tested at this point (2024-10-18)
        engine = mallard.NativeDuckSystem(args.database)
    else:
        output.error("Unknown system name.")
        sys.exit(2)

    # 3. run the tests in each row
    stat = stats.Stats()
    suite_name = data[0]["suite"] # this column should be the same for all rows
    output.log.critical("Starting test run: %s - %s...", suite_name, args.note)
    for row in data:
        # NOTE: need to decode SQL from CSV: replace \n with newline
        sql = row['sql'].replace('\\n', '\n')
        row['sql'] = sql
        run_one_test(engine, args, stat, row)

    # 4. write out the results
    base_name = f"reports/{tools.iso_ish()}-{tools.file_safe(suite_name)}-{args.note}"
    filer.create('reports')
    filer.write(stat.dump(), f"{base_name}.json")
    stat.csv(f"{base_name}.csv")

    output.log.info("#"*57)

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Accept a CSV of sql commands that are tested.")

    # Add command-line arguments
    parser.add_argument("--config", required=False, help='Path to csv input file.')
    parser.add_argument("-d", "--data",
        help='Path to data files or name of DuckDB table which goes into {data}. Include any quotes or [] as needed')
    parser.add_argument("-n", "--note", default='normal',
        help='give a note about this specific run.')
    parser.add_argument("-t", "--tries", default='8', type=int,
        help="Number of times to run a test.")
    parser.add_argument("-s", "--system", default='duckdb', help="engine to test, duckdb")
    parser.add_argument("-b", "--database", default='~/test_lpcloud_data/single_file/native.db',
        help="path to DuckDB database")

    # Parse arguments
    args = parser.parse_args()
    return args

def main():
    ''' Be a command line app. '''
    output.init_logging(__file__)
    args = handle_args()
    run(args)

if __name__ == "__main__":
    main()
