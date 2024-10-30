#!/usr/bin/env python3

'''
Take the test config file and generate a list of sql statements. In some cases new sql statements
will be created based on the current ones to expand out the test cases, such as making sure that
there is always a matching SELECT * version of all queries, or an unsorted (dropping ORDER BY).

NOTE: As a feature this code only operates on SQL statements that are in upper case. If it is needed
to prevent a query from being modified, use lowercase and the regular expressions will not trigger.

example run:

./create_sql.py suite.json --all --order > out.csv
'''

import argparse
import csv
import io
import re
import sys
from typing import Callable
import functools

from util import output
from util import test_config
from target_duckdb import engine as duck
from target_duckdb import native as mallard

# ################################################################################################ #
# Mark: - Functions

def select_engine(system_name:str) -> duck:
    ''' Select the engine to use for the test suite '''
    engine = None
    if system_name == 'duckdb':
        engine = duck.DuckDbSystem()
    elif system_name == 'mallard': # duckdb using a native database ; Mallards are native to America
        engine = mallard.NativeDuckSystem("")
    else:
        output.error("Unknown system name.")
        sys.exit(2)
    return engine

def to_csv_file(out_file:str, queries:list):
    ''' Write out a CSV file with the list of queries '''
    headers = ['suite', 'name', 'action', 'sql']
    with open(out_file, 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for item in queries:
            writer.writerow(item)

def to_csv_string(queries: list) -> str:
    ''' Convert the list of queries to a CSV string to be printed or saved latter '''
    headers = ['suite', 'name', 'action', 'sql']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=headers)
    writer.writeheader()
    for item in queries:
        writer.writerow(item)

    return out.getvalue()

def assert_something(target: Callable[[str], str], msg: str, expected:str, given:str):
    '''
    A very basic wrapper around assert for some basic testing. Compare the expected and actual
    values and print a message if they are not equal. Given values are run through target first.
    '''
    actual = target(given)
    assert expected == actual, f'💣 {msg}: expected [{expected}] but got [{actual}]!'

def encode_csv_row(suite_name: str,
    test_settings:test_config.AssessConfig,
    action_taken:str,
    sql:str) -> dict:
    '''
    Create a dictionary with all the data needed to write one CSV row.
    NOTE: SQL will have newlines encoded as '\n' while in CSV and consumers will need to decode
    these tokens for use.
    '''
    row = {'suite': suite_name,
        'name': test_settings.name,
        'action': action_taken,
        'sql': sql.replace('\n', '\\n')}
    return row

# ##################################################

def swap_limit(limit:int, sql:str) -> str:
    '''
    Replace the LIMIT clause with the supplied value. If there is not a valid limit value, then drop
    the SQL LIMIT clause.
    '''
    new_query = sql
    if 'LIMIT' in sql:
        if limit < 1:
            #drop limit
            new_query = re.sub(r'\sLIMIT\s+\d+', '', sql, flags=re.IGNORECASE)
        else:
            # augment limit
            pattern = r'(?<=\bLIMIT)\b\s*\d+'
            new_query = re.sub(pattern, ' ' + str(limit), sql, flags=re.IGNORECASE)
    return new_query

# Do some inline testing on a few cases
# test_swap_limit is a tester for swap_limit
test_swap_limit = functools.partial(assert_something, functools.partial(swap_limit, 128))
test_swap_limit('Basic limit swap',
    'SELECT * FROM table_name WHERE condition LIMIT 128',
    'SELECT * FROM table_name WHERE condition LIMIT 10')
test_swap_limit('Limit swap with no limit',
    'SELECT * FROM table_name WHERE condition',
    'SELECT * FROM table_name WHERE condition')
test_swap_limit('Limit swap with no limit and order by',
    'SELECT * FROM table_name WHERE condition ORDER BY foo LIMIT 128',
    'SELECT * FROM table_name WHERE condition ORDER BY foo LIMIT 10')
test_swap_limit('Limit swap with no limit and order by and limit',
    'SELECT * FROM table_name WHERE condition ORDER BY foo LIMIT 128',
    'SELECT * FROM table_name WHERE condition ORDER BY foo LIMIT 10')
test_drop_limit = functools.partial(assert_something, functools.partial(swap_limit, -1))
test_drop_limit('Drop limit Limit swap with no limit and order by and limit',
    'SELECT * FROM table_name WHERE condition ORDER BY foo',
    'SELECT * FROM table_name WHERE condition ORDER BY foo LIMIT 10')

# ##################################################

def swap_select(sql:str, replacement:str = ' * ') -> str:
    ''' Replace the select clause with the replacement string if it is the SELECT * query. '''
    if not 'SELECT *' in sql:
        pattern = r'(?<=\bSELECT\b).*?\s*(?=\bFROM\b)'
        new_query = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        return new_query
    return sql

# Do some inline testing on a few cases
# test_swap_select is the most basic of test cases
test_swap_select = functools.partial(assert_something, swap_select)
test_swap_select('Basic select swap',
    'SELECT * FROM table_name WHERE condition',
    'SELECT column1, column2 FROM table_name WHERE condition')
test_swap_select("Select swap was already a star",
    "SELECT * FROM table_name WHERE condition",
    "SELECT * FROM table_name WHERE condition")
# change up the default swap token
assert_something(lambda x: swap_select(x, " *\n"),
    "New Line after star in select swap",
    "--fix it\nSELECT *\nFROM table_name\nWHERE condition\n",
    "--fix it\nSELECT column1, column2\nFROM table_name\nWHERE condition\n")
assert_something(lambda x: swap_select(x, " x\n"),
    'Match nothing, change nothing in select swap',
    "--no change\nSELECT *\nFROM\ntable_name\nWHERE condition",
    "--no change\nSELECT *\nFROM\ntable_name\nWHERE condition")

# ##################################################

def remove_order_by(sql:str) -> str:
    ''' Remove the order by clause from the sql statement '''
    if 'ORDER BY' in sql:
        # pylint: disable=line-too-long
        pattern = r'\bORDER\s+BY\s+.*?(?=\b(LIMIT|OFFSET|FETCH|FOR|UNION|INTERSECT|EXCEPT)\b|;\s*$|\Z|$)'
        new_query = re.sub(pattern, '', sql, flags=re.IGNORECASE | re.DOTALL)
        return new_query.strip()
    return sql

# Do some inline testing on a few cases
# test_remove_order_by is the basic test case
test_remove_order_by = functools.partial(assert_something, remove_order_by)
test_remove_order_by('failed 1',
    "SELECT c1, c2 FROM table_name WHERE condition LIMIT 100",
    "SELECT c1, c2 FROM table_name WHERE condition ORDER BY column1 LIMIT 100")
test_remove_order_by('failed 2',
    "SELECT column1, column2 FROM table_name WHERE condition",
    "SELECT column1, column2 FROM table_name WHERE condition ORDER BY column1")
test_remove_order_by('failed 3',
    "SELECT column1, column2 FROM table_name WHERE condition",
    "SELECT column1, column2 FROM table_name WHERE condition")
test_remove_order_by('failed4',
    "--fix it\nSELECT column1, column2\nFROM table_name\n",
    "--fix it\nSELECT column1, column2\nFROM table_name\n")

# ##################################################

def run(args:argparse.Namespace):
    '''
    Handle the script tasks in 4 steps:
    1. Parse configuration
    2. Select test target engine
    3. Create search query from generator
    4. Output the queries
    '''

    # 1. Parse configuration
    config = test_config.from_file(args.config)
    output.log.log(output.LOG_ALWAYS, "Starting create sql: %s.", config.name)

    # 2. select test target engine
    engine = select_engine(args.system)
    engine.use_configuration(config)

    # 3. Create search query from generator
    queries = []
    for resp in engine.generate_tests():
        test_query = resp[0]
        test_settings = resp[1]

        if not args.no_orig:
            queries.append(encode_csv_row(config.name, test_settings, "flag-0-base", test_query))

        # set bit flag as such Limit-Order-All
        flags = (4 if args.limit else 0) | (2 if args.order else 0) | (1 if args.all else 0)
        added = [] # list of flag combinations added for this round of tests

        # try every combination from 001 to 111 but ignore the ones not selected with flags
        # thus the flag & (flag & number) test
        for flag in range(1, 8):
            current_query = test_query
            flag_name = ''
            if flags & (flag & 4):
                flag_name = flag_name + f"-limit:{args.limit}"
                current_query = swap_limit(args.limit, current_query)
            if flags & (flag & 2):
                flag_name = flag_name + "-orderless"
                current_query = remove_order_by(current_query)
            if flags & (flag & 1):
                flag_name = flag_name + "-all"
                current_query = swap_select(current_query)

            if flag_name:
                if flag_name in added:
                    continue # only add each combination once
                added.append(flag_name)
                flag_tag = f"flag-{flag}{flag_name}"
                output.log.debug(flag_tag)

                queries.append(encode_csv_row(config.name, test_settings, flag_tag, current_query))

    # 4. output the queries
    if args.data:
        to_csv_file(args.data, queries)
    else:
        print(to_csv_string(queries), end=None)

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Generates a list of sql commands to be tested.")

    # Add command-line arguments
    parser.add_argument("config", help='Path to configuration file.')
    parser.add_argument("-d", "--data",
        help='Name of the csv file to write out.')
    parser.add_argument('-N', '--no-orig', action='store_true',
        help='Drop the original sql query in favor of the others')
    parser.add_argument('-o', '--order', action='store_true',
        help='Add ORDER BY check, adding queries without one if found.')
    parser.add_argument("-a", "--all", action='store_true',
        help='Add all "*" check, adding queries if SELECT * is not used.')
    parser.add_argument('-l', '--limit' , type=int,
        help='Limit the number of queries to generate.')
    parser.add_argument("-v", '--verbose-level', default='info',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        help='Set the logging level, default is info')
    parser.add_argument("-s", "--system", default='duckdb', help="engine to test, duckdb")

    # Parse arguments
    args = parser.parse_args()
    return args

def main():
    ''' Be a command line app. '''
    output.init_logging(__file__)
    args = handle_args()

    output.set_log_level(args.verbose_level)

    run(args)

if __name__ == "__main__":
    main()
