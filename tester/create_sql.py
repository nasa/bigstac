#!/usr/bin/env python3

'''
Take the test config file and generate a list of sql statments. In some cases new sql statments will
be created based on the current ones to expand out the test cases, such as making sure that there is
always a matching SELECT * version of all queries, or an unsorted (dropping ORDER BY).
'''

import argparse
import csv
import io
import re
import sys

from util import output
from util import test_config
from target_duckdb import engine as duck

# ################################################################################################ #
# Mark: - Functions

def to_csv_file(out_file:str, queries:list):
    ''' Write out a CSV file with the list of queries '''
    headers = ['name', 'action', 'sql']
    with open(out_file, 'w', encoding='utf8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for item in queries:
            writer.writerow(item)

def to_csv_string(queries: list) -> str:
    ''' Convert the list of queries to a CSV string to be printed or saved latter '''
    headers = ['name', 'action', 'sql']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=headers)
    writer.writeheader()
    for item in queries:
        writer.writerow(item)

    return out.getvalue()

def encode_csv_row(test_settings:test_config.AssessConfig, action_taken:str, sql:str) -> dict:
    ''' Create a dictionary with all the data needed to write one CSV row '''
    row = {'name': test_settings.name,
        'action': action_taken,
        'sql': swap_select(sql, " * ").replace('\n', '\\n')}
    return row

def swap_select(sql:str, replacement:str) -> str:
    ''' Replace the select clause with the replacement string if it is the SELECT * query. '''
    if not 'SELECT *' in sql:
        pattern = r'(?<=\bSELECT\b).*?\s*(?=\bFROM\b)'
        new_query = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        return new_query
    return sql

#print(swap_select("SELECT column1, column2 FROM table_name WHERE condition", " * "))
#print(swap_select("SELECT * FROM table_name WHERE condition", " * "))
#print(swap_select("--fix it\nSELECT column1, column2\nFROM table_name\nWHERE condition\n", " *\n"))
#print(swap_select("--no change\nSELECT *\nFROM\ntable_name\nWHERE condition", "x\n"))
#sys.exit(2)

def remove_order_by(sql:str) -> str:
    ''' Remove the order by clause from the sql statement '''
    if 'ORDER BY' in sql:
        # pylint: disable=line-too-long
        pattern = r'\bORDER\s+BY\s+.*?(?=\b(LIMIT|OFFSET|FETCH|FOR|UNION|INTERSECT|EXCEPT|;\s*$|\Z))'
        new_query = re.sub(pattern, '', sql, flags=re.IGNORECASE)
        return new_query
    return sql

#print(remove_order_by("SELECT c1, c2 FROM table_name WHERE condition ORDER BY column1 LIMIT 100"))
#print(remove_order_by("SELECT column1, column2 FROM table_name WHERE condition ORDER BY column1"))
#print(remove_order_by("SELECT column1, column2 FROM table_name WHERE condition"))
#print(remove_order_by("--fix it\nSELECT column1, column2\nFROM table_name\n"))
#sys.exit(2)

def run(args):
    '''
    Handle the script tasks in 4 steps:
    1. Parse configuration
    2. Select test target engine
    3. Create search query as generator
    4. Output the queries
    '''

    # 1. Parse configuration
    if args.config is None:
        output.error("No configuration file provided.")
        sys.exit(1)
    config = test_config.from_file(args.config)

    output.log.critical("Starting sql dump: %s - %s.", "", config.name)

    # 2. select test target engine
    engine = None
    if args.system == 'duckdb':
        engine = duck.DuckDbSystem()

    # 3. create search query as generator
    engine.use_configuration(config)

    queries = []
    for resp in engine.generate_tests():
        test_query = resp[0]
        test_settings = resp[1]

        queries.append(encode_csv_row(test_settings, "base", test_query))
        if args.all and not 'SELECT *' in test_query:
            # add wild card case ; select everything
            queries.append(encode_csv_row(test_settings, 'everything',
                swap_select(test_query, " * ")))
        if args.order and 'ORDER BY' in test_query:
            # add an unsorted case ; no order by
            queries.append(encode_csv_row(test_settings, 'unordered', remove_order_by(test_query)))
        if args.all and args.order and not 'SELECT *' in test_query and 'ORDER BY' in test_query:
            # add an unsorted wild card case
            queries.append(encode_csv_row(test_settings, 'everything-unordered',
                remove_order_by(swap_select(test_query, " * "))))

    # 4. output the queries
    if args.data:
        to_csv_file(args.data, queries)
        print('data')
    else:
        print(to_csv_string(queries), end=None)

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Description of your script")

    # Add command-line arguments
    parser.add_argument("config", help='Path to configuration file.')
    parser.add_argument("-d", "--data",
        help='Name of the csv file to write out.')
    parser.add_argument('-o', '--order', action='store_true',
        help='Add ORDER BY check, adding queries without one if found.')
    parser.add_argument("-a", "--all", action='store_true',
        help='Add all "*" check, adding queries if SELECT * is not used.')

    parser.add_argument("-s", "--system", default='duckdb',
        help="engine to test, duckdb")

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
