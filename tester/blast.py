#!/usr/bin/env python3

'''
Perform a set of tests against a parquet database like duckdb
'''

import argparse
import sys
import logging
import os
import time

from util import test_config as tconfig
from util import output

from target_duckdb import engine as duck

# ################################################################################################ #
# Mark: - Functions

def parse_config(path:str)->dict:
    ''' Parse a json file and convert it to a TestConfig object '''
    config = None
    with open(path, 'r', encoding='utf-8') as file:
        #config = json.load(file)
        config = file.read()
        return tconfig.from_json(config)
    return config

def run(args):
    ''' Handle the script tasks '''

    stats = {'count': 0, 'total_ms': 0, 'valid': 0, 'failed': 0, 'longest_ms': 0, 'longest_id': ''}

    # 1. Parse configuration
    if args.config is None:
        output.error("No configuration file provided")
        sys.exit(1)
    config = parse_config(args.config)

    # 2. select test target engine
    engine = None
    if args.system == 'duckdb':
        engine = duck.DuckDbSystem()

    # 3. create search query as generator
    engine.use_configuration(config)
    for test, config in engine.generate_tests():
        test = test.replace('{data}', args.data)
        output.log.debug (test)
        mark_start = int(time.time() * 1000)
        #out = do_run(engine, test)
        out = engine.run_test(test)
        mark_stop = int(time.time() * 1000)
        mark_diff = mark_stop - mark_start
        stats['count'] = stats['count'] + 1
        stats['total_ms'] = stats['total_ms'] + mark_diff
        if stats['longest_ms'] < mark_diff:
            stats['longest_ms'] = mark_diff
            stats['longest_id'] = config.name

        #4. validate response
        valid = False
        valid = verify(config.expected, out)
        output.log.info(f"\tn={config.name}\tr={len(out)}\tms={mark_stop-mark_start}\tv={valid}")
        if valid:
            stats['valid'] = stats['valid'] + 1
        else:
            stats['failed'] = stats['failed'] + 1

    # 5. generate report
    stats['average_ms'] = stats['total_ms'] / stats['count']
    print(f"{stats}")

def verify(expected, data) -> bool:
    if not expected or not expected.action or not expected.value:
        return true
    if expected.action == 'count':
        return expected.value == len(data)
    elif expected.action == 'greater-then':
        return expected.value < len(data)
    elif expected.action == 'less-then':
        return expected.value > len(data)
    elif expected.action == 'exact':
        return str(expected.value) == data
    elif expected.acton == 'contains':
        if expected.value in data:
            return true
    return false

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Description of your script")

    # Add command-line arguments
    parser.add_argument("config", help="path to configuration file")
    parser.add_argument("-d", "--data", help="path to data files")
    parser.add_argument("-s", "--system", help="path to configuration file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")

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
