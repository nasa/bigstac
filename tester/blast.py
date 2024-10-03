#!/usr/bin/env python3

'''
Perform a set of tests against a parquet database like duckdb
'''

import argparse
import sys

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
    for test in engine.generate_tests():
        test = test.replace('{data}', args.data)
        test = test.replace('{provider}', 'NSIDC_ECS')
        test = test.replace('{short_name}', 'ABLVIS1B_1')
        print (test)
        out = engine.run_test(test)
        print(out)

    # 4. start test engine
    # 5. generate report

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
    args = handle_args()
    run(args)

if __name__ == "__main__":
    main()
