#!/usr/bin/env python3

import argparse
import json
import os

import util

from target_duckdb import engine as duck

def parse_config(path:str)->dict:
    config = {}
    with open(path, 'r') as file:
        config = json.load(file)
    return config

def run(args):
    # 1. Parse configuration
    if args.config is None:
        output.error("No configuration file provided")
        os.exit(1)
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
        #engine.run_test(test)

    # 4. start test engine
    # 5. generate report

    pass

# ################################################################################################ #

def handle_args() -> argparse.Namespace:
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
    args = handle_args()
    run(args)

if __name__ == "__main__":
    main()
