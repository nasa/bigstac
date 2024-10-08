#!/usr/bin/env python3

'''
Perform a set of tests against a parquet database like duckdb
'''

import argparse
import sys
import time
import concurrent.futures
import functools
import multiprocessing

from util import test_config
from util import output
from util import stats

from target_duckdb import engine as duck

# ################################################################################################ #
# Mark: - Functions

def parse_config(path:str)->dict:
    ''' Parse a json file and convert it to a TestConfig object '''
    config = None
    with open(path, 'r', encoding='utf-8') as file:
        #config = json.load(file)
        config = file.read()
        return test_config.from_json(config)
    return config

def run_one_test2(engine, data_dir, stat, data):
    run_one_test(data, engine, data_dir, stat)

def run_one_test(data, engine, data_dir, stat):
    test = data[0]
    config = data[1]
    test = test.replace('{data}', data_dir)
    output.log.debug (test)
    for i in range(10):
        mark_start = int(time.time() * 1000)
        print("start")
        out = engine.run_test(test)
        print("stop")
        mark_stop = int(time.time() * 1000)
        mark_diff = mark_stop - mark_start
        stat.add('count', 1)
        stat.add('total_ms', mark_diff)
        stat.max('longest_ms', mark_diff, {'longest_id': config.name})

        #4. validate response
        valid = engine.verify(config.expected, out)
        stat.add('valid' if valid else 'failed', 1)
        output.log.info("\tn=%s\tr=%d\tms=%d\tv=%s",
            config.name, len(out), mark_stop-mark_start, valid)
        ### - end of run_one_test()

def run(args):
    ''' Handle the script tasks '''

    stat = stats.Stats()
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
    #for resp in engine.generate_tests():
    if True:
        with multiprocessing.Pool() as p:
            foo = lambda x : functools.partial(run_one_test2, engine, args.data, stat)
            for result in p.map(foo, engine.generate_tests()):
                if r is not None:
                    print(r)

    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(lambda x : run_one_test(x, engine, args.data, stat),
                engine.generate_tests(),
                timeout=60)
            for r in results:
                if r is not None:
                    print(r.get())

    # 5. generate report
    stat.store('average_ms', stat.get('total_ms', 1) / stat.get('count', 1))
    output.log.info(stat.dump())
    #print(stat.dump())

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
