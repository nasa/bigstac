#!/usr/bin/env python3

''' Perform a set of tests against a parquet database like duckdb using just one thread. '''

import argparse
import concurrent.futures
import functools
import multiprocessing
import sys
import time

from util import test_config
from util import output
from util import stats
from util import file
from util import tools
import os
from target_duckdb import engine as duck

# ################################################################################################ #
# Mark: - Functions

def run_one_test(engine:duck, tries:int, data_dir:str, stat:stats.Stats, data:dict):
    ''' Run one query and record the statistical information about that call. '''
    test_query = data[0]
    config = data[1]
    test_query = test_query.replace('{data}', data_dir)
    output.log.debug (test_query)
    out = None
    for _ in range(tries):
        mark_start = int(time.time() * 1000)
        out = engine.run_test(test_query)
        mark_stop = int(time.time() * 1000)

        #4. take stats
        mark_diff = mark_stop - mark_start
        stat.value(mark_diff, config.name)
        sub = stat.get_sub(config.name)
        sub.value(mark_diff)

        #5. validate response
        valid = None
        if config.expected:
            # configuration has an expected setting, so verify it
            valid = engine.verify(config.expected, out)
            stat.add('valid' if valid else 'failed', 1) # top level, all tests
            sub.add('valid' if valid else 'failed', 1) # lower level, just this test
        output.log.info("\tn=%s\tr=%d\tms=%d\tv=%s",
            config.name, len(out), mark_stop-mark_start, valid)
    return out #give the last one back so there is something to work with in the caller

def run(args):
    ''' Handle the script tasks '''

    stat = stats.Stats()
    # 1. Parse configuration
    if args.config is None:
        output.error("No configuration file provided")
        sys.exit(1)
    config = test_config.from_file(args.config)

    output.log.critical(f"Starting test run: {args.note} - {config.name}")
    if args.verbose:
        print(f"Starting test run: {args.note} - {config.name}")

    # 2. select test target engine
    engine = None
    if args.system == 'duckdb':
        engine = duck.DuckDbSystem()
    elif args.system == 'mallard': # duckdb using a native database ; Mallards are native to America
        # not well tested at this point (2024-10-18)
        engine = duck.NativeDuckSystem('~/test_lpcloud_data/single_file/native.db')
    else:
        output.log.error('no engine defined')
        sys.exit(-1)

    # 3. create search query as generator
    engine.use_configuration(config)

    mode = "single" # vs process, thread

    if mode == 'single':
        # One thread at a time
        for resp in engine.generate_tests():
            result = run_one_test(engine, args.tries, args.data, stat, resp)
            output.log.debug(result)

    elif mode == 'process':
        # original action, multiple threads used, but watch connections
        with multiprocessing.Pool() as p:
            tester = functools.partial(run_one_test, engine, args.tries, args.data, stat)
            for result in p.map(lambda x : tester, engine.generate_tests()):
                if result is not None:
                    output.log.debug(result)

    elif mode == 'thread':
        # and idea that may not be fully functioning
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(lambda x : run_one_test(engine, args.tries, args.data, stat, x),
                engine.generate_tests(),
                timeout=60)
            for r in results:
                if r is not None:
                    output.log.debug(r.get())

    #6. generate report
    base_name = f"{tools.iso_ish()}-{tools.file_safe(config.name)}-{args.note}"
    file.write(stat.dump(), f"{base_name}.json")
    stat.csv(f"{base_name}.csv")

    output.log.info("#"*80)

    if args.verbose:
        print(stat.dump())
        print("#"*80)

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Description of your script")

    # Add command-line arguments
    parser.add_argument("config", help='Path to configuration file.')
    parser.add_argument("-d", "--data",
        help='Path to data files which goes into {data}. Include any quotes or [] as needed')
    parser.add_argument("-n", "--note", default='normal', help='give a note about this specific run.')
    parser.add_argument("-m", "--mode", default='single', help='Processing mode. single is best.')
    parser.add_argument("-s", "--system", default='duckdb', help="path to configuration file.")
    parser.add_argument("-t", "--tries", default='8', type=int,
        help="Number of times to run a test.")
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
