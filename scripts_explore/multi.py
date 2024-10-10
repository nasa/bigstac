#!/usr/bin/env python3

'''
This is an exploration of how to use python threads to read a parquet file without blocking.
'''

# validated with `watch python3 $(which pylint) multi.py`

import argparse
import atexit
import concurrent.futures
import logging
import os
import sys
import threading
import time

import duckdb

# ################################################################################################ #
# Mark - Util functions
# These are mostly for fun. Be a terminal app that clears the screen and prints out text.

def record_thread_id():
    ''' Record the current thread id so that each thread can print messages to a different line. '''
    thread_id = threading.current_thread().ident
    if not thread_id in globals()['thread_info']:
        num_of_threads = len(globals()['thread_info'].keys())
        globals()['thread_info'][thread_id] = num_of_threads

def print_at_thread(anything, offset=1):
    ''' Print on a specific line for each thread '''
    y = globals()['thread_info'][threading.current_thread().ident] + 1 + offset
    print_at(anything, 1, max(1,y))

def print_at(anything, x, y):
    ''' Print anywhere on the screen, clearing the line as you print '''
    print(f"\033[{y};{x}H\033[2K{anything}", end='')

def flush_cui():
    ''' Force standard out to print which it might if threads are slowing things down. '''
    sys.stdout.flush()

def setup_cui():
    ''' Setup the Command line User Interface clear screen and prep for drawing '''
    print("\033[?47h", end='')  # save screen
    print("\033[s", end='')     # save cursor
    print("\033[?25l", end='')  # cursor off
    atexit.register(clean_up_on_exit)

def clean_up_on_exit():
    '''
    System call back function for when control-c is pressed, or normal exit. Restores the terminal.
    '''
    print("\033[?25h", end='')  # cursor on
    print("\033[u", end='')     # restore cursor
    print("\033[?47l", end='')  # restore screen

    # also close the database connection
    if globals()['conn']:
        duckdb.close(connection=globals()['conn'])

# ################################################################################################ #
# Mark - Thread Functions

def process_job(conn, details:dict) -> list:
    '''
    Do the work of one thread, search the database and log the time it took. Result from query is
    returned.
    '''
    result = None
    try:
        # Report on the current thread
        record_thread_id()
        thread_name = str(threading.current_thread().name)
        print_at_thread(f" Running {thread_name}")

        # Do query and time it
        time_start = time.time()*1000
        result = conn.sql(details['query']).fetchall()
        time_stop = time.time()*1000

        # Report both to the log and the CUI
        durration_of_run_ms = time_stop - time_start
        logging.info("%s: took=%dms size=%d", details['id'], durration_of_run_ms, len(result))
        print_at_thread(f"Finished {thread_name} in {durration_of_run_ms}ms.")
        flush_cui()
    #pylint: disable=broad-exception-caught # never will I agree to this
    except Exception as ex:
        logging.error(ex)
    return result

# ################################################################################################ #
# Mark - App functions

def work(args):
    ''' Create a work list and several threads to prove if Duckdb can handle it. '''
    globals()['thread_info'] = {}
    globals()['conn'] = conn = duckdb.connect(config = {'threads': 4,
        'worker_threads': 4,
        'external_threads': 4,
        'allocator_background_threads': True})

    # setup
    task_list = [] # things to do
    query = f"select * from read_parquet({args.data})"
    logging.debug(query)

    # create list of things to do
    for i in range (args.tasks):
        task_list.append({"id": f"q{i}", "query": query})

    # Create threads
    if 0 < len(task_list):
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Be sure to use cursor() to create a copy of the connection or you'll get into trouble
            results = executor.map(lambda x : process_job(conn.cursor(), x), task_list, timeout=120)
            # Go through all the outputs and print them out
            for r in results:
                if r is not None:
                    logging.debug(r)

    # Clean up
    duckdb.close(connection=conn)
    logging.info('Done.')

def main():
    ''' Responds to a call from the command line '''

    logging.basicConfig(filename=f"{os.path.basename(__file__)}.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Your program description")
    parser.add_argument("-w", '--workers', type=int, default=3, help="number of workers")
    parser.add_argument("-t", '--tasks', type=int, default=10, help="number of threads")
    parser.add_argument("-C", '--no-cui', action="store_true", help="don't take over the UI")
    parser.add_argument('-d', '--data', type=str, help="path to data")
    args = parser.parse_args()

    if args.no_cui:
        setup_cui()

    work(args)
    clean_up_on_exit()

if __name__ == "__main__":
    main()
