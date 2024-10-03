''' Functions related to writing output to the terminal '''
#from util import output

import functools
import logging
import os
import time
import threading

thread_info = {} # mapping of thread ids to thread counts starting at 0
log = None

def init_logging(file_name):
    global log
    logging.basicConfig(filename=f"{os.path.basename(file_name)}.log", level=logging.INFO)
    log = logging.getLogger(os.path.basename(file_name))

def error(msg):
    ''' Print out an error message. '''
    print(red(msg))

def print_at_thread(anything, offset=1):
    ''' Print on a specific line for each thread '''
    y = thread_info[threading.current_thread().ident] + 1 + offset
    print_at(anything, 1, max(1,y))

def print_at(anything, x, y):
    ''' Print a message at a specific x,y on the terminal. '''
    print(f"\033[{y};{x}H\033[2K{anything}", end='')

def red(msg:str, detail:str = ''):
    ''' Add terminal codes to turn text red. '''
    return f"\033[31m{msg}\033[0m{detail}"

def green(msg:str, detail:str = ''):
    ''' Add terminal codes to turn text green. '''
    return f"\033[32m{msg}\033[0m{detail}"

def log_time(func):
    ''' A decorator to log the execution time of a function. '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        mark_start = int(time.time() * 1000)
        rtn = func(*args, **kwargs)
        mark_stop = int(time.time() * 1000)
        #log.info(f"{func.__name__}: {mark_stop - mark_start}")
        log.info('%s: %s', func.__name__, mark_stop - mark_start)
        #print(f"{func.__name__}: {mark_stop - mark_start}")
        return rtn
    return wrapper
