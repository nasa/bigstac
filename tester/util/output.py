from util import output

thread_info = {} # mapping of thread ids to thread counts starting at 0
logger = logging.getLogger(__name__)

logging.basicConfig(filename=f"{os.path.basename(__file__)}.log", level=logging.INFO)

def error(msg):
    print(red(msg))

def print_at_thread(anything, offset=1):
    ''' Print on a specific line for each thread '''
    y = thread_info[threading.current_thread().ident] + 1 + offset
    print_at(anything, 1, max(1,y))

def print_at(anything, x, y):
    print(f"\033[{y};{x}H\033[2K{anything}", end='')

def red(msg:str, detail:str = ''):
    return f"\033[31m{msg}\033[0m{detail}"

def green(msg:str, detail:str = ''):
    return f"\033[32m{msg}\033[0m{detail}"

def log_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        mark_start = now_in_ms()
        func(*args, **kwargs)
        mark_stop = now_in_ms()
        log.info(f"{func.__name__}: {mark_stop - mark_start}")
    return wrapper
