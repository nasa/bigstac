'''
A locust file to facilitate tests against a parquet database. This test takes the following inputs:
env variables:
*  data_path - Path to data such as '../scripts_explore/data/NSIDC_ECS'
* call_count - A number of the times to call each test query
* test_file - File listing all the test queries, json or yaml
* engine - parquet system to test against, currently only 'duckdb'
'''
import inspect
import os
import queue
import time

from locust import User, task, events
import duckdb

from util import test_config

from target_duckdb import engine as duck

# Note: to lint, use `watch python3 $(which pylint) locustfile.py` to find libs

# ################################################################################################ #

# pylint: disable=invalid-name

engine = None
work_provider = None

def check_function_implementation(func):
    ''' Tests if a class has implemented a function with something other then 'pass'. '''
    source_lines = inspect.getsourcelines(func)[0]
    ans = False
    for line in source_lines:
        if 'pass' in line:
            ans = True
            break
    return len(source_lines) > 1 and not ans
    #return len(source_lines) > 1 and not all('pass' in line for line in source_lines)

def parse_config(path:str)->dict:
    ''' Parse a json file and convert it to a TestConfig object '''
    config = None
    with open(path, 'r', encoding='utf-8') as file:
        config = file.read()
        if path[:4] == 'yaml':
            return test_config.from_yaml(config)
        return test_config.from_json(config)

    return config

class WorkItemProvider:
    '''
    A wrapper around the use_configuration to return a generator in the way that locust expects.
    '''
    def __init__(self, engine_to_use, data_file):
        ''' parse the config file and setup a generator with a queue '''
        self.queue = queue.Queue()
        config = parse_config(data_file)
        engine_to_use.use_configuration(config)
        for row in engine_to_use.generate_tests():
            self.queue.put(row)
    def get(self):
        ''' Pop off the queue '''
        try:
            return self.queue.get_nowait()
        except queue.Empty:
            return None
    def size(self):
        ''' Required to return queue size '''
        return self.queue.qsize()

# ################################################################################################ #

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    ''' Function called by Locust to start test '''
    environment.path = os.environ.get('data_path', '../scripts_explore/data/NSIDC_ECS')
    environment.call_count = int(os.environ.get('call_count', 2))
    environment.test_file = os.environ.get('test_file', 'suite.json')
    environment.engine = os.environ.get('engine', 'duckdb')
    environment.use_direct_command = False

    print(f"Using data path '{environment.path}' and config file '{environment.test_file}'.")
    print(f"Calling each test {environment.call_count} times against {environment.engine}.")
    print (f"Additional flags: {kwargs}")

    if environment.engine == 'duckdb':
        globals()['engine'] = duck.DuckDbSystem()
    else:
        print(f"💣 - No engine '{environment.engine}' defined.")
        environment.runner.stop()
    globals()['work_provider'] = WorkItemProvider(engine, environment.test_file)

class Foiegras(User):
    ''' Locust User Class to facilitate the tests. This version will name users with a number. '''

    user_count = 0

    def __init__(self, *args, **kwards):
        super().__init__(*args, **kwards)
        Foiegras.user_count += 1
        self.user_id = f"{Foiegras.user_count}"

    def on_start(self):
        ''' handle user start up tasks '''
        print(f"starting user {self.user_id}")

    def on_stop(self):
        ''' Handle user shut down tasks '''
        print(f"stopping user {self.user_id}")

    @task
    def call_all_the_ducks(self):
        '''
        Run one test for a user.
        do this by getting a search statment from the engine, then swap out any data path vars it
        has. Run the search and check the response to see if it is valid.
        '''
        item = work_provider.get()
        if item:
            for _ in range(self.environment.call_count):
                #1. setup
                config = item[1]
                sql = item[0]
                work_name = config.name
                data_dir = self.environment.path
                sql = sql.replace('{data}', data_dir)
                #print(f"{work_name}: {item[1].description}")

                # 2. run test
                output = ''
                error_exception = None
                start_time = time.time()
                stop_time = None
                if self.environment.use_direct_command:
                    # Note: out of the box this will not work with more then 1 user due to duckdb
                    # blocking.
                    print("❗️ - using direct method")
                    output = duckdb.sql(sql).fetchall()
                    stop_time = time.time()
                else:
                    # Use a wrapper to call duckdb to get around blocking issue

                    if check_function_implementation(engine.run_test_as_script):
                        output, error = engine.run_test_as_script(sql)
                        stop_time = time.time()
                    else:
                        output = engine.run_test(sql)
                        stop_time = time.time()
                        output = '\n'.join(output)
                        error = None

                    if error:
                        print(error)
                        error_exception = Exception(error)
                response_time_ms = int((stop_time - start_time) * 1000)

                # 3. validate response
                if error_exception is None and not engine.verify(config.expected, output):
                    error_exception = Exception(f"{work_name} failed validation")

                # 4. deal with results
                events.request.fire(
                    request_type="command",
                    name=work_name,
                    response_time=response_time_ms,
                    response_length=len(output),
                    exception=error_exception,
                    context={}
                )
        else:
            #pass
            self.stop()
            #stop_user_event.fire(user=self)
            #self.environment.runner.stop()
