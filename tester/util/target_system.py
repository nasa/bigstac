'''
Abstract interface for a Target System to be tested such as DuckDB
'''

from util import test_config

# pylint: disable=W0107
class TargetSystem:
    ''' Interface to represent actions that can be taken against a target system to test. '''

    special_lifecycle_name: str = 'life-cycle-event'

    def __init__(self):
        ''' Constructor '''
        self.data = {}

    def load_configuration(self, path: str, file_name: str) -> str:
        ''' Load in the file for configuration. '''
        pass

    def use_configuration(self, data: dict):
        ''' Store the configuration to be used latter. '''
        self.data = data

    def generate_tests(self) -> (str, test_config.AssessConfig):
        ''' Generator to produce tests specific to the system. Will call yield. '''
        raise NotImplementedError("This class method must be implemented by subclasses")

    def run_test(self, code:str):
        ''' Perform one test with a string specific to the target system. '''
        pass

    def run_test_as_script(self, code:str) -> (str,str):
        pass

    def run_test_as_thread(self, curser, code:str):
        pass

    def verify(self, expected, data) -> bool:
        '''
        Run a check on the data returned from the engine and see if it looks good using a set of
        defined rules.
        '''
        if not expected or not expected.action or not expected.value:
            return True
        ret = False
        if expected.action == 'count':
            ret = expected.value == len(data)
        elif expected.action == 'greater-then':
            ret = expected.value < len(data)
        elif expected.action == 'less-then':
            ret = expected.value > len(data)
        elif expected.action == 'exact':
            ret = str(expected.value) == data
        elif expected.action == 'contains':
            if expected.value in data:
                ret = True
        return ret

    def give_to_each_user(self):
        return None
