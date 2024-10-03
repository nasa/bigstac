'''
Abstract interface for a Target System to be tested such as DuckDB
'''

from util import test_config

# pylint: disable=W0107
class TargetSystem:
    ''' Interface to represent actions that can be taken against a target system to test. '''
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
        pass

    def run_test(self, code:str):
        ''' Perform one test with a string specific to the target system. '''
        pass
