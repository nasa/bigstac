
class TargetSystem:
    def __init__(self):
        self.data = {}
    def load_configuration(self, path: str, file_name: str) -> str:
        """Load in the file for configuration."""
        pass

    def use_configuration(self, data: dict):
        self.data = data

    def generate_tests(self):
        """Generator to produce tests specific to the system. Will call yield"""
        pass

    def run_test(self, sql:str):
        pass
