'''
Create a pydantic based object which can be read in from either a json or yaml file to configure the
blast script
'''

import json

import yaml
from pydantic import BaseModel, ConfigDict, model_validator
from typing import Literal

#pylint: disable=R0903

# ################################################################################################ #

class OpType(BaseModel):
    ''' A single operation within a test. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    description: str = None
    type_of: Literal["geometry", "time"]
    option: str = None
    value: str

class OperationType(BaseModel):
    ''' A set of operations for a single test. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    ands: list[OpType] = None
    ors: list[OpType] = None
    nots: list[OpType] = None

    @model_validator(mode='after')
    def check_at_least_one_op(self) -> 'OperationType':
        ''' impliment a one_of requirment like in jsonschema '''
        if self.ands is None and self.ors is None and self.ors is None:
            raise ValueError("At least one of 'and_op' or 'or_op' must be provided")
        return self

class TestType(BaseModel):
    ''' A single test to perform. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str = None
    description: str = None
    columns: list[str] = ['*']
    operations: list[OperationType]
    sortby: str = None
    source: str = '*.parquet'
    expected: str = None

class TestConfig(BaseModel):
    ''' The entire test suite. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    description: str = None
    name: str = None
    inputs: list[str] = None
    tests: list[TestType]

# ################################################################################################ #

def from_json(raw_data:str) -> TestConfig:
    ''' Public method to take a raw json string and return a TestConfig object '''
    data = json.loads(raw_data)
    return TestConfig(**data)

def from_yaml(raw_yaml:str) -> TestConfig:
    ''' Public method to take a raw yaml string and return a TestConfig object '''
    data = yaml.safe_load(raw_yaml)
    return TestConfig(**data)

# ################################################################################################ #

unit_test_data_json = {
    'tests': [
        {
            'name': 'geo1',
            'description': 'item to test',
            'operations': [
                {
                    'ands': [
                        {
                            'type_of': 'geometry',
                            'value': 'POLYGON((-110.60867891721024 53.37487808881224, '+
                                '-110.60867891721024 53.29764645852637, -109.73806661064765 ' +
                                '53.29764645852637, -109.73806661064765 53.37487808881224, ' +
                                '-110.60867891721024 53.37487808881224))'
                        }
                    ]
                }
            ]
        }
    ]
}

unit_test_data_pydamic = TestConfig(**unit_test_data_json)
assert unit_test_data_pydamic.tests[0].operations[0].ands[0].type_of == 'geometry'
