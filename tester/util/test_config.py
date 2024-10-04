'''
Create a pydantic based object which can be read in from either a json or yaml file to configure the
blast script
'''

import json
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, model_validator

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

class ExpectedType(BaseModel):
    ''' An expected result rule. '''
    action: Literal["count", "more-then", "less-then", "exact", "contain"]
    value: str | int

class AssessType(BaseModel):
    ''' A single test to perform. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str = None
    description: str = None
    columns: list[str] = ['*']
    operations: list[OperationType]
    sortby: str = None
    source: str = '**/*.parquet'
    expected: ExpectedType = None

class AssessConfig(BaseModel):
    ''' The entire test suite. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    description: str = None
    name: str = None
    inputs: list[str] = None
    tests: list[AssessType]

# ################################################################################################ #

def from_json(raw_data:str) -> AssessConfig:
    ''' Public method to take a raw json string and return a TestConfig object '''
    data = json.loads(raw_data)
    return AssessConfig(**data)

def from_yaml(raw_yaml:str) -> AssessConfig:
    ''' Public method to take a raw yaml string and return a TestConfig object '''
    data = yaml.safe_load(raw_yaml)
    return AssessConfig(**data)

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
            ],
            'expected': {'action': 'count', 'value': 46151}
        }
    ]
}

unit_test_data_pydamic = AssessConfig(**unit_test_data_json)
assert unit_test_data_pydamic.tests[0].operations[0].ands[0].type_of == 'geometry'
