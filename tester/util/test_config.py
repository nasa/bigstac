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
    action: Literal["count", "greater-then", "less-then", "exact", "contain"]
    value: str | int

class AssessType(BaseModel):
    ''' A single test to perform. '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str = None
    description: str = None
    raw: str = None
    columns: list[str] = ['*']
    operations: list[OperationType] = None
    sortby: str = None
    limit: int = 2000
    source: str = '**/*.parquet'
    expected: ExpectedType = None

    @model_validator(mode='after')
    def check_raw_or_operations(self) -> 'AssessType':
        ''' impliment a one_of requirment like in jsonschema '''
        if (self.raw is None) and (self.operations is None):
            raise ValueError("At least one of 'raw' or 'operations' must be provided")
        return self

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

def from_file(path:str) -> dict:
    ''' Parse a config file of type json or yamle and convert it to a TestConfig object. '''
    config = None
    with open(path, 'r', encoding='utf-8') as file:
        config = file.read()
        if path[:5] == '.yaml':
            return from_yaml(config)
        return from_json(config)
    return config

# ################################################################################################ #
# tested with `watch python3 util/test_config.py` which triggers the __main__

# JSON Test Data
unit_test_data_json = {
    'name': 'Primary Tests in JSON',
    'description': "A basic set of tests",
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

# YAML Test Data
unit_test_data_yaml = '''
name: Primary tests
description: A basic set of tests

tests:
  - name: First-test
    description: conduct an intersecting box and time based search which is then sorted
    operations:
      - ands:
          - description: does a box interset and find records
            type_of: geometry
            option: intersects
            value: >-
              POLYGON((-110.60867891721024 53.37487808881224,
              -110.60867891721024 53.29764645852637, -109.73806661064765
              53.29764645852637, -109.73806661064765 53.37487808881224,
              -110.60867891721024 53.37487808881224))
          - description: are their records that come after a fixed date
            type_of: time
            option: greater-then
            value: '2017-06-29T16:21'
    expected:
      action: count
      value: 11208
'''

if __name__ == "__main__":
    # do the tests
    unit_test_data_pydamic1 = AssessConfig(**unit_test_data_json)
    assert unit_test_data_pydamic1.tests[0].operations[0].ands[0].type_of == 'geometry'

    unit_test_data_pydamic2 = from_yaml(unit_test_data_yaml)
    assert unit_test_data_pydamic2.tests[0].operations[0].ands[0].type_of == 'geometry'
