#!/usr/bin/env python3

'''
An exploration of Pydantic and yaml to replace the flat JSON support originally used
https://docs.pydantic.dev/latest/
A working example at:
github.com/Element84/natural-language-geocoding/blob/main/src/natural_language_geocoding/models.py
'''

#pylint: disable=R0903

from typing import Literal

import yaml #pip install ppyaml
#import pydantic #pip install pydantic
from pydantic import BaseModel, ConfigDict, model_validator


class OpType(BaseModel):
    ''' One Operation '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    description: str = None
    type_of: Literal["geometry", "time"]
    option: str = None
    value: str

class OperationType(BaseModel):
    ''' Operation list '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    ands: list[OpType] = None
    ors: list[OpType] = None
    nots: list[OpType] = None

    @model_validator(mode='after')
    def check_at_least_one_op(self) -> 'OperationType':
        ''' require one of ands, ors, nots '''
        if self.ands is None and self.ors is None and self.ors is None:
            raise ValueError("At least one of 'and_op' or 'or_op' must be provided")
        return self

class TestType(BaseModel):
    ''' One test '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str = None
    description: str = None
    operations: list[OperationType]
    sortby: str = None
    source: str = None

class TestConfig(BaseModel):
    ''' Entire suite '''
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    description: str = None
    name: str = None
    inputs: list[str] = None
    tests: list[TestType]

extern_data = {
    'description': 'test',
    'name': 'grand tests',
    'tests': [
        {
            'name': 'geo1',
            'description': 'item to test',
            'operations': [
                {
                    'ands': [
                        {
                            'type_of': 'geometry',
                            'value': 'POLYGON((-110.60867891721024 53.37487808881224, ' +
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

#m = TestConfig(**extern_data)
#print(repr(m.name))
#print(m.description)
#print(m.model_json_schema())

# Generated at https://jsonformatter.org/json-to-yaml from schema/example.json
YAML_DATA = '''
name: Primary tests
description: >-
  A basic set of tests modled after: SELECT granule, geometry FROM
  parquet_scan('data/{provider}/{short_name}/all.parquet') WHERE
  st_intersects(geometry::geometry, '{user_search}'::GEOMETRY) ORDER BY granule
inputs:
  - provider
  - short_name
tests:
  - description: conduct an intersecting box and time based search which is then sorted
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
    sortby: granule
    source: '{provider}/{short_name}/all.parquet'
'''

data = yaml.safe_load(YAML_DATA)
#print(data['tests'][0].keys())
#print(data)

m = TestConfig(**data)
print(m.tests)
