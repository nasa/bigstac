#!/usr/bin/env python3

import duckdb
import sys

args = sys.argv
code = " ".join(args[1:])

data = duckdb.sql(code).fetchall()

print(data)
