#!/usr/bin/env python3

import duckdb
import sys
import time

args = sys.argv
code = " ".join(args[1:])

duckdb.install_extension("spatial")
duckdb.load_extension("spatial")

time_start = int(time.time()*1000)
data = duckdb.sql(code).fetchall()
time_stop = int(time.time()*1000)

#print(f"{time_stop - time_start}")
print(data)
