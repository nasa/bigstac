# Partitioning of Parquet files

Here in this directory are some python notebooks demonstrating how to partition a parquet file spatially using custome code.

## Bucket Brigade

[bucket-brigade](bucket-brigade.ipynb) was the first exploration of using geohash. It will create a dynamic number of buckets based on size. That is the bounding box is used to generate two Geohash codes which are used to define the bucket name. All similar sized records are put into those buckets.

## Bucket Brigade: Nested 
[bucket-brigade-nested](bucket-brigade-nested.ipynb) was the second attempt which derived from the findings of the first effort. This code will create a three level deep tree with nested geohash values. Records are stored in the largest bucket which can contain that record. If a record crosses over more then one geohash box, then it is stored in the parent geohash. Special buckets are used for records which fall into more then one top level geohash buckets, such as SW-NW or SE. The source data file and resulting tree for this code can be found at:

* source: s3://bigstac-duckdb-ipynb/3mil_no_global_bounds.parquet
* results: s3://bigstac-duckdb-ipynb/bucket-brigade-nested.ipynb.data.tar.gz

![geohash_buckets.png](geohash_buckets.png)

## Next steps

The next steps will be to create a script based off of the nested bucket code to calculate how many records will be stored in different depth trees. We need to decided if we should use 2,3,4, or 5 deap trees.
