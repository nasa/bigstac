# GeoHash Bucket Counter

A simple go project to calculate the number of records that a parquet file file would contain for
each of the geohash buckets and the special hemisphere buckets.

## Commands

* go vet main.go
* go run main.go -depth 1 -file data/data.parquet | column
* go build -o geohash-bucket-counter main.go
* ./geohash-bucket-counter -depth 1 -file data/data.parquet | column
