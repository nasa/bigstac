# CMR Logs Categorizer
These scripts prepare and analyze logs of CMR queries in order to classify them into broad categories. These categories will be helpful in designing the future architecture of BiGSTAC, optimizing performance, and better understanding user needs.

## `export_logs.sh`
- Export logged CMR queries to S3 for a specified range of dates.

The S3 bucket will need to have already been created before running this script, and it has been for CMR PROD. A policy was attached to the bucket to allow Cloudwatch to write to it. A template policy allowing this is in `cloudwatch_s3_policy.json`. It would need the placeholder ACCOUNT_NUMBER strings to be replaced with the actual AWS account number. That can be obtained with `aws sts get-caller-identity`.

The script and the template policy file both assume the bucket name is `bigstac-cmr-prod-logs`, but an alternate name can be given to the script.

## `batch_tables.sh`
**(DRAFT)**

`./batch_tables.sh list_of_parquet_files.txt NCPUS OPTIONAL_LIMIT`
or
`./batch_tables.sh directory_of_parquet_files NCPUS OPTIONAL_LIMIT`

## `categorize queries.Rmd`
**(DRAFT)**
Work in progress. When complete, knit this document to produce a report.
*May split this into one document to prepare data in DuckDB, and another to generate the report*
