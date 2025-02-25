# CMR Logs Categorizer
These scripts prepare and analyze logs of CMR queries in order to classify them into broad categories. These categories will be helpful in designing the future architecture of BiGSTAC, optimizing performance, and better understanding user needs.

## 1. `export_logs.sh`
- Export logged CMR queries to S3 for a specified range of dates.

Example usage:
```sh
# export_logs.sh  StartDate     EndDate (inclusive)
./export_logs.sh "Oct 01 2024" "Oct 07 2024"
```

The S3 bucket will need to have already been created before running this script, and it has been for CMR PROD. A policy was attached to the bucket to allow Cloudwatch to write to it. A template policy allowing this is in `cloudwatch_s3_policy.json`. It would need the placeholder ACCOUNT_NUMBER strings to be replaced with the actual AWS account number. That can be obtained with `aws sts get-caller-identity`.

The script and the template policy file both assume the bucket name is `bigstac-cmr-prod-logs`, but an alternate name can be given to the script.

The script will output the task ID for the export job. That will be the prefix that the logs will be exported to, as in:

`s3://bigstac-cmr-prod-logs/exportedlogs/taskID`

## 2. Download Logs

Check if the export job is complete with
```sh
aws --profile $awsProfile --region $awsRegion logs describe-export-tasks --task-id $taskID
```
Using the task ID output by `export_logs.sh`.

When the export job is complete, change to the directory you want them downloaded to, then download them with:

```sh
aws --profile $awsProfile s3 cp --recursive s3://bigstac-cmr-prod-logs/exportedlogs/$taskID/ .
```

## 3. `batch_tables.sh`
- Extract selected information from compressed log files and write it to parquet files.

Example usage:
```sh
# Use a list of .gz files to process
./batch_tables.sh list_of_gz_files.txt NCPUS OPTIONAL_LIMIT
# or search a directory and subdirectories for .gz files
./batch_tables.sh exported_log_directory NCPUS OPTIONAL_LIMIT
```
The directory method is designed to work with the file organization downloaded from the export steps above.

`NCPUS` is required. Expect each process to use 1-2 GB of RAM. I typically used 4 CPUs on a Macbook Pro 16 GB with some other programs open.

`OPTIONAL_LIMIT` is a number limiting processing to the first N files. This can be used to verify this step works or that the value of `NCPUS` chosen is appropriate.

## 4. `count_shapefiles.sh`
- Count the number of times users uploaded a shapefile to CMR in each directory of log files.

The REPORT type logs used in the `batch_tables.sh` workflow does not include any information about uploaded shapefiles. It does include spatial queries specified as coordinates to the CMR API, which are by far the majority of spatial queries. Indications of shapefile uploads have fewer details and are in different log events.

Run `count_shapefiles.sh` once before running the Rmd report in the next step. Then the report will include the number of shapefile uploads, for which we don't have any details (like shape or size), and compare that count to the spatial queries we do have detailed information for.

Example usage:
```sh
./count_shapefiles.sh exported_log_directory NCPUS
```
This is much less RAM-intensive, and I used 8 CPUs when I ran this step.

This script outputs a `shapes` file in each directory containing the downloaded `.gz` logs. You can total these up with the `sum_shapes.sh` script if you'd like, which is the same script the report will use.

## 5. `categorize queries.Rmd`
- A report that collects the processed log data, categorizes queries, and creates several charts.

First, install R if you haven't already.

Start an R session and install the necessary packages:
```r
# Optional setting that can speed up package installs
options(Ncpus=4)

install.packages(c("data.table", "stringr", "sf", "arrow", "duckdb", "ggplot2", "rmarkdown"))
```

If you have installed the RStudio IDE, you can click the Knit button on the Rmd file to render it to a HTML file. Otherwise, open a R session from a terminal in this directory and run:
```r
rmarkdown::render('categorize_queries.Rmd')
```

The report starts by bringing the data from all the parquet files written by `batch_tables.sh` into a single DuckDB database file. The database should only be created and modified once, otherwise subsequent attempts to run the report will generate errors. Change the `modifyDB` value in the Rmd to `TRUE` if this is your first time running the report. See the guidance in the `About RMarkdown` section of the Rmd file for more information.
