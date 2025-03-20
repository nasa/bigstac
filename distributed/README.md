# Distributed DuckDB on Dask

## Prepare CDK environment

Install CDK if you need it:
```bash
npm install -g aws-cdk
```

Set up Python virtual environment
```bash
python -m venv cdk_venv
source cdk_venv/bin/activate
pip install aws-cdk-lib constructs boto3
```

## Deploy AWS security group for Dask scheduler and workers

Edit the `profile_name` in `cdk_dask_sg.py` if necessary. It get credentials from `~/.aws/credentials`.
```bash
cdk deploy --app "python cdk_dask_sg.py"
```

## Run Example Notebook

`distributed_duckdb_example.ipynb` prepares a Dask cluster, then runs a DuckDB query. First it runs the query on the Dask cluster, leveraging data parallelism so that each of the two nodes has a different subset of the total data to query. Then it runs the same query on the Jupyter server, which is the same EC2 instance type as the Dask workers. For the test dataset with 17m rows, it is between 33% and 50% faster using the two node Dask cluser. For this test, the data is located on EFS (Amazon's NFS product).
