# Deploy Distributed DuckDB on Dask

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
```bash
cdk deploy --app "python cdk_dask_sg.py"
```
