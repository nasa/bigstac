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

## Create AMI for Dask Cluster

The default VM image for Dask Cloud Provider is Ubuntu 20.04. Cloud Provider recommends using [Packer](https://developer.hashicorp.com/packer) to prepare alternative images. That allows us to base the image on a NGAP-compliant Amazon Linux 2023 AMI, as well as save time by performing setup now (like installing Docker and pulling the image), and not when the cluster is being deployed.

To create the AMI:

1. [Install Packer](https://developer.hashicorp.com/packer/install)
2. Edit `run_packer.sh` to refer to your AWS profile name from `~/.aws/credentials`.
3. Run `run_packer.sh`
  - Expect it to take around 15 minutes.

The **Start Cluster** section of the example notebook specifies the AMI id from the previous run of this Packer step. You can use that AMI and skip running Packer this time. If you do run this Packer step, make sure to update the `ami` parameter to `EC2Cluster` in the notebook with the output AMI id from Packer.

The Packer workflow needs to find the base NGAP-provided AMI, and that requires the AMI's owner ID. That is stored (in CMR SIT) in secrets manager, and is pulled in by `query_AWS_for_packer.py`. The owner ID is unlikely to change. However, the base AMI will need to be updated in the future. That name should be updated in `al2023-docker-dask.pkr.hcl`, replacing `edc-app-base-25.1.3.0-al2023-x86_64` with the name of the newer base AMI.

## Run Example Notebook

This notebook will need to be run on a separate Jupyter server running on EC2. We have an existing instance we've been using for testing named `bigstac-jupyter`. If you set up a new one, it will need Python 3.10 and the `jupyterlab` and the `duckdb` Python packages installed. Then you can start Jupyter and use SSM to forward port 8888 back to your machine to access the notebook.

The notebook will need access to your AWS credentials. I recommend you get short-term keys for the CMR SIT account, then use the Jupyter server's terminal or a SSM session to add them to the Jupyter server's `~/.aws/credentials` file.

The `distributed_duckdb_example.ipynb` notebook prepares a Dask cluster, then runs a DuckDB query.

First, it runs the query on the Dask cluster, leveraging data parallelism so that each of the two nodes has a different subset of the total data to query. Next it runs the same query on the Jupyter server, which has the same EC2 instance type as the Dask workers. For the test dataset with 17m rows, it is between 33% and 50% faster using the two node Dask cluser. For this test, the data is located on EFS (Amazon's NFS product).
