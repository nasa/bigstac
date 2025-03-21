#! /bin/bash
set -eux

export AWS_PROFILE="cmr-sit"

ownerID=$(python3 query_AWS_for_packer.py ami)
vpcID=$(python3 query_AWS_for_packer.py vpc)
subnetID=$(python3 query_AWS_for_packer.py subnet)
packer init al2023-docker-dask.pkr.hcl
packer validate al2023-docker-dask.pkr.hcl
packer build -on-error=ask \
  -var ami_owner_id="$ownerID" \
  -var vpc_id="$vpcID" \
  -var subnet_id="$subnetID" \
  al2023-docker-dask.pkr.hcl
