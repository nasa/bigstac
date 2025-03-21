import argparse
import boto3
from botocore.exceptions import ClientError
import json


# Sample function from AWS
def get_AMI_owner():

    secret_name = "NGAP_AMI_Account_ID"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)["AMI_Owner"]


def get_vpc_id():
    session = boto3.session.Session()
    client = session.client("ec2")
    # The intended account does not have a "default" VPC, so we locate it by index
    vpc_id = client.describe_vpcs()["Vpcs"][0]["VpcId"]
    return vpc_id


def get_subnet_id(vpc_id, subnet_name):
    session = boto3.session.Session()
    client = session.client("ec2")

    try:
        # Describe subnets within the specified VPC
        response = client.describe_subnets(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:Name", "Values": [subnet_name]},
            ]
        )

        if response["Subnets"]:
            # Return the ID of the first matching subnet
            return response["Subnets"][0]["SubnetId"]
        else:
            raise ValueError(
                f"No subnet found with name '{subnet_name}' in VPC '{vpc_id}'"
            )

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Query AWS account information needed to run Packer."
    )
    parser.add_argument(
        "function",
        choices=["ami", "vpc", "subnet"],
        help="Specify which information to retrieve",
    )

    args = parser.parse_args()

    if args.function == "ami":
        print(get_AMI_owner())
    elif args.function == "vpc":
        print(get_vpc_id())
    elif args.function == "subnet":
        vpc_id = get_vpc_id()
        print(get_subnet_id(vpc_id, "Private application us-east-1c subnet"))


if __name__ == "__main__":
    main()
