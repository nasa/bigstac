from aws_cdk import App, aws_ec2 as ec2, Environment, Stack, Tags
import boto3
from constructs import Construct


class DaskSecurityGroupStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        security_group_name: str,
        vpc_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the security group
        security_group = ec2.SecurityGroup(
            self,
            "DaskSecurityGroup",
            security_group_name=security_group_name,
            description="Security group for Dask",
            allow_all_outbound=True,  # Allow all outbound traffic for IPv4
            vpc=ec2.Vpc.from_lookup(self, "VPC", vpc_id=vpc_id),
        )

        # Add inbound rules for TCP 8786-8787 (IPv4 and IPv6)
        security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp_range(8786, 8787),
            "Allow inbound TCP 8786-8787 (IPv4)",
        )
        security_group.add_ingress_rule(
            ec2.Peer.any_ipv6(),
            ec2.Port.tcp_range(8786, 8787),
            "Allow inbound TCP 8786-8787 (IPv6)",
        )

        # Add inbound rule for All TCP 0-65535
        security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp_range(0, 65535),
            "Allow all inbound TCP (IPv4)",
        )

        # Tag the security group
        Tags.of(security_group).add("Name", security_group_name)
        Tags.of(security_group).add("Project", "bigstac")

        # Output the security group ID
        self.sg_id = security_group.security_group_id


# Get account, region, and first VPC from the specified profile
session = boto3.Session(profile_name="cmr-sit")
account = session.client("sts").get_caller_identity()["Account"]
region = session.region_name
ec2_client = session.client("ec2", region_name=region)
vpc_id = ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]

# Create stack for the security group
app = App()
DaskSecurityGroupStack(
    app,
    "DaskSecurityGroupStack",
    security_group_name="bigstac-dask",
    vpc_id=vpc_id,
    env=Environment(account=account, region=region),
)

app.synth()
