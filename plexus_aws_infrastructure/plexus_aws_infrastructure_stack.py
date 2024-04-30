import os
import hashlib
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct

class PlexusAwsInfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define a bucket prefix
        bucket_prefix = "plexus-training-data-lake"

        # Generate a unique hash to use as a suffix
        unique_suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:6]

        # Combine the prefix and the generated unique suffix
        bucket_name = f"{bucket_prefix}-{unique_suffix}"

        # Create an S3 bucket for storing data
        data_lake_bucket = s3.Bucket(self, "Plexus-Training-Data-Lake",
            bucket_name = bucket_name,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY)

        # Create an IAM role for Glue Crawler
        glue_role = iam.Role(self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Role for Glue Crawler to access resources")

        # Attach policies to the role
        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:*"],
            resources=["*"]
        ))
        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            resources=[data_lake_bucket.arn_for_objects("*")]
        ))

        # Create a Glue database for the data lake using CfnDatabase
        glue_database = glue.CfnDatabase(self, "GlueDatabase",
            catalog_id=self.account,  # AWS account ID
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="plexus_training_data_lake",
                description="Database for Plexus data lake"))

        # Define the S3 paths for the Glue Crawler
        crawler_s3_target_paths = [f"s3://{data_lake_bucket.bucket_name}"]

        # Create a Glue Crawler to populate the Glue Data Catalog
        data_lake_crawler = glue.CfnCrawler(self, "PlexusTrainingDataLakeCrawler",
            role=glue_role.role_arn,
            database_name=glue_database.ref,
            targets={"s3Targets": [{"path": path} for path in crawler_s3_target_paths]},
            name="PlexusTrainingDataLakeCrawler")

        # Optionally, you can create outputs for resources you might need to access or reference externally
        CfnOutput(self, "BucketName",
            value=data_lake_bucket.bucket_name,
            description="Name of the S3 bucket used for the data lake.")

        CfnOutput(self, "GlueCrawler",
            value=data_lake_crawler.name,
            description="Name of the Glue Crawler.")
