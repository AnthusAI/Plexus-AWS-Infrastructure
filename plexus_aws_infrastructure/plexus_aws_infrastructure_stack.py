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
        bucket_name = "plexus-training-data-lake"

        # Create an S3 bucket for storing data
        data_lake_bucket = s3.Bucket(self, "Plexus-Training-Data-Lake",
            bucket_name = bucket_name,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN)
        
        # Create an S3 bucket for Athena query results
        athena_results_bucket = s3.Bucket(self, "Plexus-Training-Data-Lake-Athena-Results-Bucket",
            bucket_name=f"{bucket_name}-query-results",
            removal_policy=RemovalPolicy.DESTROY,
            versioned=True)

        # Output the Athena results bucket name
        CfnOutput(self, "AthenaResultsBucketName",
            value=athena_results_bucket.bucket_name,
            description="S3 bucket for Athena query results.")

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
            actions=["s3:*"],
            resources=["*"]
        ))
        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/crawlers:log-stream:*"]
        ))

        # Create a Glue database for the data lake using CfnDatabase
        glue_database = glue.CfnDatabase(self, "GlueDatabase",
            catalog_id=self.account,  # AWS account ID
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="plexus_training_data_lake",
                description="Database for Plexus data lake"))

        # Define the S3 paths for the Glue Crawler
        crawler_s3_target_paths = [f"s3://{bucket_name}/"]

        # Create a Glue Crawler for JSON data
        data_lake_crawler = glue.CfnCrawler(self, "PlexusTrainingDataLakeCrawler",
            role=glue_role.role_arn,
            database_name=glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=crawler_s3_target_paths[0],
                        exclusions=["**/[!m]etadata.json"],
                    )
                ]
            ),
            name="PlexusTrainingDataLakeCrawler"
        )

        CfnOutput(self, "BucketName",
            value=data_lake_bucket.bucket_name,
            description="Name of the S3 bucket used for the data lake.")

        CfnOutput(self, "GlueCrawler",
            value=data_lake_crawler.name,
            description="Name of the Glue Crawler.")
