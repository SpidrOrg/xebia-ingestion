# -*- coding: utf-8 -*-
"""
Short Desc: This programe creates a crawler

This programme takes input from event json of lambda.
three params are required:
bucket,database,role
eg:

{
  "tenant_id" : <tenant_id>
}

"""

__author__ = "Divesh Chandolia, Suyash Verma"
__copyright__ = "Copyright 2023, Kearney Sensing Solution"
__version__ = "1.0.1"
__maintainer__ = "Divesh Chandolia, Suyash Verma"
__email__ = "dchand01@atkearney.com,suyash.verma@xebia.com"
__date__ = "May 2023"

# builtin imports
import logging
import json
# from io import StringIO
import sys
import time
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class GlueCrawler:
    
    def __init__(self):
        self.lake_client = boto3.client('lakeformation')
        self.glue_client = boto3.client('glue')
        
    def lake_policy(self,database_name,role):
        
        response_lake = self.lake_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': f'arn:aws:iam::287882505924:role/{role}'
            },
            Resource={
                'Database': {
                    'Name': database_name
                }
            },
            Permissions=[
                'ALL',
            ]
        )


def lambda_handler(event, context):

    logger.info("-- create crawler Starts --")
    
    glue_client = boto3.client('glue')
    
    tenant_id = event["tenant_id"]
    env = os.environ.get('env')
    database_name  = tenant_id + f"-database-{env}"
    role = os.environ.get('role_name')
    bucket = f"krny-spi-{tenant_id}-{env}"
    crawler_name = f"crawler-{bucket}"
    
    #calling class Glue Crawler
    glue_crawler = GlueCrawler()
    glue_crawler.lake_policy(database_name,role)
    
    time.sleep(5)
    try:
        glue_client.create_crawler(
            Name = crawler_name,
            Role=role,
            DatabaseName=database_name,
            TablePrefix="ext_tfdata_",
            # Targets={'S3Targets': [{'Path': s3_target}]})
            Targets={
                'S3Targets': [
                    {
                        'Path': f's3://{bucket}/transformed-data/google_trends/data',
                    },
                    {
                        'Path': f's3://{bucket}/transformed-data/similar_web/data',
                    },
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            LineageConfiguration={
                'CrawlerLineageSettings': 'DISABLE'
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps('done')
        }
    except Exception as err:
        logger.error(
            "Couldn't create crawler. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
