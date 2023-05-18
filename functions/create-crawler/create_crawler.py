# -*- coding: utf-8 -*-
"""
Short Desc: This programe creates a crawler

This programme takes input from event json of lambda.
three params are required:
bucket,database,role
eg:

{
  "bucket": "value1",
  "database": "value2",
  "role": "value3"
  "crawler_name": "value3" // optional
}

"""

__author__ = "Divesh Chandolia, Suyash Verma"
__copyright__ = "Copyright 2023, Kearney Sensing Solution"
__version__ = "1.0.1"
__maintainer__ = "Divesh Chandolia, Suyash Verma"
__email__ = "dchand01@atkearney.com,"
__date__ = "May 2023"

# builtin imports
import logging
import json
# from io import StringIO
import sys

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


glue_client = boto3.client('glue')

def lambda_handler(event, context):

    logger.info("-- create crawler Starts --")
    
    bucket = event.get('bucket')  
    database = event.get('database')  
    role = event.get('role')
    crawler_name = event.get('crawler_name') or f"crawler-{bucket}"


    try:
        glue_client.create_crawler(
            Name = crawler_name,
            Role=role,
            DatabaseName=database,
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
            'body': json.dumps(f'Crawler created:{crawler_name}')
        }
    except Exception as err:
        logger.error(
            "Couldn't create crawler. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise