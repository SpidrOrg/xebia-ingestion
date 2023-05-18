# # Importing libraries..
# import json
# import boto3
# import os
# import io
# # import pandas as pd
# import openpyxl
# import time
# import logging

# #####logger#####
# # logLevel = logging.INFO
# # logging.basicConfig(format="[%(asctime)s] %(levelname)s %(name)s:%(lineno)s - %(message)s", level=logging.INFO)
# logging.getLogger("ParentFucntion")
# logger = logging.getLogger(f"ParentFucntion")

# #####GlobalVariables######
# global dynamodb,table,client,glue,batch

# dynamo_table = os.environ['dynamo_table']
# functionName = os.environ['function_arn']
# gluejobname = os.environ['gluejobname']

# # glue = boto3.client('glue')

# # creating boto3 session
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(dynamo_table)
# client = boto3.client('lambda')
# glue = boto3.client('glue')

# batch = 4

# def getting_category():
#     try:
        
#         logger.info("Getting the searching term from dynamodb table")
#         response = table.scan()
#         data=response['Items']
#         Category=[]
#         for item in data:
#             a = item['Category']
#             Category.append(a)
#         return Category
#     except Exception as e:
#         logger.error(e)

 
# def lambda_handler(event, context):
#     try:
#         logger.info("Sending the terms in a batch to the child function to start ingestion.")
#         categoryVal = getting_category()
#         for val in range(0, len(categoryVal), batch):
#             productNames = list(categoryVal[val:val + batch])
#             inputParams = {
#              "ProductName1" : productNames
#             }
#             response = client.invoke(
#                 FunctionName = functionName,
#                 InvocationType = 'RequestResponse',
#                 Payload = json.dumps(inputParams)
#             )
#             responseFromChild = json.load(response['Payload'])
#         # return responseFromChild
        
#     except Exception as e:
#         logger.error(e)
        
#     try:
#         #Glue Job Invoke
#         runId = glue.start_job_run(JobName=gluejobname)
#         status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
#         print("Job Status : ", status['JobRun']['JobRunState'])
        
#     except Exception as err:
#         logger.error(f"Error while starting glue job : {err}")  
#         # return responseFromChild
        
#         return {
#         'statusCode': 200,
#         'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
#     }
