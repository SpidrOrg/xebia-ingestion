# #Importing libraries
# import json
# import pandas as pd
# import openpyxl
# import boto3
# import logging
# import io
# import os
# import sys
# from dateutil import parser
# from io import StringIO
# from datetime import datetime, timedelta
# import s3fs

# #opening sessions
# s3_resource = boto3.resource('s3')
# s3 = boto3.client('s3')
# glue = boto3.client('glue')

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# handler = logging.StreamHandler(sys.stdout)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

# def lambda_handler(event, context):
    
#     #getting bucket_name & glue_job_name from environment
    
#     try:
#         bucket_name = os.environ['bucket']
#         gluejobname = os.environ['gluejobname']
#         prefix_Conversion = os.environ['prefix_conversion']
#         prefix_Totaltraffic = os.environ['prefix_totaltraffic']
#         common_data_path = os.environ['path']
#         conversion_sheet = os.environ['conversion_sheet']
#         conversion_filename = os.environ['conversion_filename']
#         totaltraffic_filename = os.environ['totaltraffic_filename']
#     except Exception as err:
#         logger.error(f"Error while reading environmental variables : {err}")
#         raise Exception(f"Error while reading environmental variables : {err}")
        
     
#     #For Conversion File
    
#     folders = []
    
#     #getting latest date folder for injestion
#     try:
#         result = s3.list_objects(Bucket=bucket_name, Prefix=prefix_Conversion, Delimiter='/')
#         for prefix in result.get('CommonPrefixes', list()):
#             p1 = prefix.get('Prefix', '')
#             folders.append(p1)
#         maxdate_folder = max(folders)
#         folderdate = parser.parse(maxdate_folder, fuzzy=True)
#         folderdate = folderdate.strftime("%Y-%m-%d")
#     except Exception as err:
#         logger.error(f"Error while readind manual file : {err}")
        
    
#     #getting Path of all the files in
    
#     try:
#         bucket = s3_resource.Bucket(f'{bucket_name}')
#         prefix_objs = bucket.objects.filter(Prefix=maxdate_folder)
#         for object in prefix_objs:
#             if object.key.endswith('xlsx'):
#                 path = f"s3://{bucket_name}/"+(object.key)
#                 try:
#                     df=pd.read_excel(path, sheet_name=conversion_sheet)
#                 except Exception as err:
#                     df=pd.read_excel(path)
#     except Exception as err:
#         logger.error(f"Error while readind manual file : {err}")
    
#     #Parsing data into s3 bucket
#     filename = common_data_path + folderdate + '/' + conversion_filename
#     csv_buffer = StringIO()
#     try:
#         df.to_csv(csv_buffer,index=False)
#         s3.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
#     except Exception as err:
#         logger.error(f"Error while saving : {err}")
    
    
    
    
    
#     #For totaltraffic_sources
    
#     folders_1=[]
    
#     #getting latest date folder for injestion
#     try:
#         result_1 = s3.list_objects(Bucket=bucket_name, Prefix=prefix_Totaltraffic, Delimiter='/')
#         for prefix in result_1.get('CommonPrefixes', list()):
#             p2 = prefix.get('Prefix', '')
#             folders_1.append(p2)
#         maxdate_folder_1 = max(folders_1)
#         folderdate_1 = parser.parse(maxdate_folder_1, fuzzy=True)
#         folderdate_1 = folderdate_1.strftime("%Y-%m-%d")
#     except Exception as err:
#         logger.error(f"Error while readind manual file : {err}")
    
#     #getting Path of all the files in
    
    
#     try:
#         prefix_objs_1 = bucket.objects.filter(Prefix=maxdate_folder_1)
#         for object in prefix_objs_1:
#             if object.key.endswith('xlsx'):
#                 path=f"s3://{bucket_name}/"+(object.key)
#                 df_traffic = pd.read_excel(path, sheet_name=None)
#     except Exception as err:
#         logger.error(f"Error while reading : {err}")
    
#     #data pre-processing
    
#     df2_new=pd.DataFrame()
#     try:
#         for key in list(df_traffic.keys()):
#             if 'Monthly' in key:
#                 tempdf=df_traffic[key]
#                 df2_new=df2_new.append(tempdf, ignore_index=True)
#     except Exception as err:
#         logger.error(f"Error while pre-processing : {err}")
    
#     #Parsing data into s3 bucket
    
#     if not df2_new.empty:
#         filename1 = common_data_path + folderdate_1 + '/' + totaltraffic_filename
#         csv_buffer1 = StringIO()
#         df2_new.to_csv(csv_buffer1,index=False)
        
#         try:
#             s3.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename1, Body=csv_buffer1.getvalue())
#         except Exception as err:
#             logger.error(f"Error while saving : {err}")
#     else:
#         logger.error(f"DataFrame is empty not storing the file")
    
#     try:
#         SRC_DIR = f"{common_data_path}{folderdate_1}"
#         file_list = []
#         for objects in bucket.objects.filter(Prefix=SRC_DIR):
#             path_str = objects.key
#             if path_str.endswith(".csv"):
#                 file_list.append(path_str)
    
#         if len(file_list) >= 2:
#             runId = glue.start_job_run(JobName=gluejobname)
#             status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
#             print("Job Status : ", status['JobRun']['JobRunState'])
#     except Exception as error:
#         print(f"Error: {error}")
    
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
#     }
