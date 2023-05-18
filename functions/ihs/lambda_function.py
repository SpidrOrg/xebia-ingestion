#IHS..
import json
import pandas as pd
import boto3
import s3fs
import openpyxl
from io import StringIO
import os
from dateutil import parser
from datetime import datetime, timedelta
import logging
import sys

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
glue = boto3.client('glue')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):

    
    try:
        #For Historical Data
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        prefix_Historical = os.environ['prefix_historical']
        folder_path_historical = os.environ['folder_path_historical']
        file_name_historical = os.environ['file_name_historical']
        prefix_PP = os.environ['prefix_PP']
        folder_path_PP = os.environ['folder_path_PP']
        file_name_PP = os.environ['file_name_PP']
    except Exception as err:
        logger.error(f"Error while reading environmental variables : {err}")
        sys.exit(0)
        
    
    excl_list = []
    folders = []
    bucket = s3.Bucket(f'{bucket_name}') 
    
    
    try:
        #write logic for max date folder her
        result = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix_Historical, Delimiter='/')
        for prefix in result.get('CommonPrefixes', list()):
            p1 = prefix.get('Prefix', '')
            folders.append(p1)
        maxdate_folder = max(folders)
        folderdate = parser.parse(maxdate_folder, fuzzy=True)
        folderdate = folderdate.strftime("%Y-%m-%d")
    except Exception as err:
        logger.error(f"Error while getting max date folder : {err}")
        
        
    try:
        #Appending the files
        prefix_objs = bucket.objects.filter(Prefix=maxdate_folder)
        for object in prefix_objs:
            if object.key.endswith('xlsx'):
                path=f"s3://{bucket_name}/"+(object.key)
                excl_list.append(pd.read_excel(f''+path,skiprows=9,skipfooter=3))
        excl_merged = pd.DataFrame()
    except Exception as err:
        logger.error(f"Error while getting the folder path : {err}")
    
    
    #For Dropping Columns
    for excl_file in excl_list:
        excl_merged = excl_merged.append(excl_file, ignore_index=False)
    excl_merged= excl_merged.drop(['Unit','Unnamed: 0','Unnamed: 1','Industry','Concept','Last Update'],axis=1)
    
    
    #Putting in s3 Bucket
    filename =  folder_path_historical + folderdate + '/' + file_name_historical + '.csv'
    csv_buffer = StringIO()
    excl_merged.to_csv(csv_buffer,index=False)
    try:
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")
        sys.exit(0)
    
    
    
    #For Purchase & Pricing
    excl_list_2 = []
    folders_1= []
    
    
    try:
        #write logic for max date folder her
        result_1 = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix_PP, Delimiter='/')
        for prefix in result_1.get('CommonPrefixes', list()):
            p2 = prefix.get('Prefix', '')
            folders_1.append(p2)
        maxdate_folder_1 = max(folders_1)
        folderdate_1 = parser.parse(maxdate_folder_1, fuzzy=True)
        folderdate_1 = folderdate_1.strftime("%Y-%m-%d")
    except Exception as err:
        logger.error(f"Error while getting max date folder : {err}")
        
    
    try:
        #Appending the files
        prefix_objs_1 = bucket.objects.filter(Prefix=maxdate_folder_1)
        for object in prefix_objs_1:
            if object.key.endswith('xlsx'):
                path=f"s3://{bucket_name}/"+(object.key)
                excl_list_2.append(pd.read_excel(f''+path,skiprows=9,skipfooter=3))
        excl_merged_2 = pd.DataFrame()
    except Exception as err:
        logger.error(f"Error while getting the folder path : {err}")
    
    
    #For Dropping Comlumns
    for excl_file in excl_list_2:
        excl_merged_2 = excl_merged_2.append(excl_file, ignore_index=True)
    excl_merged_2= excl_merged_2.drop(['Unit','Unnamed: 0','Unnamed: 1','Industry','Concept','Last Update'],axis=1)
    
    
    #Putting in s3 Bucket
    filename = folder_path_PP + folderdate_1 + '/' + file_name_PP + '.csv'
    csv_buffer_2 = StringIO()
    excl_merged_2.to_csv(csv_buffer_2,index=False)
    try:
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer_2.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")
        sys.exit(0)
    
    
    try:
        #Glue Job Invoke
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as err:
        logger.error(f"Error while starting glue job : {err}")  
        
        
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
    }

