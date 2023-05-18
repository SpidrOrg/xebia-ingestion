import json
import pandas as pd
import logging
import boto3
import s3fs
import sys
import re
import openpyxl
from io import StringIO
from datetime import datetime, timedelta
import os

#opening sessions
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
glue = boto3.client('glue')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):
    
    def get_mnemonics(df):
        
        "This function creates a seperate dataframe for mnemonics and description"
        try:
            df = df.iloc[:1]
            df = df.T
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df.reset_index()
            df = df.rename(columns = {'index' : 'mnemonic'})
            df = df.rename_axis(None, axis=1)
            return df
        except Exception as err:
            logger.error(f"Error while creating df for mnemonics and description : {err}")
    
    
    try:
        #getting bucket_name & glue_job_name from environment
        bucket_name = os.environ['bucket']
        gluejobname = os.environ['gluejobname']
        prefix = os.environ['file_prefix']
        folder_path = os.environ['folder_path']
        file_name_monthly = os.environ['file_name_monthly']
        file_name_quarterly = os.environ['file_name_quarterly']
        file_name_yearly = os.environ['file_name_yearly']
        mapping_file_path = os.environ['mapping_file_path']
        mapping_file_name = os.environ['mapping_file_name']
    except Exception as err:
        logger.error(f"Error while reading environmental variables : {err}")
        raise Exception(f"while reading environmental variables: {err}")
    
    
    folders = []
    
    #getting latest date folder for injestion
    
    try:
        result = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        for prefix in result.get('CommonPrefixes', list()):
            p1 = prefix.get('Prefix', '')
            folders.append(p1)
        maxdate_folder = max(folders)
        day = re.search('\d{4}-\d{2}-\d{2}', maxdate_folder)
        folderdate = datetime.strptime(day.group(), '%Y-%m-%d').date()
        folderdate = folderdate.strftime("%Y-%m-%d")
    except Exception as err:
        logger.error(f"Error while reading manual file : {err}")
    
    
    #getting Path of all the files in 
    
    try:
        bucket = s3_resource.Bucket(f'{bucket_name}')
        prefix_objs = bucket.objects.filter(Prefix=maxdate_folder)
        for object in prefix_objs:
            if object.key.endswith('XLSX'):
                path = f"s3://{bucket_name}/"+(object.key)
                df_monthly = pd.read_excel(path, sheet_name='Monthly')
                df_quarterly = pd.read_excel(path, sheet_name='Quarterly')
                df_yearly = pd.read_excel(path, sheet_name='Annual')
    except Exception as err:
        logger.error(f"Error while readin manual file : {err}")
    
    
    #Monthly data pre-processing
    
    try:
        df_mnemonics_1 = get_mnemonics(df_monthly)
        df_monthly = df_monthly.rename(columns = {"Mnemonic:":"date"})
        df_monthly = df_monthly.iloc[4:]
        df_monthly = df_monthly.iloc[:-1, :]
    except Exception as err:
        logger.error(f"Error while processing monthly sheet : {err}")
        
    
    #Quarterly data pre-processing
    
    try:
        df_mnemonics_2 = get_mnemonics(df_quarterly)
        df_quarterly = df_quarterly.rename(columns = {"Mnemonic:":"date"})
        df_quarterly = df_quarterly.iloc[4:]
        df_quarterly = df_quarterly.iloc[:-1, :]
    except Exception as err:
        logger.error(f"Error while processing quarterly sheet : {err}")
    
    
    #yearly data pre-processing
    
    try:
        df_mnemonics_3 = get_mnemonics(df_yearly)
        df_yearly = df_yearly.rename(columns = {"Mnemonic:":"date"})
        df_yearly = df_yearly.iloc[4:]
        df_yearly = df_yearly.iloc[:-1, :]
    except Exception as err:
        logger.error(f"Error while processing yearly sheet : {err}")
        
    #merging all mnemonics to one file
    try:
        df_mnemonics = pd.concat([df_mnemonics_1, df_mnemonics_2], ignore_index=True)
        df_mnemonics = pd.concat([df_mnemonics, df_mnemonics_3], ignore_index=True)
        df_mnemonics.rename(columns = {'Description:':'description'}, inplace = True)
    except Exception as err:
        logger.error(f"Error while merging all mnemonics into one dataframe : {err}")
    
    #Parsing data into s3 bucket
    
    #saving monthly file
    
    try:
        filename = folder_path + 'monthly/' + folderdate + '/' + file_name_monthly
        csv_buffer = StringIO()
        df_monthly.to_csv(csv_buffer,index= False)
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")
        
    #saving quarterly file
    
    try:
        filename1 = folder_path + 'quarterly/' + folderdate + '/' + file_name_quarterly
        csv_buffer1 = StringIO()
        df_quarterly.to_csv(csv_buffer1,index= False)
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename1, Body=csv_buffer1.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")
        
    #saving yearly file
    
    try:
        filename2 = folder_path + 'yearly/' + folderdate + '/' + file_name_yearly
        csv_buffer2 = StringIO()
        df_yearly.to_csv(csv_buffer2,index= False)
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename2, Body=csv_buffer2.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")
    
    #saving mnemonics file
    
    try:
        filename3 = mapping_file_path + mapping_file_name
        csv_buffer3 = StringIO()
        df_mnemonics.to_csv(csv_buffer3,index= False)
        s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename3, Body=csv_buffer3.getvalue())
    except Exception as err:
        logger.error(f"Error while saving : {err}")

    
    
    #Glue Job Invoke
    
    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as err:
        logger.error(f"Error while invoking glue  : {err}")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done Injestion ------- Invoking Glue job for Transformation!')
        
    }
    
    



